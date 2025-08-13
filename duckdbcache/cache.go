package duckdbcache

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/BytePengwin/GoToolkit/singleflight"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/BytePengwin/GoToolkit/connectionpool"
	"github.com/BytePengwin/GoToolkit/profiling"
)

type cacheEntry struct {
	localPath    string
	downloadedAt time.Time
	lastAccessed time.Time
}

type S3VersionedDataCache struct {
	s3Bucket      string
	localCacheDir string
	cacheTTL      time.Duration

	s3Client    *s3.Client
	uploader    *manager.Uploader
	downloader  *manager.Downloader
	timespansDB *sql.DB

	// In-memory cache
	memCache  map[string]*cacheEntry
	cacheLock sync.RWMutex

	// Connection pool
	connPool *connectionpool.HybridPool
	//timespans Connection Pool
	timePool *connectionpool.HybridPool
	// Single-flight downloader
	singleFlightDownloader *singleflight.Downloader[string]

	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

type UploadResult struct {
	UploadedFiles  int
	TotalSizeBytes int64
	VersionFolder  string
}

func NewS3VersionedDataCache(s3Bucket, localCacheDir string, cacheTTL time.Duration,
	endpointURL, accessKey, secretKey string) (*S3VersionedDataCache, error) {

	if err := os.MkdirAll(localCacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Configure AWS SDK
	ctx := context.Background()
	var opts []func(*config.LoadOptions) error

	// Set region
	opts = append(opts, config.WithRegion("us-east-1"))

	// Set endpoint if provided
	if endpointURL != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               endpointURL,
				HostnameImmutable: true,
				SigningRegion:     "us-east-1",
			}, nil
		})
		opts = append(opts, config.WithEndpointResolverWithOptions(customResolver))
	}

	// Set credentials if provided
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}

	// Force path style for S3
	opts = append(opts, config.WithDefaultsMode(aws.DefaultsModeInRegion))

	// Load the configuration
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	// Create S3 client and transfer manager
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	uploader := manager.NewUploader(s3Client)
	downloader := manager.NewDownloader(s3Client, func(d *manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024
		d.Concurrency = 10
	})

	cache := &S3VersionedDataCache{
		s3Bucket:      s3Bucket,
		localCacheDir: localCacheDir,
		cacheTTL:      cacheTTL,
		s3Client:      s3Client,
		uploader:      uploader,
		downloader:    downloader,
		memCache:      make(map[string]*cacheEntry),
		connPool: connectionpool.NewHybridPool(
			20,            // maxPoolSize
			5*time.Minute, // connectionTTL
			5000,          // maxUsageCount
		),
		timePool: connectionpool.NewHybridPool(
			20,            // maxPoolSize
			5*time.Minute, // connectionTTL
			5000,          // maxUsageCount
		),
	}

	// Initialize single-flight downloader
	cache.singleFlightDownloader = singleflight.New[string](
		cache.downloadVersion,
		singleflight.Config{
			CleanupInterval: 5 * time.Minute,
			PromiseTimeout:  15 * time.Minute,
		})

	if err := cache.initTimespansDB(); err != nil {
		return nil, fmt.Errorf("failed to initialize timespans DB: %w", err)
	}

	cache.startCleanupRoutine()

	return cache, nil
}

func (c *S3VersionedDataCache) GetVersionConn(versionFolder string) (interface{}, error) {
	timer := profiling.Start(CacheGet)
	defer timer.End()

	localPath, err := c.ensureVersionAvailable(versionFolder)
	if err != nil {
		return nil, err
	}

	dbPath := filepath.Join(localPath, "database.db")
	return c.connPool.GetConnection(versionFolder, dbPath)
}

func (c *S3VersionedDataCache) GetVersionConnMultiplexed(versionFolder string) (interface{}, error) {
	timer := profiling.Start(CacheGet)
	defer timer.End()

	localPath, err := c.ensureVersionAvailable(versionFolder)
	if err != nil {
		return nil, err
	}

	dbPath := filepath.Join(localPath, "database.db")
	return c.connPool.GetMultiplexedConnection(versionFolder, dbPath)
}

// ensureVersionAvailable checks if a version is available locally and downloads it if needed.
// It implements a memory cache to avoid repeated filesystem checks and uses a single-flight
// pattern to prevent multiple concurrent downloads of the same version.
//
// Parameters:
//   - versionFolder: The name of the version folder to ensure is available
//
// Returns:
//   - The local path where the version is stored
//   - An error if the version couldn't be ensured (e.g., download failed)
//
// This method is called by GetVersionConn and GetVersionConnMultiplexed to ensure
// the requested version is available locally before returning a database connection.
// It updates the last accessed time for cache entries to support the LRU cleanup mechanism.
func (c *S3VersionedDataCache) ensureVersionAvailable(versionFolder string) (string, error) {
	// Fast path: check memory cache first
	c.cacheLock.RLock()
	entry, exists := c.memCache[versionFolder]
	c.cacheLock.RUnlock()

	if exists {
		// Update last accessed time
		c.cacheLock.Lock()
		entry.lastAccessed = time.Now()
		c.cacheLock.Unlock()
		return entry.localPath, nil
	}

	// Use single-flight downloader
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Minute)
	result, err := c.singleFlightDownloader.Download(ctx, versionFolder)
	if err != nil {
		return "", err
	}

	localPath := result

	// Add to memory cache
	now := time.Now()
	c.cacheLock.Lock()
	c.memCache[versionFolder] = &cacheEntry{
		localPath:    localPath,
		downloadedAt: now,
		lastAccessed: now,
	}
	c.cacheLock.Unlock()

	return localPath, nil
}

// downloadVersion downloads a specific version folder from S3 to the local cache directory.
// It lists all objects in the S3 bucket with the given version folder prefix, downloads
// all .db files, and verifies the downloaded files are valid.
//
// Parameters:
//   - ctx: Context for the download operation (can be used for cancellation)
//   - versionFolder: The name of the version folder to download
//
// Returns:
//   - The local path where the version was downloaded
//   - An error if the download or verification failed
//
// This method is called by the singleFlightDownloader when a version needs to be downloaded.
// It creates the necessary local directory structure, downloads all database files,
// and verifies the integrity of the downloaded files. If verification fails, it cleans up
// the partially downloaded files.
//
// The method is profiled to track performance metrics of download operations.
func (c *S3VersionedDataCache) downloadVersion(ctx context.Context, versionFolder string) (string, error) {
	timer := profiling.Start(CacheDownload)
	defer timer.End()

	localVersionPath := filepath.Join(c.localCacheDir, versionFolder)
	if err := os.MkdirAll(localVersionPath, 0755); err != nil {
		return "", err
	}

	// List objects in S3
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.s3Bucket),
		Prefix: aws.String(versionFolder + "/"),
	}

	result, err := c.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return "", err
	}

	for _, obj := range result.Contents {
		if strings.HasSuffix(*obj.Key, ".db") {
			fileName := filepath.Base(*obj.Key)
			localFile := filepath.Join(localVersionPath, fileName)

			file, err := os.Create(localFile)
			if err != nil {
				return "", err
			}

			_, err = c.downloader.Download(ctx, file, &s3.GetObjectInput{
				Bucket: aws.String(c.s3Bucket),
				Key:    obj.Key,
			})

			if closeErr := file.Close(); closeErr != nil {
				return "", closeErr
			}

			if err != nil {
				return "", err
			}
		}
	}

	// Verify download
	if hasValidFiles, err := c.verifyVersionFolder(localVersionPath); err != nil || !hasValidFiles {
		os.RemoveAll(localVersionPath)
		return "", fmt.Errorf("verification failed for %s", versionFolder)
	}

	return localVersionPath, nil
}

// verifyVersionFolder checks if a downloaded version folder contains valid database files.
// It performs two checks:
// 1. Verifies that database.db exists and is not empty
// 2. Attempts to open the database to ensure it's a valid DuckDB file
//
// Parameters:
//   - folderPath: The local path to the version folder to verify
//
// Returns:
//   - true if the folder contains valid database files, false otherwise
//   - an error if one occurred during verification
//
// This method is called after downloading a version to ensure the downloaded files
// are valid and usable. It helps prevent corrupted or incomplete downloads from
// being used, which could cause application errors.
func (c *S3VersionedDataCache) verifyVersionFolder(folderPath string) (bool, error) {
	dbFile := filepath.Join(folderPath, "database.db")
	if stat, err := os.Stat(dbFile); err != nil || stat.Size() == 0 {
		return false, nil
	}

	if db, err := sql.Open("duckdb", dbFile); err != nil {
		return false, nil
	} else {
		db.Close()
	}

	return true, nil
}

func (c *S3VersionedDataCache) UploadVersion(localFolderPath, versionFolder string) (*UploadResult, error) {
	timer := profiling.Start(CacheUpload)
	defer timer.End()

	if _, err := os.Stat(localFolderPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("local folder not found: %s", localFolderPath)
	}

	cacheVersionPath := filepath.Join(c.localCacheDir, versionFolder)
	needsCopy := !strings.HasPrefix(filepath.Clean(localFolderPath), filepath.Clean(c.localCacheDir))

	if needsCopy {
		if err := os.MkdirAll(cacheVersionPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create cache directory: %w", err)
		}
	}

	uploadedFiles := 0
	totalSize := int64(0)

	err := filepath.WalkDir(localFolderPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && (strings.HasSuffix(path, ".db") || strings.HasSuffix(path, ".sql")) {
			fileName := filepath.Base(path)
			var uploadPath string
			isDBFile := strings.HasSuffix(path, ".db")

			if needsCopy && isDBFile {
				cacheFile := filepath.Join(cacheVersionPath, fileName)
				src, err := os.Open(path)
				if err != nil {
					return err
				}

				dst, err := os.Create(cacheFile)
				if err != nil {
					src.Close()
					return err
				}

				_, err = io.Copy(dst, src)
				src.Close()
				dst.Close()
				if err != nil {
					return err
				}

				uploadPath = cacheFile
			} else {
				uploadPath = path
			}

			sourceFile, err := os.Open(uploadPath)
			if err != nil {
				return err
			}

			s3Key := fmt.Sprintf("%s/%s", versionFolder, fileName)
			_, err = c.uploader.Upload(context.Background(), &s3.PutObjectInput{
				Bucket:       aws.String(c.s3Bucket),
				Key:          aws.String(s3Key),
				Body:         sourceFile,
				StorageClass: types.StorageClassStandard,
			})

			stat, _ := sourceFile.Stat()
			sourceFile.Close()

			if err != nil {
				return err
			}

			uploadedFiles++
			totalSize += stat.Size()
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Add to cache
	now := time.Now()
	c.cacheLock.Lock()
	c.memCache[versionFolder] = &cacheEntry{
		localPath:    cacheVersionPath,
		downloadedAt: now,
		lastAccessed: now,
	}
	c.cacheLock.Unlock()

	return &UploadResult{
		UploadedFiles:  uploadedFiles,
		TotalSizeBytes: totalSize,
		VersionFolder:  versionFolder,
	}, nil
}

func (c *S3VersionedDataCache) CleanupExpired() error {
	timer := profiling.Start(CacheCleanup)
	defer timer.End()

	//c.connPool.PrintStatus("version_2024-09-19")
	cutoff := time.Now().Add(-c.cacheTTL)

	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	var toDelete []string

	for versionFolder, entry := range c.memCache {
		if entry.lastAccessed.Before(cutoff) {
			// Note: connpool doesn't have direct equivalents for GetActiveCount and ClearPoolForVersion
			// We'll just remove the local files and let the pool's own cleanup handle the connections
			if err := os.RemoveAll(entry.localPath); err != nil {
				continue
			}
			toDelete = append(toDelete, versionFolder)
		}
	}

	for _, versionFolder := range toDelete {
		delete(c.memCache, versionFolder)
	}

	return nil
}

// startCleanupRoutine initializes and starts a background goroutine that periodically
// cleans up expired cache entries. It runs on a 5-second interval and continues
// until the cache is shut down.
//
// This method is called during cache initialization to ensure automatic cleanup
// of expired cache entries. It creates a ticker and a stop channel, then starts
// a goroutine that calls CleanupExpired at regular intervals.
//
// The cleanup routine helps prevent the cache from growing too large by removing
// entries that haven't been accessed within the configured TTL period.
func (c *S3VersionedDataCache) startCleanupRoutine() {
	c.cleanupTicker = time.NewTicker(5 * time.Second)
	c.stopCleanup = make(chan struct{})

	go func() {
		for {
			select {
			case <-c.cleanupTicker.C:
				if err := c.CleanupExpired(); err != nil {
					fmt.Printf("Cache cleanup error: %v\n", err)
				}
			case <-c.stopCleanup:
				return
			}
		}
	}()
}

// initTimespansDB initializes the timespans database that maps version folders to their
// corresponding start dates. It creates a local DuckDB database file and ensures the
// necessary table structure exists.
//
// Returns an error if the database initialization fails.
//
// This method is called during cache initialization to set up the timespans tracking system.
// It performs the following steps:
//  1. Opens or creates a local DuckDB database file for timespans
//  2. Creates the timespans table if it doesn't exist
//  3. Attempts to pull existing timespan data from S3
//
// The timespans database is used to track the time periods covered by each version folder,
// which can be useful for applications that need to query data based on time ranges.
func (c *S3VersionedDataCache) initTimespansDB() error {
	var err error
	c.timespansDB, err = sql.Open("duckdb", filepath.Join(c.localCacheDir, "timespans.db"))
	if err != nil {
		return err
	}

	query := `
		CREATE TABLE IF NOT EXISTS timespans (
			folder_name VARCHAR PRIMARY KEY,
			start_date DATE
		)
	`
	_, err = c.timespansDB.Exec(query)
	if err != nil {
		return err
	}

	if err := c.PullTimespansFromS3(); err != nil {
		fmt.Printf("failed to pull timespans from S3: %v\n", err)
	}

	return nil
}

func (c *S3VersionedDataCache) PullTimespansFromS3() error {
	timespansFile := filepath.Join(c.localCacheDir, "timespans_temp.db")

	file, err := os.Create(timespansFile)
	if err != nil {
		return err
	}
	defer file.Close()
	defer os.Remove(timespansFile)

	_, err = c.downloader.Download(context.Background(), file, &s3.GetObjectInput{
		Bucket: aws.String(c.s3Bucket),
		Key:    aws.String("timespans.db"),
	})
	if err != nil {
		return err
	}

	_, err = c.timespansDB.Exec("DELETE FROM timespans")
	if err != nil {
		return err
	}

	_, err = c.timespansDB.Exec(fmt.Sprintf("ATTACH '%s' AS temp_db", timespansFile))
	if err != nil {
		return err
	}

	_, err = c.timespansDB.Exec("INSERT INTO timespans SELECT * FROM temp_db.timespans")
	if err != nil {
		return err
	}

	_, err = c.timespansDB.Exec("DETACH temp_db")
	return err
}

func (c *S3VersionedDataCache) UpdateTimespans(folderName string, startDate time.Time) error {
	query := `INSERT OR REPLACE INTO timespans (folder_name, start_date) VALUES (?, ?)`
	_, err := c.timespansDB.Exec(query, folderName, startDate)
	if err != nil {
		return err
	}

	_, err = c.timespansDB.Exec("CHECKPOINT")
	if err != nil {
		return err
	}

	timespansFile := filepath.Join(c.localCacheDir, "timespans.db")
	file, err := os.Open(timespansFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = c.uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket:       aws.String(c.s3Bucket),
		Key:          aws.String("timespans.db"),
		Body:         file,
		StorageClass: types.StorageClassStandard,
	})
	return err
}

func (c *S3VersionedDataCache) GetTimespansConn() *sql.DB {
	return c.timespansDB
}

func (c *S3VersionedDataCache) Shutdown() error {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
	}
	if c.stopCleanup != nil {
		close(c.stopCleanup)
	}

	c.singleFlightDownloader.Shutdown()
	c.connPool.Shutdown()

	if c.timespansDB != nil {
		c.timespansDB.Close()
	}

	if err := os.RemoveAll(c.localCacheDir); err != nil {
		return fmt.Errorf("failed to remove cache directory: %w", err)
	}

	if err := os.MkdirAll(c.localCacheDir, 0755); err != nil {
		return fmt.Errorf("failed to recreate cache directory: %w", err)
	}

	c.cacheLock.Lock()
	c.memCache = make(map[string]*cacheEntry)
	c.cacheLock.Unlock()

	return nil
}

//package versioncache
//
//import (
//	"context"
//	"database/sql"
//	"fmt"
//	"io"
//	"io/fs"
//	"os"
//	"path/filepath"
//	"strings"
//	"sync"
//	"time"
//	singleflight "github.com/BytePengwin/GoToolkit/Singleflight"
//
//	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/aws/credentials"
//	"github.com/aws/aws-sdk-go/aws/session"
//	"github.com/aws/aws-sdk-go/service/s3"
//	"github.com/aws/aws-sdk-go/service/s3/s3manager"
//	_ "github.com/marcboeker/go-duckdb"
//
//	"github.com/BytePengwin/GoToolkit/connpool"
//)
//
//type cacheEntry struct {
//	localPath    string
//	downloadedAt time.Time
//	lastAccessed time.Time
//}
//
//type S3VersionedDataCache struct {
//	s3Bucket      string
//	localCacheDir string
//	cacheTTL      time.Duration
//
//	s3Client   *s3.S3
//	uploader   *s3manager.Uploader
//	downloader *s3manager.Downloader
//
//	// In-memory cache
//	memCache  map[string]*cacheEntry
//	cacheLock sync.RWMutex
//
//	// Connection pool
//	connPool *connpool.HybridPool
//	// Timespans Connection Pool
//	timePool *connpool.HybridPool
//	// Single-flight downloader
//	singleFlightDownloader *singleflight.Downloader[string]
//
//	cleanupTicker *time.Ticker
//	stopCleanup   chan struct{}
//}
//
//type UploadResult struct {
//	UploadedFiles  int
//	TotalSizeBytes int64
//	VersionFolder  string
//}
//
//func NewS3VersionedDataCache(s3Bucket, localCacheDir string, cacheTTL time.Duration,
//	endpointURL, accessKey, secretKey string) (*S3VersionedDataCache, error) {
//
//	if err := os.MkdirAll(localCacheDir, 0755); err != nil {
//		return nil, fmt.Errorf("failed to create cache directory: %w", err)
//	}
//
//	// Configure AWS session
//	config := &aws.Config{
//		Region:           aws.String("us-east-1"),
//		S3ForcePathStyle: aws.Bool(true),
//	}
//
//	if endpointURL != "" {
//		config.Endpoint = aws.String(endpointURL)
//	}
//
//	if accessKey != "" && secretKey != "" {
//		config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
//	}
//
//	sess, err := session.NewSession(config)
//	if err != nil {
//		return nil, fmt.Errorf("failed to create AWS session: %w", err)
//	}
//
//	s3Client := s3.New(sess)
//	uploader := s3manager.NewUploader(sess)
//	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
//		d.PartSize = 64 * 1024 * 1024
//		d.Concurrency = 10
//	})
//
//	cache := &S3VersionedDataCache{
//		s3Bucket:      s3Bucket,
//		localCacheDir: localCacheDir,
//		cacheTTL:      cacheTTL,
//		s3Client:      s3Client,
//		uploader:      uploader,
//		downloader:    downloader,
//		memCache:      make(map[string]*cacheEntry),
//		connPool: connpool.NewHybridPool(
//			20,            // maxPoolSize
//			5*time.Minute, // connectionTTL
//			5000,          // maxUsageCount
//		),
//		timePool: connpool.NewHybridPool(
//			20,            // maxPoolSize
//			5*time.Minute, // connectionTTL
//			5000,          // maxUsageCount
//		),
//	}
//
//	// Initialize single-flight downloader
//	cache.singleFlightDownloader = singleflight.New[string](
//		cache.downloadVersion,
//		singleflight.Config{
//			CleanupInterval: 5 * time.Minute,
//			PromiseTimeout:  15 * time.Minute,
//		})
//
//	if err := cache.initTimespansDB(); err != nil {
//		return nil, fmt.Errorf("failed to initialize timespans DB: %w", err)
//	}
//
//	cache.startCleanupRoutine()
//
//	return cache, nil
//}
//
//func (c *S3VersionedDataCache) GetVersionConn(versionFolder string) (interface{}, error) {
//	localPath, err := c.ensureVersionAvailable(versionFolder)
//	if err != nil {
//		return nil, err
//	}
//
//	dbPath := filepath.Join(localPath, "database.db")
//	return c.connPool.GetConnection(versionFolder, dbPath)
//}
//
//func (c *S3VersionedDataCache) GetVersionConnMultiplexed(versionFolder string) (interface{}, error) {
//	localPath, err := c.ensureVersionAvailable(versionFolder)
//	if err != nil {
//		return nil, err
//	}
//
//	dbPath := filepath.Join(localPath, "database.db")
//	return c.connPool.GetMultiplexedConnection(versionFolder, dbPath)
//}
//
//func (c *S3VersionedDataCache) ensureVersionAvailable(versionFolder string) (string, error) {
//	// Fast path: check memory cache first
//	c.cacheLock.RLock()
//	entry, exists := c.memCache[versionFolder]
//	c.cacheLock.RUnlock()
//
//	if exists {
//		// Update last accessed time
//		c.cacheLock.Lock()
//		entry.lastAccessed = time.Now()
//		c.cacheLock.Unlock()
//		return entry.localPath, nil
//	}
//
//	// Use single-flight downloader
//	ctx, _ := context.WithTimeout(context.Background(), 10*time.Minute)
//	result, err := c.singleFlightDownloader.Download(ctx, versionFolder)
//	if err != nil {
//		return "", err
//	}
//
//	localPath := result
//
//	// Add to memory cache
//	now := time.Now()
//	c.cacheLock.Lock()
//	c.memCache[versionFolder] = &cacheEntry{
//		localPath:    localPath,
//		downloadedAt: now,
//		lastAccessed: now,
//	}
//	c.cacheLock.Unlock()
//
//	return localPath, nil
//}
//
//func (c *S3VersionedDataCache) downloadVersion(ctx context.Context, versionFolder string) (string, error) {
//	localVersionPath := filepath.Join(c.localCacheDir, versionFolder)
//	if err := os.MkdirAll(localVersionPath, 0755); err != nil {
//		return "", err
//	}
//
//	// List objects in S3
//	input := &s3.ListObjectsV2Input{
//		Bucket: aws.String(c.s3Bucket),
//		Prefix: aws.String(versionFolder + "/"),
//	}
//
//	result, err := c.s3Client.ListObjectsV2(input)
//	if err != nil {
//		return "", err
//	}
//
//	for _, obj := range result.Contents {
//		if strings.HasSuffix(*obj.Key, ".db") {
//			fileName := filepath.Base(*obj.Key)
//			localFile := filepath.Join(localVersionPath, fileName)
//
//			file, err := os.Create(localFile)
//			if err != nil {
//				return "", err
//			}
//
//			_, err = c.downloader.Download(file, &s3.GetObjectInput{
//				Bucket: aws.String(c.s3Bucket),
//				Key:    obj.Key,
//			})
//
//			if closeErr := file.Close(); closeErr != nil {
//				return "", closeErr
//			}
//
//			if err != nil {
//				return "", err
//			}
//		}
//	}
//
//	// Verify download
//	if hasValidFiles, err := c.verifyVersionFolder(localVersionPath); err != nil || !hasValidFiles {
//		os.RemoveAll(localVersionPath)
//		return "", fmt.Errorf("verification failed for %s", versionFolder)
//	}
//
//	return localVersionPath, nil
//}
//
//func (c *S3VersionedDataCache) verifyVersionFolder(folderPath string) (bool, error) {
//	dbFile := filepath.Join(folderPath, "database.db")
//	if stat, err := os.Stat(dbFile); err != nil || stat.Size() == 0 {
//		return false, nil
//	}
//
//	if db, err := sql.Open("duckdb", dbFile); err != nil {
//		return false, nil
//	} else {
//		db.Close()
//	}
//
//	return true, nil
//}
//
//func (c *S3VersionedDataCache) UploadVersion(localFolderPath, versionFolder string) (*UploadResult, error) {
//	if _, err := os.Stat(localFolderPath); os.IsNotExist(err) {
//		return nil, fmt.Errorf("local folder not found: %s", localFolderPath)
//	}
//
//	cacheVersionPath := filepath.Join(c.localCacheDir, versionFolder)
//	needsCopy := !strings.HasPrefix(filepath.Clean(localFolderPath), filepath.Clean(c.localCacheDir))
//
//	if needsCopy {
//		if err := os.MkdirAll(cacheVersionPath, 0755); err != nil {
//			return nil, fmt.Errorf("failed to create cache directory: %w", err)
//		}
//	}
//
//	uploadedFiles := 0
//	totalSize := int64(0)
//
//	err := filepath.WalkDir(localFolderPath, func(path string, d fs.DirEntry, err error) error {
//		if err != nil {
//			return err
//		}
//
//		if !d.IsDir() && (strings.HasSuffix(path, ".db") || strings.HasSuffix(path, ".sql")) {
//			fileName := filepath.Base(path)
//			var uploadPath string
//			isDBFile := strings.HasSuffix(path, ".db")
//
//			if needsCopy && isDBFile {
//				cacheFile := filepath.Join(cacheVersionPath, fileName)
//				src, err := os.Open(path)
//				if err != nil {
//					return err
//				}
//
//				dst, err := os.Create(cacheFile)
//				if err != nil {
//					src.Close()
//					return err
//				}
//
//				_, err = io.Copy(dst, src)
//				src.Close()
//				dst.Close()
//				if err != nil {
//					return err
//				}
//
//				uploadPath = cacheFile
//			} else {
//				uploadPath = path
//			}
//
//			sourceFile, err := os.Open(uploadPath)
//			if err != nil {
//				return err
//			}
//
//			s3Key := fmt.Sprintf("%s/%s", versionFolder, fileName)
//			_, err = c.uploader.Upload(&s3manager.UploadInput{
//				Bucket:       aws.String(c.s3Bucket),
//				Key:          aws.String(s3Key),
//				Body:         sourceFile,
//				StorageClass: aws.String("STANDARD"),
//			})
//
//			stat, _ := sourceFile.Stat()
//			sourceFile.Close()
//
//			if err != nil {
//				return err
//			}
//
//			uploadedFiles++
//			totalSize += stat.Size()
//		}
//		return nil
//	})
//
//	if err != nil {
//		return nil, err
//	}
//
//	// Add to cache
//	now := time.Now()
//	c.cacheLock.Lock()
//	c.memCache[versionFolder] = &cacheEntry{
//		localPath:    cacheVersionPath,
//		downloadedAt: now,
//		lastAccessed: now,
//	}
//	c.cacheLock.Unlock()
//
//	return &UploadResult{
//		UploadedFiles:  uploadedFiles,
//		TotalSizeBytes: totalSize,
//		VersionFolder:  versionFolder,
//	}, nil
//}
//
//func (c *S3VersionedDataCache) CleanupExpired() error {
//	//c.connPool.PrintStatus("version_2024-09-19")
//	cutoff := time.Now().Add(-c.cacheTTL)
//
//	c.cacheLock.Lock()
//	defer c.cacheLock.Unlock()
//
//	var toDelete []string
//
//	for versionFolder, entry := range c.memCache {
//		if entry.lastAccessed.Before(cutoff) {
//			// Note: connpool doesn't have direct equivalents for GetActiveCount and ClearPoolForVersion
//			// We'll just remove the local files and let the pool's own cleanup handle the connections
//			if err := os.RemoveAll(entry.localPath); err != nil {
//				continue
//			}
//			toDelete = append(toDelete, versionFolder)
//		}
//	}
//
//	for _, versionFolder := range toDelete {
//		delete(c.memCache, versionFolder)
//	}
//
//	return nil
//}
//
//func (c *S3VersionedDataCache) startCleanupRoutine() {
//	c.cleanupTicker = time.NewTicker(5 * time.Second)
//	c.stopCleanup = make(chan struct{})
//
//	go func() {
//		for {
//			select {
//			case <-c.cleanupTicker.C:
//				if err := c.CleanupExpired(); err != nil {
//					fmt.Printf("Cache cleanup error: %v\n", err)
//				}
//			case <-c.stopCleanup:
//				return
//			}
//		}
//	}()
//}
//
//func (c *S3VersionedDataCache) initTimespansDB() error {
//	timespansPath := filepath.Join(c.localCacheDir, "timespans.db")
//
//	// Create timespans.db if it doesn't exist
//	if _, err := os.Stat(timespansPath); os.IsNotExist(err) {
//		db, err := sql.Open("duckdb", timespansPath)
//		if err != nil {
//			return err
//		}
//
//		query := `
//			CREATE TABLE IF NOT EXISTS timespans (
//				folder_name VARCHAR PRIMARY KEY,
//				start_date DATE
//			)
//		`
//		_, err = db.Exec(query)
//		db.Close()
//		if err != nil {
//			return err
//		}
//	}
//
//	if err := c.PullTimespansFromS3(); err != nil {
//		fmt.Printf("failed to pull timespans from S3: %v\n", err)
//	}
//
//	return nil
//}
//
//func (c *S3VersionedDataCache) PullTimespansFromS3() error {
//	timespansFile := filepath.Join(c.localCacheDir, "timespans_temp.db")
//
//	file, err := os.Create(timespansFile)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//	defer os.Remove(timespansFile)
//
//	_, err = c.downloader.Download(file, &s3.GetObjectInput{
//		Bucket: aws.String(c.s3Bucket),
//		Key:    aws.String("timespans.db"),
//	})
//	if err != nil {
//		return err
//	}
//
//	// Get connection from timepool
//	conn, err := c.timePool.GetConnection("timespans", filepath.Join(c.localCacheDir, "timespans.db"))
//	if err != nil {
//		return err
//	}
//	defer conn.Close()
//
//	_, err = conn.Exec("DELETE FROM timespans")
//	if err != nil {
//		return err
//	}
//
//	_, err = conn.Exec(fmt.Sprintf("ATTACH '%s' AS temp_db", timespansFile))
//	if err != nil {
//		return err
//	}
//
//	_, err = conn.Exec("INSERT INTO timespans SELECT * FROM temp_db.timespans")
//	if err != nil {
//		return err
//	}
//
//	_, err = conn.Exec("DETACH temp_db")
//	return err
//}
//
//func (c *S3VersionedDataCache) UpdateTimespans(folderName string, startDate time.Time) error {
//	// Get connection from timepool
//	conn, err := c.timePool.GetConnection("timespans", filepath.Join(c.localCacheDir, "timespans.db"))
//	if err != nil {
//		return err
//	}
//	defer conn.Close()
//
//	query := `INSERT OR REPLACE INTO timespans (folder_name, start_date) VALUES (?, ?)`
//	_, err = conn.Exec(query, folderName, startDate)
//	if err != nil {
//		return err
//	}
//
//	_, err = conn.Exec("CHECKPOINT")
//	if err != nil {
//		return err
//	}
//
//	timespansFile := filepath.Join(c.localCacheDir, "timespans.db")
//	file, err := os.Open(timespansFile)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	_, err = c.uploader.Upload(&s3manager.UploadInput{
//		Bucket:       aws.String(c.s3Bucket),
//		Key:          aws.String("timespans.db"),
//		Body:         file,
//		StorageClass: aws.String("STANDARD"),
//	})
//	return err
//}
//
//func (c *S3VersionedDataCache) GetTimespansConn() (interface{}, error) {
//
//	return c.timePool.GetMultiplexedConnection("timespans", filepath.Join(c.localCacheDir, "timespans.db"))
//}
//
//func (c *S3VersionedDataCache) Shutdown() error {
//	if c.cleanupTicker != nil {
//		c.cleanupTicker.Stop()
//	}
//	if c.stopCleanup != nil {
//		close(c.stopCleanup)
//	}
//
//	c.singleFlightDownloader.Shutdown()
//	c.connPool.Shutdown()
//	c.timePool.Shutdown()
//
//	if err := os.RemoveAll(c.localCacheDir); err != nil {
//		return fmt.Errorf("failed to remove cache directory: %w", err)
//	}
//
//	if err := os.MkdirAll(c.localCacheDir, 0755); err != nil {
//		return fmt.Errorf("failed to recreate cache directory: %w", err)
//	}
//
//	c.cacheLock.Lock()
//	c.memCache = make(map[string]*cacheEntry)
//	c.cacheLock.Unlock()
//
//	return nil
//}
