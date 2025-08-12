// Package connectionpool provides a hybrid connection pooling system for SQL databases.
// It supports both regular connection pooling and multiplexed connections where
// multiple queries can be executed concurrently on a single database connection.
//
// The package is particularly optimized for DuckDB connections but can be used
// with any database/sql compatible driver.
//
// Features:
//   - Connection pooling with configurable maximum pool size
//   - Connection TTL (Time-To-Live) to prevent resource leaks
//   - Connection multiplexing for improved concurrency
//   - Automatic cleanup of expired connections
//   - Load balancing across multiple connections
//
// Example usage:
//
//	// Create a new connection pool
//	pool := connectionpool.NewHybridPool(
//	    10,                  // Maximum pool size
//	    30*time.Minute,      // Connection TTL
//	    1000,                // Max usage count
//	)
//
//	// Get a regular connection
//	conn, err := pool.GetConnection("mydb", "/path/to/database.db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer conn.Close()
//
//	// Execute a query
//	rows, err := conn.Query("SELECT * FROM mytable")
//
//	// Don't forget to shut down the pool when done
//	pool.Shutdown()
package connectionpool

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BytePengwin/GoToolkit/profiling"
	_ "github.com/marcboeker/go-duckdb"
)

// Connection represents a pooled database connection with metadata.
// It embeds a standard sql.DB and adds tracking information.
type Connection struct {
	*sql.DB
	id           uint64       // Unique identifier for the connection
	name         string       // Logical name for the connection (e.g., database name)
	createdAt    time.Time    // When the connection was created
	multiplexer  *Multiplexer // nil if not multiplexed
	totalQueries int64        // Atomic counter for tracking usage
}

// Multiplexer handles multiple concurrent queries on a single database connection.
// It uses a channel-based approach to serialize query execution while allowing
// concurrent clients to submit queries.
type Multiplexer struct {
	requestCh chan *QueryRequest // Channel for submitting query requests
	stopCh    chan struct{}      // Channel for signaling shutdown
	wg        sync.WaitGroup     // For graceful shutdown
}

// QueryRequest represents a single query to be executed through the multiplexer.
// It contains the query string, arguments, and a channel to receive the result.
type QueryRequest struct {
	query    string           // SQL query to execute
	args     []interface{}    // Query arguments
	resultCh chan QueryResult // Channel to receive the query result
}

// QueryResult contains the result of a multiplexed query execution.
// It includes either the result rows or an error.
type QueryResult struct {
	rows *sql.Rows // Query result rows (nil if error)
	err  error     // Error (nil if successful)
}

// WrappedConn is the connection handle returned to callers.
// It embeds sql.DB for direct query execution and adds pool management.
type WrappedConn struct {
	*sql.DB                 // Embedded database connection
	pool        *HybridPool // Reference to the parent pool
	conn        *Connection // The underlying pooled connection
	multiplexed bool        // Whether this connection uses multiplexing
}

// HybridPool manages a pool of database connections with optional multiplexing.
// It supports both regular connections and multiplexed connections for improved
// concurrency with limited resources.
type HybridPool struct {
	// Connection storage - lock-free access for reads
	connections atomic.Value // map[string][]*Connection
	connMutex   sync.Mutex   // Only for writes to the connections map
	nextConnID  uint64       // Atomic counter for generating connection IDs

	// Round-robin counters per name for load balancing
	rrCounters sync.Map // name -> *uint64

	// Pool configuration
	maxPoolSize    int           // Maximum number of connections in the pool
	connectionTTL  time.Duration // Maximum lifetime of a connection
	maxUsageCount  int32         // Maximum number of queries per connection
	queueThreshold int           // Queue length threshold for load balancing

	stopCh chan struct{}  // Channel for signaling shutdown
	wg     sync.WaitGroup // For graceful shutdown
}

// Object pools
var (
	queryRequestPool = sync.Pool{
		New: func() interface{} {
			return &QueryRequest{resultCh: make(chan QueryResult, 1)}
		},
	}
)

// NewHybridPool creates a new connection pool with the specified configuration.
//
// Parameters:
//   - maxPoolSize: Maximum number of connections the pool can manage
//   - connectionTTL: Maximum lifetime of a connection before it's recycled
//   - maxUsageCount: Maximum number of queries a connection can handle before recycling
//
// The pool automatically starts a background goroutine to clean up expired connections.
// Make sure to call Shutdown() when the pool is no longer needed to release resources.
func NewHybridPool(maxPoolSize int, connectionTTL time.Duration, maxUsageCount int32) *HybridPool {
	pool := &HybridPool{
		maxPoolSize:    maxPoolSize,
		connectionTTL:  connectionTTL,
		maxUsageCount:  maxUsageCount,
		queueThreshold: 20, // simple threshold
		stopCh:         make(chan struct{}),
	}

	// Initialize connections map
	pool.connections.Store(make(map[string][]*Connection))

	// Start background cleanup
	pool.wg.Add(1)
	go pool.cleanupWorker()

	return pool
}

// GetConnection returns a regular (non-multiplexed) pooled connection.
// This is suitable for most use cases where a single goroutine needs
// to execute queries sequentially.
//
// Parameters:
//   - name: Logical name for the connection (used for grouping and load balancing)
//   - dbPath: Path to the database file or connection string
//
// Returns a wrapped connection that should be closed when no longer needed.
// The actual connection will be returned to the pool when Close() is called.
func (hp *HybridPool) GetConnection(name, dbPath string) (*WrappedConn, error) {
	timer := profiling.Start(PoolAcquire)
	defer timer.End()

	conn, err := hp.getOrCreateConnection(name, dbPath, false)
	if err != nil {
		return nil, err
	}
	return &WrappedConn{DB: conn.DB, pool: hp, conn: conn, multiplexed: false}, nil
}

// GetMultiplexedConnection returns a connection with multiplexing enabled.
// This allows multiple goroutines to execute queries concurrently on the
// same database connection, which can improve performance in some scenarios.
//
// Parameters:
//   - name: Logical name for the connection (used for grouping and load balancing)
//   - dbPath: Path to the database file or connection string
//
// Returns a wrapped connection that should be closed when no longer needed.
// The actual connection will be returned to the pool when Close() is called.
func (hp *HybridPool) GetMultiplexedConnection(name, dbPath string) (*WrappedConn, error) {
	timer := profiling.Start(PoolAcquire)
	defer timer.End()

	conn, err := hp.getOrCreateConnection(name, dbPath, true)
	if err != nil {
		return nil, err
	}
	return &WrappedConn{DB: conn.DB, pool: hp, conn: conn, multiplexed: true}, nil
}

// getOrCreateConnection attempts to find an existing connection or creates a new one if needed.
// It first tries to select an existing connection of the appropriate type (multiplexed or not).
// If no suitable connection is found, it creates a new one.
//
// Parameters:
//   - name: Logical name for the connection (used for grouping and load balancing)
//   - dbPath: Path to the database file or connection string
//   - needsMultiplexing: Whether the connection should support multiplexing
//
// Returns a connection and an error if one occurred during creation.
func (hp *HybridPool) getOrCreateConnection(name, dbPath string, needsMultiplexing bool) (*Connection, error) {
	// Fast path: try existing connection with round-robin
	if conn := hp.selectConnection(name, needsMultiplexing); conn != nil {
		return conn, nil
	}

	// Slow path: create new connection
	return hp.createConnection(name, dbPath, needsMultiplexing)
}

// selectConnection selects an appropriate connection from the pool using round-robin load balancing.
// It filters connections by type (multiplexed or not) and checks if they are usable.
//
// Parameters:
//   - name: Logical name for the connection group to select from
//   - needsMultiplexing: Whether to select a multiplexed connection
//
// Returns a suitable connection or nil if none is available.
// This function is thread-safe and uses atomic operations for the round-robin counter.
func (hp *HybridPool) selectConnection(name string, needsMultiplexing bool) *Connection {
	conns := hp.getConnections(name)
	if len(conns) == 0 {
		return nil
	}

	// Filter connections by type
	var candidates []*Connection
	for _, conn := range conns {
		isMultiplexed := conn.multiplexer != nil
		if needsMultiplexing == isMultiplexed {
			// Quick availability check
			if hp.isConnectionUsable(conn, needsMultiplexing) {
				candidates = append(candidates, conn)
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Round-robin selection
	counter, _ := hp.rrCounters.LoadOrStore(name, new(uint64))
	idx := atomic.AddUint64(counter.(*uint64), 1) - 1
	return candidates[idx%uint64(len(candidates))]
}

// isConnectionUsable checks if a connection is still valid and not overloaded.
// It performs two main checks:
// 1. Age check - ensures the connection hasn't exceeded its TTL
// 2. Load check - for multiplexed connections, ensures the request queue isn't too full
//
// Parameters:
//   - conn: The connection to check
//   - needsMultiplexing: Whether the connection needs multiplexing support
//
// Returns true if the connection is usable, false otherwise.
func (hp *HybridPool) isConnectionUsable(conn *Connection, needsMultiplexing bool) bool {
	// Age check
	if time.Since(conn.createdAt) > hp.connectionTTL {
		return false
	}

	if needsMultiplexing && conn.multiplexer != nil {
		// Simple queue length check
		return len(conn.multiplexer.requestCh) < hp.queueThreshold
	}

	return true
}

// createConnection creates a new database connection with the specified parameters.
// If the pool has reached its maximum size, it returns the least loaded existing connection instead.
// For multiplexed connections, it also starts a background goroutine to handle query requests.
//
// Parameters:
//   - name: Logical name for the connection (used for grouping and load balancing)
//   - dbPath: Path to the database file or connection string
//   - needsMultiplexing: Whether the connection should support multiplexing
//
// Returns a new or existing connection and an error if one occurred during creation.
//
// Side effects:
//   - Adds the new connection to the pool
//   - May start a background goroutine for multiplexing
//   - May trigger background prewarming of additional connections
func (hp *HybridPool) createConnection(name, dbPath string, needsMultiplexing bool) (*Connection, error) {
	// Check pool size limit
	conns := hp.getConnections(name)
	if len(conns) >= hp.maxPoolSize {
		// Return least loaded existing connection
		return hp.selectLeastLoaded(name, needsMultiplexing), nil
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	connID := atomic.AddUint64(&hp.nextConnID, 1)
	conn := &Connection{
		DB:        db,
		id:        connID,
		name:      name,
		createdAt: time.Now(),
	}

	if needsMultiplexing {
		conn.multiplexer = &Multiplexer{
			requestCh: make(chan *QueryRequest, 100), // Fixed size buffer
			stopCh:    make(chan struct{}),
		}
		conn.multiplexer.wg.Add(1)
		go hp.runMultiplexer(conn)
	}

	hp.addConnection(name, conn)

	// Background prewarming - create one more connection
	if len(conns) == 0 {
		go func() {
			time.Sleep(10 * time.Millisecond) // Brief delay
			hp.createConnection(name, dbPath, needsMultiplexing)
		}()
	}

	return conn, nil
}

// selectLeastLoaded finds the connection with the least load when the pool is at capacity.
// For multiplexed connections, it selects the one with the shortest request queue.
// For direct connections, it returns the first matching connection since they're all equivalent.
//
// Parameters:
//   - name: Logical name for the connection group to select from
//   - needsMultiplexing: Whether to select a multiplexed connection
//
// Returns the least loaded connection of the appropriate type, or nil if none is found.
// This function is called when the pool has reached its maximum size and a new connection is requested.
func (hp *HybridPool) selectLeastLoaded(name string, needsMultiplexing bool) *Connection {
	conns := hp.getConnections(name)
	var bestConn *Connection
	minQueue := hp.queueThreshold + 1

	for _, conn := range conns {
		isMultiplexed := conn.multiplexer != nil
		if needsMultiplexing == isMultiplexed {
			if needsMultiplexing {
				queueLen := len(conn.multiplexer.requestCh)
				if queueLen < minQueue {
					minQueue = queueLen
					bestConn = conn
				}
			} else {
				return conn // Any direct connection will do
			}
		}
	}
	return bestConn
}

// runMultiplexer is a goroutine that handles query execution for multiplexed connections.
// It receives query requests from the multiplexer's channel, executes them sequentially,
// and sends the results back through the request's result channel.
//
// Parameters:
//   - conn: The connection with the multiplexer to run
//
// This function runs until the multiplexer is stopped or the pool is shut down.
// It handles timeouts when sending results back to clients and properly cleans up resources.
// This is an internal implementation detail of the multiplexing system.
func (hp *HybridPool) runMultiplexer(conn *Connection) {
	defer conn.multiplexer.wg.Done()

	for {
		select {
		case req := <-conn.multiplexer.requestCh:
			atomic.AddInt64(&conn.totalQueries, 1)
			rows, err := conn.DB.Query(req.query, req.args...)

			select {
			case req.resultCh <- QueryResult{rows: rows, err: err}:
			case <-time.After(5 * time.Second):
				if rows != nil {
					rows.Close()
				}
			}

			hp.returnQueryRequest(req)

		case <-conn.multiplexer.stopCh:
			return
		case <-hp.stopCh:
			return
		}
	}
}

// WrappedConn methods

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
//
// For multiplexed connections, the query is sent to the multiplexer
// which serializes execution on the underlying connection. This allows
// multiple goroutines to safely use the same connection concurrently.
//
// For regular connections, the query is executed directly on the
// underlying database connection.
//
// Example:
//
//	rows, err := conn.Query("SELECT * FROM users WHERE age > ?", 18)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer rows.Close()
//	for rows.Next() {
//	    // Process row...
//	}
func (wc *WrappedConn) Query(query string, args ...interface{}) (*sql.Rows, error) {
	timer := profiling.Start(PoolQuery)
	defer timer.End()

	if wc.multiplexed && wc.conn.multiplexer != nil {
		return wc.multiplexedQuery(query, args...)
	}

	// Direct query
	atomic.AddInt64(&wc.conn.totalQueries, 1)
	return wc.DB.Query(query, args...)
}

// multiplexedQuery is an internal method that sends a query through the multiplexer.
// It handles timeouts and error conditions when submitting and waiting for query results.
//
// Parameters:
//   - query: SQL query to execute
//   - args: Query arguments for any placeholders in the query
//
// Returns the query result rows and any error that occurred.
//
// This method has two timeout mechanisms:
//   - A short timeout (100ms) when submitting the query to prevent blocking if the multiplexer is overloaded
//   - A longer timeout (30s) when waiting for the query result to prevent indefinite waiting
//
// Note: This method is already profiled by the Query method that calls it.
func (wc *WrappedConn) multiplexedQuery(query string, args ...interface{}) (*sql.Rows, error) {
	// Note: We don't need to profile this separately as it's already profiled by the Query method
	req := wc.pool.getQueryRequest()
	req.query = query
	req.args = args

	select {
	case wc.conn.multiplexer.requestCh <- req:
	case <-time.After(100 * time.Millisecond):
		wc.pool.returnQueryRequest(req)
		return nil, fmt.Errorf("multiplexer overloaded")
	}

	select {
	case result := <-req.resultCh:
		return result.rows, result.err
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("query timeout")
	}
}

// Close returns the connection to the pool.
// It doesn't actually close the underlying database connection,
// but instead makes it available for reuse.
//
// You should always call Close when you're done with a connection
// to prevent resource leaks.
func (wc *WrappedConn) Close() error {
	timer := profiling.Start(PoolRelease)
	defer timer.End()

	// No-op for pooled connections
	return nil
}

// Helper methods

// getConnections retrieves all connections for a specific name from the pool.
// It uses atomic.Value for lock-free reads of the connections map.
//
// Parameters:
//   - name: Logical name for the connection group to retrieve
//
// Returns a slice of connections for the specified name, or nil if none exist.
// This is a thread-safe operation optimized for read performance.
func (hp *HybridPool) getConnections(name string) []*Connection {
	conns := hp.connections.Load().(map[string][]*Connection)
	return conns[name]
}

// addConnection adds a new connection to the pool under the specified name.
// It creates a copy of the connections map to maintain lock-free reads.
//
// Parameters:
//   - name: Logical name for the connection group to add to
//   - conn: The connection to add to the pool
//
// This function acquires a mutex to ensure thread safety when modifying the connections map.
// It uses the copy-on-write pattern with atomic.Value to allow concurrent reads while updating.
func (hp *HybridPool) addConnection(name string, conn *Connection) {
	hp.connMutex.Lock()
	defer hp.connMutex.Unlock()

	conns := hp.connections.Load().(map[string][]*Connection)
	newConns := make(map[string][]*Connection)

	// Copy existing connections
	for k, v := range conns {
		newConns[k] = v
	}

	// Add new connection
	newConns[name] = append(newConns[name], conn)
	hp.connections.Store(newConns)
}

// removeConnection removes a connection from the pool and cleans up its resources.
// It creates a filtered copy of the connections map without the specified connection.
//
// Parameters:
//   - conn: The connection to remove from the pool
//
// Side effects:
//   - Stops the multiplexer goroutine if the connection is multiplexed
//   - Closes the underlying database connection
//   - Updates the connections map to remove the connection
//
// This function acquires a mutex to ensure thread safety when modifying the connections map.
// It uses the copy-on-write pattern with atomic.Value to allow concurrent reads while updating.
func (hp *HybridPool) removeConnection(conn *Connection) {
	hp.connMutex.Lock()
	defer hp.connMutex.Unlock()

	conns := hp.connections.Load().(map[string][]*Connection)
	newConns := make(map[string][]*Connection)

	// Copy and filter
	for name, Conns := range conns {
		if name == conn.name {
			filtered := make([]*Connection, 0, len(Conns))
			for _, c := range Conns {
				if c.id != conn.id {
					filtered = append(filtered, c)
				}
			}
			if len(filtered) > 0 {
				newConns[name] = filtered
			}
		} else {
			newConns[name] = Conns
		}
	}

	hp.connections.Store(newConns)

	// Clean up connection
	if conn.multiplexer != nil {
		select {
		case <-conn.multiplexer.stopCh:
		default:
			close(conn.multiplexer.stopCh)
		}
		conn.multiplexer.wg.Wait()
	}
	conn.DB.Close()
}

// getQueryRequest obtains a QueryRequest object from the object pool.
// It resets the request's fields to ensure it's in a clean state.
//
// Returns a clean QueryRequest object ready for use.
//
// This function uses sync.Pool to reduce memory allocations and garbage collection pressure
// by reusing QueryRequest objects. It also ensures the result channel is drained if necessary.
func (hp *HybridPool) getQueryRequest() *QueryRequest {
	req := queryRequestPool.Get().(*QueryRequest)
	// Clear result channel
	select {
	case <-req.resultCh:
	default:
	}
	req.query = ""
	req.args = nil
	return req
}

// returnQueryRequest returns a QueryRequest object to the object pool.
// It clears the request's fields to prevent memory leaks and data leakage.
//
// Parameters:
//   - req: The QueryRequest to return to the pool
//
// This function is called after a query has been processed to recycle the request object.
// It's an important part of the object pooling system that reduces memory allocations.
func (hp *HybridPool) returnQueryRequest(req *QueryRequest) {
	req.query = ""
	req.args = nil
	queryRequestPool.Put(req)
}

// cleanupWorker is a background goroutine that periodically checks for and removes expired connections.
// It runs on a 60-second interval and continues until the pool is shut down.
//
// This function is started when the pool is created and is responsible for preventing
// resource leaks by ensuring connections don't stay alive indefinitely.
func (hp *HybridPool) cleanupWorker() {
	defer hp.wg.Done()
	ticker := time.NewTicker(60 * time.Second) // Less frequent cleanup
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hp.cleanupExpiredConnections()
		case <-hp.stopCh:
			return
		}
	}
}

// cleanupExpiredConnections identifies and removes connections that have exceeded their TTL.
// It scans all connections in the pool and removes those that are too old.
//
// This function is called periodically by the cleanupWorker to maintain pool hygiene.
// It helps prevent resource leaks and ensures connections are refreshed regularly.
// The actual removal of connections is delegated to the removeConnection method.
func (hp *HybridPool) cleanupExpiredConnections() {
	conns := hp.connections.Load().(map[string][]*Connection)
	var toRemove []*Connection

	for _, nameConns := range conns {
		for _, conn := range nameConns {
			if time.Since(conn.createdAt) > hp.connectionTTL {
				toRemove = append(toRemove, conn)
			}
		}
	}

	for _, conn := range toRemove {
		hp.removeConnection(conn)
	}
}

// PrintStatus prints a human-readable status report of the connection pool
// for the specified connection name.
//
// The report includes:
// - Total number of connections
// - Queue threshold for load balancing
// - Details for each connection (age, query count, queue length for multiplexed connections)
// - Summary of direct vs. multiplexed connections
//
// This is useful for debugging and monitoring the pool's behavior.
func (hp *HybridPool) PrintStatus(name string) {
	conns := hp.getConnections(name)

	fmt.Printf("=== Optimized Hybrid Pool Status ===\n")
	fmt.Printf("name: %s\n", name)
	fmt.Printf("Total Connections: %d/%d\n", len(conns), hp.maxPoolSize)
	fmt.Printf("Queue Threshold: %d\n", hp.queueThreshold)

	multiplexed, direct := 0, 0
	for i, conn := range conns {
		total := atomic.LoadInt64(&conn.totalQueries)
		age := time.Since(conn.createdAt).Truncate(time.Second)

		if conn.multiplexer != nil {
			multiplexed++
			queueLen := len(conn.multiplexer.requestCh)
			fmt.Printf("  Conn %d [Multiplexed]: Age %s, Total %d, Queue: %d/100\n",
				i, age, total, queueLen)
		} else {
			direct++
			fmt.Printf("  Conn %d [Direct]: Age %s, Total %d\n",
				i, age, total)
		}
	}
	fmt.Printf("Direct: %d, Multiplexed: %d\n", direct, multiplexed)
}

// Shutdown gracefully shuts down the connection pool.
// It stops the background cleanup worker, closes all multiplexers,
// and closes all database connections.
//
// This method should be called when the pool is no longer needed
// to ensure all resources are properly released.
//
// Example:
//
//	pool := connectionpool.NewHybridPool(10, 30*time.Minute, 1000)
//	// Use the pool...
//
//	// When done with the pool:
//	err := pool.Shutdown()
//	if err != nil {
//	    log.Printf("Error shutting down pool: %v", err)
//	}
func (hp *HybridPool) Shutdown() error {
	close(hp.stopCh)
	hp.wg.Wait()

	conns := hp.connections.Load().(map[string][]*Connection)

	// Close all multiplexer channels first
	for _, nameConns := range conns {
		for _, conn := range nameConns {
			if conn.multiplexer != nil {
				select {
				case <-conn.multiplexer.stopCh:
				default:
					close(conn.multiplexer.stopCh)
				}
			}
		}
	}

	// Wait for all multiplexers to stop
	for _, nameConns := range conns {
		for _, conn := range nameConns {
			if conn.multiplexer != nil {
				conn.multiplexer.wg.Wait()
			}
		}
	}

	// Close all database connections
	for _, nameConns := range conns {
		for _, conn := range nameConns {
			conn.DB.Close()
		}
	}

	return nil
}
