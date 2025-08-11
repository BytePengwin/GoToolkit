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

// Connection represents a pooled database connection
type Connection struct {
	*sql.DB
	id           uint64
	name         string
	createdAt    time.Time
	multiplexer  *Multiplexer // nil if not multiplexed
	totalQueries int64        // atomic counter
}

// Multiplexer handles multiple concurrent queries on a single connection
type Multiplexer struct {
	requestCh chan *QueryRequest
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

type QueryRequest struct {
	query    string
	args     []interface{}
	resultCh chan QueryResult
}

type QueryResult struct {
	rows *sql.Rows
	err  error
}

// WrappedConn is returned to callers
type WrappedConn struct {
	*sql.DB
	pool        *HybridPool
	conn        *Connection
	multiplexed bool
}

// HybridPool manages connections with optional multiplexing
type HybridPool struct {
	// Connection storage - lock-free access for reads
	connections atomic.Value // map[string][]*Connection
	connMutex   sync.Mutex   // Only for writes
	nextConnID  uint64       // atomic

	// Round-robin counters per name
	rrCounters sync.Map // name -> *uint64

	// Pool configuration
	maxPoolSize    int
	connectionTTL  time.Duration
	maxUsageCount  int32
	queueThreshold int // simple queue length threshold

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Object pools
var (
	queryRequestPool = sync.Pool{
		New: func() interface{} {
			return &QueryRequest{resultCh: make(chan QueryResult, 1)}
		},
	}
)

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

// GetConnection returns a simple pooled connection
func (hp *HybridPool) GetConnection(name, dbPath string) (*WrappedConn, error) {
	timer := profiling.Start(PoolAcquire)
	defer timer.End()

	conn, err := hp.getOrCreateConnection(name, dbPath, false)
	if err != nil {
		return nil, err
	}
	return &WrappedConn{DB: conn.DB, pool: hp, conn: conn, multiplexed: false}, nil
}

// GetMultiplexedConnection returns a connection with multiplexing
func (hp *HybridPool) GetMultiplexedConnection(name, dbPath string) (*WrappedConn, error) {
	timer := profiling.Start(PoolAcquire)
	defer timer.End()

	conn, err := hp.getOrCreateConnection(name, dbPath, true)
	if err != nil {
		return nil, err
	}
	return &WrappedConn{DB: conn.DB, pool: hp, conn: conn, multiplexed: true}, nil
}

func (hp *HybridPool) getOrCreateConnection(name, dbPath string, needsMultiplexing bool) (*Connection, error) {
	// Fast path: try existing connection with round-robin
	if conn := hp.selectConnection(name, needsMultiplexing); conn != nil {
		return conn, nil
	}

	// Slow path: create new connection
	return hp.createConnection(name, dbPath, needsMultiplexing)
}

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

func (wc *WrappedConn) Close() error {
	timer := profiling.Start(PoolRelease)
	defer timer.End()

	// No-op for pooled connections
	return nil
}

// Helper methods
func (hp *HybridPool) getConnections(name string) []*Connection {
	conns := hp.connections.Load().(map[string][]*Connection)
	return conns[name]
}

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

func (hp *HybridPool) returnQueryRequest(req *QueryRequest) {
	req.query = ""
	req.args = nil
	queryRequestPool.Put(req)
}

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
