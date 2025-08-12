# GoToolkit

A comprehensive collection of Go packages providing utilities for database connection pooling, caching, performance profiling, and concurrent operations.

## Repository

This toolkit is hosted at [https://github.com/BytePengwin/GoToolkit](https://github.com/BytePengwin/GoToolkit)

## Packages

- **connectionpool**: Advanced connection pooling for SQL databases with support for multiplexing
- **duckdbcache**: Specialized caching system for DuckDB with S3 integration
- **profiling**: Performance measurement and profiling utilities with CSV export
- **singleflight**: Concurrent-safe downloader that prevents duplicate work

## Features

- **Efficient Resource Management**: Connection pooling with TTL and usage limits
- **Performance Optimization**: Query multiplexing and connection reuse
- **Observability**: Built-in profiling for performance monitoring
- **Concurrency Control**: Prevent duplicate work with singleflight pattern
- **Cloud Integration**: S3 support for DuckDB caching

## Installation

```bash
go get github.com/BytePengwin/GoToolkit
```

## Usage Examples

### Connection Pooling

```go
package main

import (
    "log"
    "time"

    "github.com/BytePengwin/GoToolkit/connectionpool"
)

func main() {
    // Create a connection pool
    pool := connectionpool.NewHybridPool(
        10,                  // Maximum pool size
        30*time.Minute,      // Connection TTL
        1000,                // Max usage count
    )
    defer pool.Shutdown()

    // Get a connection
    conn, err := pool.GetConnection("mydb", "/path/to/database.db")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Execute a query
    rows, err := conn.Query("SELECT * FROM mytable")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    // Process results
    for rows.Next() {
        // Read data from rows...
    }
}
```

### Performance Profiling

```go
package main

import (
    "fmt"

    "github.com/BytePengwin/GoToolkit/profiling"
)

// Define an operation type
type MyOp string
func (o MyOp) String() string { return string(o) }

func main() {
    // Time a function execution
    profiling.Time(MyOp("database_query"), func() {
        // Database query code here
    })

    // Manual timing
    timer := profiling.Start(MyOp("complex_operation"))
    // Do work...
    timer.End()

    // Print results
    profiling.Print()

    // Export to CSV
    profiling.ExportCSV("performance_results.csv")
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
