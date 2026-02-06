// Package presto provides a Go client library for Presto and Trino SQL query engines.
//
// The client communicates with Presto/Trino coordinators via the REST API,
// supporting query execution, batch result streaming, session management,
// and automatic transaction state tracking.
//
// # Getting Started
//
// Create a client and execute a query:
//
//	client, err := presto.NewClient("http://presto-coordinator:8080")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	session := client.NewSession()
//	session.Catalog("hive").Schema("default")
//
//	results, _, err := session.Query(ctx, "SELECT * FROM my_table")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Sessions
//
// Sessions provide isolated execution contexts with their own catalog, schema,
// user identity, transaction state, and session parameters. Sessions are
// thread-safe and can be cloned for parallel workloads:
//
//	s1 := client.NewSession().Catalog("hive").Schema("prod")
//	s2 := s1.Clone().Schema("staging")
//
// # Result Streaming
//
// Large result sets are returned in batches. Use Drain for memory-efficient
// streaming, or FetchNextBatch for manual iteration:
//
//	err = results.Drain(ctx, func(qr *presto.QueryResults) error {
//	    for _, row := range qr.Data {
//	        // process row
//	    }
//	    return nil
//	})
//
// # Trino Compatibility
//
// The client supports both Presto and Trino by automatically translating
// protocol headers:
//
//	client.IsTrino(true)
package presto
