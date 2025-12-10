"""
MVP Demo of Parallel DuckDB - Thread-safe DuckDB access using official cursor approach.

This demonstrates DuckDB's recommended approach for using connections across multiple threads:
- Create one connection
- Each thread calls .cursor() to get a thread-local cursor
- Use cursors for all database operations

Reference: https://duckdb.org/docs/stable/guides/python/multiple_threads
"""

import time
from concurrent.futures import ThreadPoolExecutor
import duckdb


def worker_thread(main_con, thread_id: int, n_operations: int):
    """
    Worker function that performs database operations from a thread.

    Each thread:
    1. Creates a thread-local cursor using .cursor()
    2. Performs multiple INSERT and SELECT operations
    3. DuckDB handles concurrency internally

    Args:
        main_con: The main DuckDB connection (passed to thread)
        thread_id: ID of this thread
        n_operations: Number of operations to perform
    """
    # Create a thread-local cursor - this is the DuckDB recommended approach!
    local_cursor = main_con.cursor()

    print(f"Thread {thread_id}: Starting {n_operations} operations...")

    for i in range(n_operations):
        # Insert a record using the thread-local cursor
        local_cursor.execute(f"""
            INSERT INTO test_table (thread_id, operation_num, data)
            VALUES ({thread_id}, {i}, 'Thread {thread_id} - Op {i}')
        """)

        # Simulate some processing time
        time.sleep(0.01)

        # Query the data using the thread-local cursor
        result = local_cursor.execute(f"""
            SELECT COUNT(*) as count
            FROM test_table
            WHERE thread_id = {thread_id}
        """).fetchone()

        if (i + 1) % 10 == 0:
            print(f"Thread {thread_id}: Completed {i + 1} operations, inserted {result[0]} rows")

    # Close the thread-local cursor when done
    local_cursor.close()
    print(f"Thread {thread_id}: Finished all operations")


def main():
    print("=== DuckDB Parallel Execution Demo (Official Cursor Approach) ===\n")

    # Step 1: Create a DuckDB connection
    print("Step 1: Creating DuckDB connection...")
    con = duckdb.connect(':memory:')  # In-memory database for demo
    print("✓ Connection created\n")

    # Step 2: Create test table
    print("Step 2: Creating test table...")
    con.execute("CREATE SEQUENCE seq START 1")
    con.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq'),
            thread_id INTEGER,
            operation_num INTEGER,
            data VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Table created\n")

    # Step 3: Run parallel operations
    n_threads = 20
    n_operations_per_thread = 1000

    print(f"Step 3: Spawning {n_threads} worker threads...")
    print(f"Each thread will perform {n_operations_per_thread} INSERT and SELECT operations")
    print("Each thread creates its own cursor using .cursor()\n")

    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = []
            for thread_id in range(n_threads):
                # Pass the main connection to each thread
                future = executor.submit(worker_thread, con, thread_id, n_operations_per_thread)
                futures.append(future)

            # Wait for all threads to complete
            for future in futures:
                future.result()

    except Exception as e:
        print(f"\n❌ Error during parallel execution: {e}")
        import traceback
        traceback.print_exc()
        return

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"\n✓ All threads completed in {elapsed:.2f} seconds\n")

    # Step 4: Verify results
    print("Step 4: Verifying results...")

    total_count = con.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
    expected_count = n_threads * n_operations_per_thread

    print(f"Total rows inserted: {total_count}")
    print(f"Expected rows: {expected_count}")

    if total_count == expected_count:
        print("✓ Row count matches!\n")
    else:
        print(f"❌ Row count mismatch! Expected {expected_count}, got {total_count}\n")
        return

    # Show breakdown by thread
    print("Breakdown by thread:")
    results = con.execute("""
        SELECT thread_id, COUNT(*) as count
        FROM test_table
        GROUP BY thread_id
        ORDER BY thread_id
    """).fetchall()

    for thread_id, count in results:
        print(f"  Thread {thread_id}: {count} rows")

    # Step 5: Cleanup
    print("\nStep 5: Cleaning up...")
    con.close()
    print("✓ Connection closed\n")

    print("=== Demo completed successfully! ===")
    print("\nKey takeaways:")
    print("1. ✓ Single DuckDB connection created")
    print("2. ✓ Connection passed to threads (can be safely passed!)")
    print("3. ✓ Each thread calls .cursor() to get thread-local cursor")
    print("4. ✓ DuckDB handles concurrency internally")
    print("5. ✓ No SIGABRT errors or connection conflicts")
    print("6. ✓ Official DuckDB-recommended approach for multi-threading")


if __name__ == "__main__":
    main()
