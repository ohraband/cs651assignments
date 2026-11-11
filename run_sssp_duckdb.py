#!/usr/bin/env python3

import duckdb


def expand_frontier(con, frontier_in, frontier_out):
    """Takes in the name of a table with (node, distance) pairs containing all currently
    reachable nodes and expands the frontier by one hop.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to DuckDB.
    frontier_in : str
        Table containing reachable (node, distance) pairs.
    frontier_out : str
        Table containing reachable (node, distance) pairs after expanding the frontier by one hop.
    """
    # BEGIN OF YOUR CODE
    # Write your code below.

    con.execute(f"""
        CREATE OR REPLACE TABLE {frontier_out} AS
        SELECT 
            node, 
            MIN(dist) AS dist

        FROM (
            SELECT
                node,
                dist
            FROM {frontier_in}
            
            UNION ALL
            
            SELECT
                t2.dst AS node,
                t1.dist + t2.weight AS dist
            FROM {frontier_in} AS t1 
            JOIN edges AS t2  
                ON t1.node = t2.src
                
        ) AS combined_paths
        GROUP BY node;
    """)

    # END OF YOUR CODE


# BEGIN OF YOUR CODE
# Write your code below.

# Other helper functions here as needed...


# END OF YOUR CODE


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--edges", required=True, help="edges.csv (src,dst,weight)")
    ap.add_argument("--iterations", required=True, type=int, help="Numer of iterations to run")
    ap.add_argument("--source", required=True, type=str, help="Source of SSSP")
    ap.add_argument("--output", required=True, type=str, help="Output file")
    args = ap.parse_args()

    con = duckdb.connect()

    # Load edges
    con.execute(f"""
        CREATE OR REPLACE TABLE edges AS
        SELECT
          src::VARCHAR    AS src,
          dst::VARCHAR    AS dst,
          weight::INT     AS weight
        FROM read_csv_auto('{args.edges}');
    """)

    con.execute(f"CREATE OR REPLACE TABLE reachable000 (node VARCHAR(255), dist INT);")
    con.execute(f"INSERT INTO reachable000 (node, dist) VALUES ('{args.source}', 0);")

    for i in range(1, args.iterations+1):
        table_in = f"reachable{i-1:03}"
        table_out = f"reachable{i:03}"
        print(f"Iteration {i}: reading from {table_in}, writing to {table_out}")
        expand_frontier(con, table_in, table_out)

    # Note: DuckDB's ORDER BY is inside the SELECT/FROM/WHERE, so we use a final SELECT for ordering.
    results = con.execute(f"SELECT * FROM reachable{args.iterations:03} ORDER BY dist, node ASC;")

    rows = results.fetchall()
    with open(args.output, "w") as out_f:
        for row in rows:
            out_f.write(f"{row[0]},{row[1]}\n")

    print(f"-> wrote output to {args.output}")

    con.close()