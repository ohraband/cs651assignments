#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession


def expand_frontier(sc, frontier_in, frontier_out, edges_rdd):
    """Takes in the path of a directory with (node, distance) pairs containing all currently
    reachable nodes (in CSV) and expands the frontier by one hop.

    Parameters
    ----------
    sc : pyspark.SparkContext
        SparkContext for manipuating RDDs.
    frontier_in : str
        Directory containing reachable (node, distance) pairs from the previous iteration.
    frontier_out : str
        Directory containing reachable (node, distance) pairs after expanding the frontier by one hop.
    edges_rdd : pyspark.RDD
        RDD of edges, of type `RDD[(src, (dst, weight))]`
    """
    # BEGIN OF YOUR CODE
    # Write your code below.
    
    reachable_rdd = (
        sc.textFile(f"{frontier_in}/*")
        .map(lambda l: l.strip().split(","))
        .filter(lambda parts: len(parts) == 2)
        .map(lambda parts: (parts[0], int(parts[1])))
    ).cache()


    joined_rdd = reachable_rdd.join(edges_rdd)

    new_paths_rdd = joined_rdd.map(
        lambda x: (
            x[1][1][0],            #dst node
            x[1][0] + x[1][1][1]   #distance + edge
        )
    )

    all_paths_rdd = reachable_rdd.union(new_paths_rdd)

    min_paths_rdd = all_paths_rdd.reduceByKey(min)

    output_rdd = min_paths_rdd.map(
        lambda pair: f"{pair[0]},{pair[1]}"
    )

    output_rdd.saveAsTextFile(frontier_out)

    # END OF YOUR CODE


# BEGIN OF YOUR CODE
# Write your code below.

# Other helper functions here as needed...

# END OF YOUR CODE


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--edges", required=True, help="Path to edges.csv (src,dst,weight)")
    ap.add_argument("--source", required=True, help="Path to sources.csv (id,0)")
    ap.add_argument("--iterations", required=True, type=int, help="Numer of iterations to run")
    ap.add_argument("--app-name", default="RDD-SSSP", help="Spark application name")
    return ap.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName(args.app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR") #reduce SPARK noise
    sc = spark.sparkContext

    # ---- Load edges as RDD[(src, (dst, weight))] ----
    # edges.csv has header: src,dst,weight
    edges_rdd = (
        sc.textFile(args.edges)
        .filter(lambda l: not l.startswith("src"))  # skip header
        .map(lambda l: l.strip().split(","))
        .filter(lambda parts: len(parts) >= 3)      # skip bad lines
        .map(lambda parts: (parts[0], (parts[1], int(parts[2])))) # format as RDD[(src, (dst, weight))]
    ).cache()

    for i in range(1, args.iterations+1):
        input_dir = f"{args.source}-iter{i-1:03}"
        output_dir = f"{args.source}-iter{i:03}"
        print(f"Iteration {i}: reading from {input_dir}, writing to {output_dir}")
        expand_frontier(sc, input_dir, output_dir, edges_rdd)

    spark.stop()


if __name__ == "__main__":
    main()