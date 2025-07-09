#!/usr/bin/env python3
"""
Simplified Yelp Reviews MapReduce - Word Count Implementation
For Phase 2 of Scalable Cloud Programming Project
"""

import sys
import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
import boto3

class YelpWordCountMapReduce:
    def __init__(self):
        """Initialize the MapReduce processor"""
        self.spark = None
        self.start_time = datetime.now()
        
    def create_spark_session(self):
        """Create and configure Spark session for EMR"""
        self.spark = SparkSession.builder \
            .appName("YelpWordCountMapReduce") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Get cluster information
        sc = self.spark.sparkContext
        print(f"=" * 60)
        print(f"Spark Application Started")
        print(f"Application ID: {sc.applicationId}")
        print(f"Default Parallelism: {sc.defaultParallelism}")
        print(f"Number of Executors: {sc._jsc.sc().getExecutorMemoryStatus().size()}")
        print(f"=" * 60)
        
        return self.spark
    
    def load_reviews_data(self, input_path):
        """Load Yelp reviews from S3"""
        print(f"\nLoading data from: {input_path}")
        
        # Read the review JSON file
        df = self.spark.read.json(input_path)
        
        # Cache for better performance
        df.cache()
        
        # Get data statistics
        total_reviews = df.count()
        print(f"Total reviews loaded: {total_reviews:,}")
        
        # Show schema
        print("\nData Schema:")
        df.printSchema()
        
        # Show sample data
        print("\nSample Reviews:")
        df.select("review_id", "stars", "text").show(5, truncate=50)
        
        return df, total_reviews
    
    def word_count_mapreduce(self, df, output_bucket, num_cores):
        """
        Implement Word Count using MapReduce pattern
        Maps review text to words and reduces by counting occurrences
        """
        print("\n" + "=" * 60)
        print("WORD COUNT MAPREDUCE")
        print(f"Running with {num_cores} cores")
        print("=" * 60)
        
        start_time = time.time()
        
        # Convert DataFrame to RDD for classic MapReduce
        reviews_rdd = df.select("text").rdd
        
        # MAP PHASE: Split text into words and create (word, 1) pairs
        print("\nMAP PHASE: Splitting text into words...")
        words_mapped = reviews_rdd.flatMap(lambda row: row.text.lower().split() if row.text else []) \
                                 .map(lambda word: word.strip('.,!?;:"()[]{}')) \
                                 .filter(lambda word: len(word) > 2 and word.isalpha()) \
                                 .map(lambda word: (word, 1))
        
        # REDUCE PHASE: Sum counts for each word
        print("REDUCE PHASE: Aggregating word counts...")
        word_counts = words_mapped.reduceByKey(lambda a, b: a + b)
        
        # Sort by count (descending) and get top 100
        print("SORT PHASE: Getting top 100 words...")
        top_words = word_counts.sortBy(lambda x: x[1], ascending=False)
        top_100_words = top_words.take(100)
        
        # Calculate total unique words
        total_unique_words = word_counts.count()
        
        execution_time = time.time() - start_time
        
        # Convert to DataFrame for saving
        words_df = self.spark.createDataFrame(top_100_words, ["word", "count"])
        
        # Save results to S3
        output_path = f"s3://{output_bucket}/results/word_count/{self.start_time.strftime('%Y%m%d_%H%M%S')}"
        words_df.coalesce(1).write.mode("overwrite").json(output_path)
        
        # Print results
        print(f"\nTop 20 words:")
        print(f"{'Word':<20} {'Count':>10}")
        print("-" * 30)
        for word, count in top_100_words[:20]:
            print(f"{word:<20} {count:>10,}")
        
        print(f"\nTotal unique words: {total_unique_words:,}")
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Results saved to: {output_path}")
        
        # Save benchmark data
        benchmark_data = {
            "timestamp": self.start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "num_cores": num_cores,
            "spark_config": {
                "default_parallelism": self.spark.sparkContext.defaultParallelism,
                "executor_instances": self.spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()
            },
            "word_count_results": {
                "execution_time": execution_time,
                "total_unique_words": total_unique_words,
                "top_10_words": top_100_words[:10]
            }
        }
        
        # Save benchmark summary
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=output_bucket,
            Key=f"benchmarks/cores_{num_cores}_{self.start_time.strftime('%Y%m%d_%H%M%S')}.json",
            Body=json.dumps(benchmark_data, indent=2)
        )
        
        print(f"\nBenchmark data saved to: s3://{output_bucket}/benchmarks/")
        
        return execution_time, total_unique_words
    
    def run_mapreduce(self, input_path, output_bucket, num_cores):
        """Run the MapReduce word count job"""
        print(f"\n{'=' * 60}")
        print(f"STARTING WORD COUNT MAPREDUCE - {num_cores} CORES")
        print(f"{'=' * 60}")
        
        # Create Spark session
        self.create_spark_session()
        
        # Load data
        df, total_reviews = self.load_reviews_data(input_path)
        
        # Run word count MapReduce
        execution_time, unique_words = self.word_count_mapreduce(df, output_bucket, num_cores)
        
        # Print summary
        print(f"\n{'=' * 60}")
        print(f"MAPREDUCE JOB SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total reviews processed: {total_reviews:,}")
        print(f"Number of cores: {num_cores}")
        print(f"Total unique words found: {unique_words:,}")
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Throughput: {total_reviews/execution_time:.0f} reviews/second")
        
        # Stop Spark session
        self.spark.stop()
        
        return execution_time


def main():
    """Main function to run on EMR"""
    parser = argparse.ArgumentParser(description='Run Yelp Word Count MapReduce on EMR')
    parser.add_argument('--input-path', required=True, help='S3 path to Yelp reviews JSON')
    parser.add_argument('--output-bucket', required=True, help='S3 bucket for output (without s3://)')
    parser.add_argument('--num-cores', type=int, default=3, help='Number of cores (for logging)')
    
    args = parser.parse_args()
    
    # Create and run MapReduce processor
    processor = YelpWordCountMapReduce()
    execution_time = processor.run_mapreduce(
        input_path=args.input_path,
        output_bucket=args.output_bucket,
        num_cores=args.num_cores
    )
    
    print("\nMapReduce word count completed successfully!")
    sys.exit(0)  # Explicit successful exit


if __name__ == "__main__":
    main()
