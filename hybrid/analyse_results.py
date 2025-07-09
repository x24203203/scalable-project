#!/usr/bin/env python3
# analyze_results.py - Analyze benchmark results from completed workers

import boto3
import json
from datetime import datetime
import statistics

BUCKET = 'scalable-x24203203'
s3 = boto3.client('s3')

def download_results():
    """Download all worker results from S3"""
    results = {
        'shard_results': [],
        'worker_summaries': []
    }
    
    # List all result files
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='results/')
    
    for obj in response.get('Contents', []):
        key = obj['Key']
        
        # Skip test results
        if 'test_' in key:
            continue
            
        # Download and parse JSON
        response = s3.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(response['Body'].read())
        
        if '_summary.json' in key:
            results['worker_summaries'].append(data)
        elif '_shard_' in key and '_result.json' in key:
            results['shard_results'].append(data)
    
    return results

def analyze_performance(results):
    """Analyze performance metrics"""
    print("📊 PERFORMANCE ANALYSIS")
    print("=" * 80)
    
    # 1. Overall Statistics
    print("\n1. OVERALL STATISTICS:")
    print("-" * 40)
    
    total_reviews = sum(s['total_reviews_processed'] for s in results['worker_summaries'])
    total_time = max(s['total_processing_time'] for s in results['worker_summaries'])
    
    print(f"Total reviews processed: {total_reviews:,}")
    print(f"Total processing time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    print(f"Overall throughput: {total_reviews/total_time:,.0f} reviews/second")
    
    # 2. Worker Performance
    print("\n2. WORKER PERFORMANCE:")
    print("-" * 40)
    print(f"{'Worker':<10} {'Reviews':<15} {'Time (s)':<12} {'Rate (r/s)':<15} {'Shards'}")
    print("-" * 65)
    
    for summary in sorted(results['worker_summaries'], key=lambda x: x['worker_id']):
        worker_id = summary['worker_id']
        reviews = summary['total_reviews_processed']
        time = summary['total_processing_time']
        rate = summary['average_reviews_per_second']
        shards = len(summary['shards_processed'])
        
        print(f"Worker {worker_id:<3} {reviews:>14,} {time:>11.1f} {rate:>14,.0f} {shards:>6}")
    
    # 3. Shard Analysis
    print("\n3. SHARD PROCESSING DETAILS:")
    print("-" * 40)
    print(f"{'Shard':<8} {'Worker':<8} {'Reviews':<12} {'Time (s)':<12} {'Download':<12} {'Process':<12} {'Rate'}")
    print("-" * 85)
    
    shard_times = []
    download_times = []
    process_times = []
    
    for result in sorted(results['shard_results'], key=lambda x: x['shard_id']):
        shard_id = result['shard_id']
        worker_id = result['worker_id']
        reviews = result['total_reviews']
        total_time = result['processing_time']
        download_time = result.get('download_time', 0)
        compute_time = result.get('compute_time', total_time - download_time)
        rate = result['reviews_per_second']
        
        shard_times.append(total_time)
        if download_time > 0:
            download_times.append(download_time)
            process_times.append(compute_time)
        
        print(f"Shard {shard_id:<2} Worker {worker_id:<1} {reviews:>11,} {total_time:>11.1f} {download_time:>11.1f} {compute_time:>11.1f} {rate:>8,.0f}")
    
    # 4. Performance Statistics
    print("\n4. PERFORMANCE STATISTICS:")
    print("-" * 40)
    
    print(f"Average shard processing time: {statistics.mean(shard_times):.1f}s")
    print(f"Min/Max shard time: {min(shard_times):.1f}s / {max(shard_times):.1f}s")
    
    if download_times:
        print(f"Average download time: {statistics.mean(download_times):.1f}s")
        print(f"Average compute time: {statistics.mean(process_times):.1f}s")
    
    # 5. Parallel Efficiency
    print("\n5. PARALLEL EFFICIENCY:")
    print("-" * 40)
    
    # Sequential baseline (sum of all shard times)
    sequential_time = sum(shard_times)
    parallel_time = total_time
    speedup = sequential_time / parallel_time
    efficiency = speedup / 3  # 3 workers
    
    print(f"Sequential time (if processed one by one): {sequential_time:.1f}s ({sequential_time/60:.1f} min)")
    print(f"Parallel time (actual): {parallel_time:.1f}s ({parallel_time/60:.1f} min)")
    print(f"Speedup: {speedup:.2f}x")
    print(f"Parallel efficiency: {efficiency:.1%}")
    
    # 6. Data Processing Summary
    print("\n6. DATA PROCESSING SUMMARY:")
    print("-" * 40)
    
    total_unique_words = sum(r.get('unique_words', 0) for r in results['shard_results'])
    avg_unique_per_shard = total_unique_words / len(results['shard_results'])
    
    print(f"Total unique words found: {total_unique_words:,}")
    print(f"Average unique words per shard: {avg_unique_per_shard:,.0f}")
    
    # 7. Top Words Across All Shards
    print("\n7. TOP 10 WORDS ACROSS ALL SHARDS:")
    print("-" * 40)
    
    # Aggregate word counts from all shards
    global_word_counts = {}
    for result in results['shard_results']:
        for word, count in result.get('top_100_words', {}).items():
            global_word_counts[word] = global_word_counts.get(word, 0) + count
    
    top_words = sorted(global_word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    for word, count in top_words:
        print(f"{word:15} {count:>15,}")

def create_benchmark_report(results):
    """Create a benchmark report file"""
    report = {
        'timestamp': datetime.now().isoformat(),
        'configuration': {
            'workers': 3,
            'shards': 9,
            'parallelism': 'distributed (3 EC2 instances)'
        },
        'performance': {
            'total_reviews': sum(s['total_reviews_processed'] for s in results['worker_summaries']),
            'total_time_seconds': max(s['total_processing_time'] for s in results['worker_summaries']),
            'overall_throughput': sum(s['total_reviews_processed'] for s in results['worker_summaries']) / max(s['total_processing_time'] for s in results['worker_summaries']),
            'worker_details': results['worker_summaries'],
            'shard_details': results['shard_results']
        }
    }
    
    # Upload to S3
    s3.put_object(
        Bucket=BUCKET,
        Key='results/benchmark_report.json',
        Body=json.dumps(report, indent=2)
    )
    
    print("\n✅ Benchmark report saved to s3://scalable-x24203203/results/benchmark_report.json")

def main():
    print("🔍 ANALYZING WORKER RESULTS FROM S3")
    print("=" * 80)
    
    # Download results
    print("Downloading results from S3...")
    results = download_results()
    
    print(f"Found {len(results['shard_results'])} shard results")
    print(f"Found {len(results['worker_summaries'])} worker summaries")
    
    if len(results['worker_summaries']) < 3:
        print("\n⚠️  Warning: Not all workers have completed yet!")
    
    # Analyze performance
    analyze_performance(results)
    
    # Create report
    create_benchmark_report(results)

if __name__ == "__main__":
    main()
