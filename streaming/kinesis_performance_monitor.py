import boto3
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime, timedelta
import numpy as np
from collections import deque
import time
import json
import threading

class KinesisPerformanceMonitor:
    def __init__(self, stream_name, region='us-east-1'):
        self.kinesis = boto3.client('kinesis', region_name=region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
        self.stream_name = stream_name
        
        # Data storage for graphs
        self.time_points = deque(maxlen=60)  # Last 60 data points
        self.incoming_records = deque(maxlen=60)
        self.incoming_bytes = deque(maxlen=60)
        self.get_records_latency = deque(maxlen=60)
        self.put_records_latency = deque(maxlen=60)
        self.throughput = deque(maxlen=60)
        
        # For calculating real throughput
        self.last_record_count = 0
        self.last_check_time = time.time()
        
        # Setup plot
        self.fig, self.axes = plt.subplots(2, 2, figsize=(15, 10))
        self.fig.suptitle(f'Kinesis Stream Performance Monitor: {stream_name}', fontsize=16)
        
    def get_stream_metrics(self):
        """Fetch metrics from CloudWatch"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=5)
        
        metrics = {}
        
        # Define metrics to fetch
        metric_queries = [
            ('IncomingRecords', 'Sum'),
            ('IncomingBytes', 'Sum'),
            ('GetRecords.Latency', 'Average'),
            ('PutRecords.Latency', 'Average'),
            ('GetRecords.Success', 'Sum')
        ]
        
        for metric_name, stat in metric_queries:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/Kinesis',
                    MetricName=metric_name,
                    Dimensions=[
                        {
                            'Name': 'StreamName',
                            'Value': self.stream_name
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=60,
                    Statistics=[stat]
                )
                
                if response['Datapoints']:
                    # Get the most recent data point
                    latest = max(response['Datapoints'], key=lambda x: x['Timestamp'])
                    metrics[metric_name] = latest[stat]
                else:
                    metrics[metric_name] = 0
                    
            except Exception as e:
                print(f"Error fetching {metric_name}: {e}")
                metrics[metric_name] = 0
                
        return metrics
    
    def get_shard_metrics(self):
        """Get real-time metrics directly from shards"""
        try:
            # Describe stream to get shard count
            response = self.kinesis.describe_stream(StreamName=self.stream_name)
            shard_count = len(response['StreamDescription']['Shards'])
            
            # Get stream summary for more metrics
            summary = self.kinesis.describe_stream_summary(StreamName=self.stream_name)
            
            return {
                'shard_count': shard_count,
                'open_shard_count': summary['StreamDescriptionSummary']['OpenShardCount'],
                'consumer_count': summary['StreamDescriptionSummary'].get('ConsumerCount', 0)
            }
        except Exception as e:
            print(f"Error getting shard metrics: {e}")
            return {'shard_count': 0, 'open_shard_count': 0, 'consumer_count': 0}
    
    def calculate_real_throughput(self, current_records):
        """Calculate actual throughput based on record count changes"""
        current_time = time.time()
        time_diff = current_time - self.last_check_time
        
        if time_diff > 0:
            record_diff = current_records - self.last_record_count
            throughput = record_diff / time_diff
            
            self.last_record_count = current_records
            self.last_check_time = current_time
            
            return throughput
        return 0
    
    def update_data(self):
        """Update data for graphs"""
        # Get CloudWatch metrics
        metrics = self.get_stream_metrics()
        shard_info = self.get_shard_metrics()
        
        # Update time
        current_time = datetime.now()
        self.time_points.append(current_time)
        
        # Update metrics
        self.incoming_records.append(metrics.get('IncomingRecords', 0))
        self.incoming_bytes.append(metrics.get('IncomingBytes', 0) / 1024)  # Convert to KB
        self.get_records_latency.append(metrics.get('GetRecords.Latency', 0))
        self.put_records_latency.append(metrics.get('PutRecords.Latency', 0))
        
        # Calculate throughput
        total_records = sum(self.incoming_records)
        current_throughput = self.calculate_real_throughput(total_records)
        self.throughput.append(current_throughput)
        
        return shard_info
    
    def animate(self, frame):
        """Animation function for real-time updates"""
        # Update data
        shard_info = self.update_data()
        
        # Clear all axes
        for ax in self.axes.flat:
            ax.clear()
        
        # Convert time points to seconds ago for x-axis
        if self.time_points:
            current_time = self.time_points[-1]
            x_values = [(current_time - t).total_seconds() for t in self.time_points]
            x_values = [-x for x in x_values]  # Make recent times positive
            
            # Plot 1: Records/Throughput
            ax1 = self.axes[0, 0]
            if self.incoming_records:
                ax1.plot(x_values, list(self.incoming_records), 'b-', label='Records/min', linewidth=2)
                ax1.fill_between(x_values, list(self.incoming_records), alpha=0.3)
            if self.throughput:
                ax1_twin = ax1.twinx()
                ax1_twin.plot(x_values, list(self.throughput), 'r-', label='Throughput/sec', linewidth=2)
                ax1_twin.set_ylabel('Records/sec', color='r')
                ax1_twin.tick_params(axis='y', labelcolor='r')
            ax1.set_title('Incoming Records & Throughput')
            ax1.set_xlabel('Seconds Ago')
            ax1.set_ylabel('Records/minute', color='b')
            ax1.tick_params(axis='y', labelcolor='b')
            ax1.grid(True, alpha=0.3)
            ax1.legend(loc='upper left')
            
            # Plot 2: Data Volume
            ax2 = self.axes[0, 1]
            if self.incoming_bytes:
                ax2.plot(x_values, list(self.incoming_bytes), 'g-', linewidth=2)
                ax2.fill_between(x_values, list(self.incoming_bytes), alpha=0.3, color='g')
            ax2.set_title('Incoming Data Volume')
            ax2.set_xlabel('Seconds Ago')
            ax2.set_ylabel('KB/minute')
            ax2.grid(True, alpha=0.3)
            
            # Add current value text
            if self.incoming_bytes:
                current_kb = self.incoming_bytes[-1]
                ax2.text(0.95, 0.95, f'Current: {current_kb:.1f} KB/min', 
                        transform=ax2.transAxes, ha='right', va='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            # Plot 3: Latency Comparison
            ax3 = self.axes[1, 0]
            if self.get_records_latency:
                ax3.plot(x_values, list(self.get_records_latency), 'r-', label='GetRecords', linewidth=2)
            if self.put_records_latency:
                ax3.plot(x_values, list(self.put_records_latency), 'b-', label='PutRecords', linewidth=2)
            ax3.set_title('Operation Latency')
            ax3.set_xlabel('Seconds Ago')
            ax3.set_ylabel('Latency (ms)')
            ax3.legend()
            ax3.grid(True, alpha=0.3)
            
            # Plot 4: Stream Status
            ax4 = self.axes[1, 1]
            ax4.axis('off')
            
            # Create status text
            status_text = f"""Stream Status
            
Shards: {shard_info['shard_count']}
Open Shards: {shard_info['open_shard_count']}
Active Consumers: {shard_info['consumer_count']}

Current Metrics:
Records/min: {self.incoming_records[-1] if self.incoming_records else 0:.0f}
Throughput: {self.throughput[-1] if self.throughput else 0:.1f} rec/sec
Data Rate: {self.incoming_bytes[-1] if self.incoming_bytes else 0:.1f} KB/min

Avg Latency:
GetRecords: {np.mean(list(self.get_records_latency)) if self.get_records_latency else 0:.1f} ms
PutRecords: {np.mean(list(self.put_records_latency)) if self.put_records_latency else 0:.1f} ms
"""
            
            ax4.text(0.1, 0.9, status_text, transform=ax4.transAxes,
                    fontsize=12, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
        
        plt.tight_layout()
        
    def start_monitoring(self):
        """Start the real-time monitoring"""
        print(f"Starting real-time monitoring for stream: {self.stream_name}")
        print("Close the window to stop monitoring...")
        
        # Create animation
        ani = animation.FuncAnimation(self.fig, self.animate, interval=2000)  # Update every 2 seconds
        
        plt.show()

class PerformanceBenchmark:
    """Run performance benchmarks and generate report"""
    
    def __init__(self, stream_name, producer_script='yelp_producer_scalable.py'):
        self.stream_name = stream_name
        self.producer_script = producer_script
        self.results = []
        
    def run_benchmark(self, test_configs):
        """Run benchmarks with different configurations"""
        import subprocess
        
        print("Running performance benchmarks...")
        
        for config in test_configs:
            print(f"\nTest: {config['name']}")
            print(f"Records: {config['records']}, Rate: {config['rate']}")
            
            # Start monitoring in background
            monitor = KinesisPerformanceMonitor(self.stream_name)
            
            # Run producer with specific config
            start_time = time.time()
            
            # You would run the producer here
            # subprocess.run(['python3', self.producer_script, ...])
            
            end_time = time.time()
            
            # Collect results
            result = {
                'name': config['name'],
                'records': config['records'],
                'rate': config['rate'],
                'duration': end_time - start_time,
                'throughput': config['records'] / (end_time - start_time)
            }
            
            self.results.append(result)
            print(f"Completed in {result['duration']:.1f}s, Throughput: {result['throughput']:.1f} rec/s")
            
            # Wait between tests
            time.sleep(30)
    
    def generate_report(self):
        """Generate performance report with graphs"""
        if not self.results:
            print("No benchmark results to report")
            return
            
        # Create comparison graphs
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Kinesis Performance Benchmark Results', fontsize=16)
        
        # Extract data
        names = [r['name'] for r in self.results]
        throughputs = [r['throughput'] for r in self.results]
        durations = [r['duration'] for r in self.results]
        
        # Bar chart of throughput
        axes[0, 0].bar(names, throughputs)
        axes[0, 0].set_title('Throughput Comparison')
        axes[0, 0].set_ylabel('Records/second')
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        # Duration comparison
        axes[0, 1].bar(names, durations)
        axes[0, 1].set_title('Processing Duration')
        axes[0, 1].set_ylabel('Seconds')
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig('kinesis_benchmark_results.png')
        print("Benchmark report saved as kinesis_benchmark_results.png")

def main():
    import sys
    
    print("Kinesis Performance Monitor")
    print("1. Real-time monitoring dashboard")
    print("2. Run performance benchmarks")
    print("3. Generate CloudWatch metrics report")
    
    choice = input("\nSelect option (1-3): ")
    
    if choice == '1':
        # Real-time monitoring
        monitor = KinesisPerformanceMonitor('yelp-review-stream')
        monitor.start_monitoring()
        
    elif choice == '2':
        # Performance benchmarks
        benchmark = PerformanceBenchmark('yelp-review-stream')
        
        # Define test configurations
        test_configs = [
            {'name': 'Low Load', 'records': 1000, 'rate': 10},
            {'name': 'Medium Load', 'records': 10000, 'rate': 100},
            {'name': 'High Load', 'records': 50000, 'rate': 500},
            {'name': 'Max Throughput', 'records': 100000, 'rate': None}
        ]
        
        benchmark.run_benchmark(test_configs)
        benchmark.generate_report()
        
    elif choice == '3':
        # Generate static report
        monitor = KinesisPerformanceMonitor('yelp-review-stream')
        
        # Collect data for 1 minute
        print("Collecting metrics for 60 seconds...")
        data_points = []
        
        for i in range(30):  # 30 iterations, 2 seconds each
            monitor.update_data()
            time.sleep(2)
            print(f"Progress: {i+1}/30", end='\r')
        
        # Generate final plot
        monitor.animate(0)
        plt.savefig('kinesis_metrics_report.png')
        print("\nMetrics report saved as kinesis_metrics_report.png")

if __name__ == "__main__":
    main()
