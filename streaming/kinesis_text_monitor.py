import boto3
import time
from datetime import datetime, timedelta
from collections import deque
import os
import sys

class KinesisTextMonitor:
    def __init__(self, stream_name, region='us-east-1'):
        self.kinesis = boto3.client('kinesis', region_name=region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
        self.stream_name = stream_name
        
        # Performance tracking
        self.records_processed = deque(maxlen=60)
        self.bytes_processed = deque(maxlen=60)
        self.latencies = deque(maxlen=60)
        
        # Timing
        self.start_time = time.time()
        self.last_record_count = 0
        self.last_byte_count = 0
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('clear' if os.name == 'posix' else 'cls')
        
    def get_metrics(self):
        """Get current metrics from CloudWatch"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=1)
        
        metrics = {}
        
        # Get metrics
        metric_names = ['IncomingRecords', 'IncomingBytes', 'GetRecords.Latency']
        
        for metric_name in metric_names:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/Kinesis',
                    MetricName=metric_name,
                    Dimensions=[{'Name': 'StreamName', 'Value': self.stream_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=60,
                    Statistics=['Sum'] if 'Records' in metric_name or 'Bytes' in metric_name else ['Average']
                )
                
                if response['Datapoints']:
                    latest = max(response['Datapoints'], key=lambda x: x['Timestamp'])
                    value = latest['Sum'] if 'Records' in metric_name or 'Bytes' in metric_name else latest['Average']
                    metrics[metric_name] = value
                else:
                    metrics[metric_name] = 0
                    
            except Exception as e:
                metrics[metric_name] = 0
                
        return metrics
    
    def get_shard_info(self):
        """Get shard information"""
        try:
            response = self.kinesis.describe_stream(StreamName=self.stream_name)
            return {
                'status': response['StreamDescription']['StreamStatus'],
                'shard_count': len(response['StreamDescription']['Shards']),
                'retention': response['StreamDescription']['RetentionPeriodHours']
            }
        except:
            return {'status': 'Unknown', 'shard_count': 0, 'retention': 0}
    
    def calculate_rates(self, current_records, current_bytes):
        """Calculate throughput rates"""
        current_time = time.time()
        time_diff = current_time - self.start_time
        
        if time_diff > 0:
            # Overall rates
            overall_record_rate = current_records / time_diff
            overall_byte_rate = current_bytes / time_diff / 1024  # KB/s
            
            # Instantaneous rates (last measurement)
            instant_record_rate = (current_records - self.last_record_count) / 60  # per second
            instant_byte_rate = (current_bytes - self.last_byte_count) / 60 / 1024  # KB/s
            
            self.last_record_count = current_records
            self.last_byte_count = current_bytes
            
            return {
                'overall_records': overall_record_rate,
                'overall_bytes': overall_byte_rate,
                'instant_records': instant_record_rate,
                'instant_bytes': instant_byte_rate
            }
        
        return {'overall_records': 0, 'overall_bytes': 0, 'instant_records': 0, 'instant_bytes': 0}
    
    def draw_bar(self, value, max_value, width=30):
        """Draw a text-based bar chart"""
        if max_value == 0:
            return '│' + ' ' * width + '│'
        
        filled = int((value / max_value) * width)
        bar = '│' + '█' * filled + '░' * (width - filled) + '│'
        return bar
    
    def monitor(self):
        """Main monitoring loop"""
        total_records = 0
        total_bytes = 0
        max_rate = 1
        
        print("Starting Kinesis Stream Monitor...")
        print("Press Ctrl+C to stop")
        time.sleep(2)
        
        try:
            while True:
                # Clear screen
                self.clear_screen()
                
                # Get current metrics
                metrics = self.get_metrics()
                shard_info = self.get_shard_info()
                
                # Update totals
                total_records += metrics.get('IncomingRecords', 0)
                total_bytes += metrics.get('IncomingBytes', 0)
                
                # Calculate rates
                rates = self.calculate_rates(total_records, total_bytes)
                
                # Update max rate for bar chart scaling
                if rates['instant_records'] > max_rate:
                    max_rate = rates['instant_records']
                
                # Store historical data
                self.records_processed.append(rates['instant_records'])
                self.bytes_processed.append(rates['instant_bytes'])
                self.latencies.append(metrics.get('GetRecords.Latency', 0))
                
                # Display header
                print("╔" + "═" * 78 + "╗")
                print(f"║{'KINESIS STREAM PERFORMANCE MONITOR':^78}║")
                print(f"║{self.stream_name:^78}║")
                print("╚" + "═" * 78 + "╝")
                
                # Stream info
                print(f"\n📊 STREAM STATUS")
                print(f"├─ Status: {shard_info['status']}")
                print(f"├─ Shards: {shard_info['shard_count']}")
                print(f"└─ Retention: {shard_info['retention']} hours")
                
                # Current performance
                print(f"\n⚡ CURRENT PERFORMANCE (last minute)")
                print(f"├─ Records: {metrics.get('IncomingRecords', 0):,.0f}")
                print(f"├─ Data: {metrics.get('IncomingBytes', 0)/1024/1024:.2f} MB")
                print(f"└─ Latency: {metrics.get('GetRecords.Latency', 0):.1f} ms")
                
                # Throughput
                print(f"\n📈 THROUGHPUT")
                print(f"├─ Current: {rates['instant_records']:,.1f} records/sec")
                print(f"│  {self.draw_bar(rates['instant_records'], max_rate, 40)}")
                print(f"├─ Average: {rates['overall_records']:,.1f} records/sec")
                print(f"├─ Data Rate: {rates['instant_bytes']:.1f} KB/sec")
                print(f"└─ Total Processed: {total_records:,.0f} records")
                
                # Mini chart (last 10 measurements)
                if len(self.records_processed) > 1:
                    print(f"\n📉 THROUGHPUT TREND (records/sec)")
                    recent = list(self.records_processed)[-10:]
                    max_recent = max(recent) if recent else 1
                    
                    # Scale to 5 rows
                    for row in range(4, -1, -1):
                        threshold = (row / 4) * max_recent
                        line = "   "
                        for val in recent:
                            if val >= threshold:
                                line += "█ "
                            else:
                                line += "  "
                        print(line)
                    print("   " + "──" * len(recent))
                    print("   " + "".join(f"{i:<2}" for i in range(len(recent))))
                
                # Statistics
                if self.records_processed:
                    avg_throughput = sum(self.records_processed) / len(self.records_processed)
                    max_throughput = max(self.records_processed)
                    avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
                    
                    print(f"\n📊 STATISTICS")
                    print(f"├─ Avg Throughput: {avg_throughput:,.1f} rec/sec")
                    print(f"├─ Peak Throughput: {max_throughput:,.1f} rec/sec")
                    print(f"├─ Avg Latency: {avg_latency:.1f} ms")
                    print(f"└─ Runtime: {time.time() - self.start_time:.0f} seconds")
                
                # Footer
                print("\n" + "─" * 80)
                print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Wait before next update
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
            
            # Save final statistics
            self.save_report()
    
    def save_report(self):
        """Save performance report to file"""
        filename = f"kinesis_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(filename, 'w') as f:
            f.write("KINESIS STREAM PERFORMANCE REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Stream: {self.stream_name}\n")
            f.write(f"Duration: {time.time() - self.start_time:.0f} seconds\n")
            f.write(f"Total Records: {self.last_record_count:,}\n")
            f.write(f"Total Data: {self.last_byte_count/1024/1024:.2f} MB\n")
            
            if self.records_processed:
                f.write(f"\nThroughput Statistics:\n")
                f.write(f"- Average: {sum(self.records_processed)/len(self.records_processed):,.1f} rec/sec\n")
                f.write(f"- Maximum: {max(self.records_processed):,.1f} rec/sec\n")
                f.write(f"- Minimum: {min(self.records_processed):,.1f} rec/sec\n")
            
            if self.latencies:
                f.write(f"\nLatency Statistics:\n")
                f.write(f"- Average: {sum(self.latencies)/len(self.latencies):.1f} ms\n")
                f.write(f"- Maximum: {max(self.latencies):.1f} ms\n")
                f.write(f"- Minimum: {min(self.latencies):.1f} ms\n")
        
        print(f"\nPerformance report saved to: {filename}")

def main():
    # Check if stream name provided as argument
    stream_name = sys.argv[1] if len(sys.argv) > 1 else 'yelp-review-stream'
    
    monitor = KinesisTextMonitor(stream_name)
    monitor.monitor()

if __name__ == "__main__":
    main()
