import time
import boto3
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import json

class YelpStreamPerformanceTest:
    def __init__(self, stream_name):
        self.kinesis = boto3.client('kinesis')
        self.cloudwatch = boto3.client('cloudwatch')
        self.stream_name = stream_name
        
    def load_test(self, records_per_second, duration_seconds):
        """Send burst of records to test throughput"""
        print(f"Starting load test: {records_per_second} records/sec for {duration_seconds} seconds")
        
        start_time = time.time()
        records_sent = 0
        
        # Sample Yelp review for testing
        test_review = {
            'review_id': 'test_review',
            'text': 'This is a test review for performance testing. Great food and service!',
            'stars': 5,
            'date': '2025-07-06',
            'business_id': 'test_business',
            'user_id': 'test_user',
            'timestamp': ''
        }
        
        while time.time() - start_time < duration_seconds:
            batch = []
            
            # Create batch of records
            for i in range(min(records_per_second, 500)):  # Max 500 per batch
                test_review['timestamp'] = datetime.now().isoformat()
                test_review['review_id'] = f'test_{records_sent + i}'
                
                batch.append({
                    'Data': json.dumps(test_review),
                    'PartitionKey': str(records_sent + i)
                })
            
            # Send batch
            try:
                response = self.kinesis.put_records(
                    Records=batch,
                    StreamName=self.stream_name
                )
                records_sent += len(batch) - response['FailedRecordCount']
            except Exception as e:
                print(f"Error sending batch: {e}")
            
            # Wait to maintain rate
            time.sleep(1)
        
        print(f"Load test complete. Sent {records_sent} records")
        return records_sent
    
    def measure_latency(self, num_samples=10):
        """Measure end-to-end latency"""
        latencies = []
        
        for i in range(num_samples):
            # Send record with timestamp
            send_time = time.time()
            test_data = {
                'test_id': f'latency_test_{i}',
                'send_timestamp': send_time,
                'data': 'Latency test review'
            }
            
            self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(test_data),
                PartitionKey='latency_test'
            )
            
            # Measure time to appear in stream
            # (In real scenario, consumer would record receive time)
            latencies.append(time.time() - send_time)
            time.sleep(0.1)
        
        avg_latency = sum(latencies) / len(latencies)
        print(f"Average latency: {avg_latency*1000:.2f} ms")
        return latencies

if __name__ == "__main__":
    tester = YelpStreamPerformanceTest('yelp-review-stream')
    
    # Run different load tests
    print("Running performance tests...")
    
    # Test 1: Moderate load
    tester.load_test(records_per_second=10, duration_seconds=30)
    
    # Test 2: High load
    tester.load_test(records_per_second=100, duration_seconds=30)
    
    # Test 3: Measure latency
    tester.measure_latency()
