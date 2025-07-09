import boto3
import json
import time
import random
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YelpStreamProducer:
    def __init__(self, bucket_name, file_key, stream_name, region='us-east-1'):
        self.s3_client = boto3.client('s3', region_name=region)
        self.kinesis_client = boto3.client('kinesis', region_name=region)
        self.bucket_name = bucket_name
        self.file_key = file_key
        self.stream_name = stream_name
        self.batch_size = 25  # Kinesis allows max 25 records per batch
        self.record_batch = []
        self.total_sent = 0
        
    def process_yelp_reviews(self, streaming_mode='continuous', max_records=None):
        """
        Stream Yelp reviews from S3 to Kinesis
        The file is in NDJSON format (newline-delimited JSON)
        """
        logger.info(f"Starting to stream from s3://{self.bucket_name}/{self.file_key}")
        
        # Stream the file line by line instead of loading it all
        try:
            # For large files, we'll read in chunks
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)
            
            records_processed = 0
            buffer = ""
            
            # Read the file in chunks
            for chunk in s3_object['Body'].iter_chunks(chunk_size=1024*1024):  # 1MB chunks
                # Decode chunk and add to buffer
                buffer += chunk.decode('utf-8', errors='ignore')
                
                # Process complete lines
                lines = buffer.split('\n')
                
                # Keep the last incomplete line in buffer
                buffer = lines[-1]
                
                # Process all complete lines
                for line in lines[:-1]:
                    if not line.strip():
                        continue
                        
                    if max_records and records_processed >= max_records:
                        break
                    
                    try:
                        # Parse the JSON line
                        review = json.loads(line)
                        
                        # Extract relevant fields from Yelp review
                        processed_review = {
                            'review_id': review.get('review_id', ''),
                            'text': review.get('text', ''),
                            'stars': review.get('stars', 0),
                            'date': review.get('date', ''),
                            'useful': review.get('useful', 0),
                            'funny': review.get('funny', 0),
                            'cool': review.get('cool', 0),
                            'business_id': review.get('business_id', ''),
                            'user_id': review.get('user_id', ''),
                            'timestamp': datetime.now().isoformat()
                        }
                        
                        # Add to batch
                        self.add_to_batch(processed_review)
                        records_processed += 1
                        
                        # Log progress
                        if records_processed % 100 == 0:
                            logger.info(f"Processed {records_processed} reviews, sent {self.total_sent} to Kinesis")
                        
                        # Show sample of first few reviews
                        if records_processed <= 3:
                            logger.info(f"Sample review: {processed_review['text'][:100]}... (Rating: {processed_review['stars']}★)")
                        
                        # Simulate real-time streaming if in continuous mode
                        if streaming_mode == 'continuous' and records_processed % 10 == 0:
                            time.sleep(random.uniform(0.1, 0.3))
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing review: {e}")
                        continue
                
                # Check if we've reached max_records
                if max_records and records_processed >= max_records:
                    break
                    
        except Exception as e:
            logger.error(f"Error reading from S3: {e}")
        finally:
            # Send any remaining records
            if self.record_batch:
                self.send_batch()
                
        logger.info(f"Completed! Total reviews streamed: {self.total_sent}")
        
    def add_to_batch(self, review):
        """Add review to batch and send when batch is full"""
        record = {
            'Data': json.dumps(review),
            'PartitionKey': review.get('user_id', 'default')[:10]  # Use part of user_id as partition key
        }
        self.record_batch.append(record)
        
        if len(self.record_batch) >= self.batch_size:
            self.send_batch()
    
    def send_batch(self):
        """Send batch of records to Kinesis"""
        if not self.record_batch:
            return
            
        try:
            response = self.kinesis_client.put_records(
                Records=self.record_batch,
                StreamName=self.stream_name
            )
            
            # Check for failed records
            if response['FailedRecordCount'] > 0:
                logger.warning(f"Failed to send {response['FailedRecordCount']} records")
                # Retry failed records
                failed_records = []
                for i, record_response in enumerate(response['Records']):
                    if 'ErrorCode' in record_response:
                        failed_records.append(self.record_batch[i])
                
                if failed_records:
                    time.sleep(1)  # Wait before retry
                    self.record_batch = failed_records
                    self.send_batch()  # Recursive retry
                    return
            
            self.total_sent += len(self.record_batch)
            self.record_batch = []
            
        except Exception as e:
            logger.error(f"Error sending batch to Kinesis: {e}")
            time.sleep(5)  # Wait before retry

    def test_with_sample_data(self):
        """Send sample data for testing without S3"""
        logger.info("Sending sample test data to Kinesis...")
        
        sample_reviews = [
            {
                'review_id': f'test_{i}',
                'text': f'This is test review {i}. ' + random.choice([
                    'Great food and excellent service! Would definitely come back.',
                    'Terrible experience. Food was cold and service was slow.',
                    'Average place. Nothing special but not bad either.',
                    'Amazing atmosphere and delicious meals. Highly recommend!',
                    'Disappointed with the quality. Used to be much better.',
                ]),
                'stars': random.randint(1, 5),
                'date': '2025-07-06',
                'business_id': f'biz_{random.randint(1, 10)}',
                'user_id': f'user_{random.randint(1, 100)}',
                'timestamp': datetime.now().isoformat()
            }
            for i in range(50)
        ]
        
        for review in sample_reviews:
            self.add_to_batch(review)
            
        # Send any remaining
        if self.record_batch:
            self.send_batch()
            
        logger.info(f"Sent {len(sample_reviews)} test reviews")

def main():
    # Configuration
    BUCKET_NAME = 'scalable-x24203203'
    FILE_KEY = 'yelp_academic_dataset_review.json'
    STREAM_NAME = 'yelp-review-stream'
    
    # Create producer
    producer = YelpStreamProducer(BUCKET_NAME, FILE_KEY, STREAM_NAME)
    
    # Choose mode
    print("\nYelp Review Streaming Options:")
    print("1. Stream first 1000 reviews (continuous/real-time simulation)")
    print("2. Batch load first 10000 reviews (fast)")
    print("3. Stream full dataset (batch mode)")
    print("4. Send test data (no S3 needed)")
    
    choice = input("\nEnter choice (1-4): ")
    
    if choice == '1':
        producer.process_yelp_reviews(streaming_mode='continuous', max_records=1000)
    elif choice == '2':
        producer.process_yelp_reviews(streaming_mode='batch', max_records=10000)
    elif choice == '3':
        producer.process_yelp_reviews(streaming_mode='batch')
    elif choice == '4':
        producer.test_with_sample_data()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()
