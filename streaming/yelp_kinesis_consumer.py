import boto3
import json
import time
from datetime import datetime, timedelta
from collections import Counter, deque, defaultdict
from textblob import TextBlob
import threading
import re

class YelpReviewStreamProcessor:
    def __init__(self, stream_name, region='us-east-1'):
        self.kinesis_client = boto3.client('kinesis', region_name=region)
        self.stream_name = stream_name
        self.window_size = 300  # 5 minutes
        
        # Sliding windows
        self.review_window = deque()
        self.word_window = deque()
        self.rating_window = deque()
        
        # Statistics
        self.stats = {
            'total_reviews': 0,
            'total_words': 0,
            'sentiment_distribution': defaultdict(int),
            'rating_distribution': defaultdict(int),
            'avg_review_length': 0
        }
        
        # Business trending
        self.business_mentions = defaultdict(int)
        
        # Thread safety for multi-shard processing
        self.lock = threading.Lock()
        self.analytics_started = False
        
    def preprocess_text(self, text):
        """Clean and preprocess review text"""
        # Convert to lowercase and remove special characters
        text = re.sub(r'[^a-zA-Z\s]', '', text.lower())
        
        # Common words to filter out
        stop_words = {'the', 'and', 'was', 'for', 'with', 'but', 'this', 'that', 
                     'have', 'had', 'were', 'been', 'their', 'would', 'there',
                     'could', 'when', 'where', 'what', 'from', 'they', 'will',
                     'just', 'into', 'your', 'more', 'very', 'than', 'then',
                     'them', 'some', 'only', 'also', 'which', 'about', 'after'}
        
        words = [word for word in text.split() 
                if len(word) > 3 and word not in stop_words]
        
        return words
    
    def analyze_review_sentiment(self, text, stars):
        """Enhanced sentiment analysis combining text and star rating"""
        try:
            blob = TextBlob(text)
            text_sentiment = blob.sentiment.polarity
        except:
            text_sentiment = 0
        
        # Combine with star rating for better accuracy
        # 1-2 stars = negative, 3 = neutral, 4-5 = positive
        if stars <= 2:
            expected_sentiment = 'negative'
        elif stars == 3:
            expected_sentiment = 'neutral'
        else:
            expected_sentiment = 'positive'
        
        # Text-based sentiment
        if text_sentiment > 0.1:
            text_based = 'positive'
        elif text_sentiment < -0.1:
            text_based = 'negative'
        else:
            text_based = 'neutral'
        
        # Weighted combination (60% stars, 40% text analysis)
        if expected_sentiment == text_based:
            final_sentiment = expected_sentiment
        else:
            # In case of conflict, weight star rating higher
            final_sentiment = expected_sentiment
        
        return {
            'final': final_sentiment,
            'text_score': text_sentiment,
            'star_based': expected_sentiment,
            'confidence': abs(text_sentiment)
        }
    
    def process_record(self, record):
        """Process individual Yelp review record"""
        try:
            # Decode record
            data = json.loads(record['Data'])
            
            # Extract fields
            review_text = data.get('text', '')
            stars = float(data.get('stars', 0))
            business_id = data.get('business_id', '')
            timestamp = datetime.fromisoformat(data['timestamp'])
            
            # Preprocess text
            words = self.preprocess_text(review_text)
            
            # Sentiment analysis
            sentiment_result = self.analyze_review_sentiment(review_text, stars)
            
            # Thread-safe statistics update
            with self.lock:
                # Update statistics
                self.stats['total_reviews'] += 1
                self.stats['total_words'] += len(words)
                self.stats['sentiment_distribution'][sentiment_result['final']] += 1
                self.stats['rating_distribution'][int(stars)] += 1
                if self.stats['total_reviews'] > 0:
                    self.stats['avg_review_length'] = self.stats['total_words'] / self.stats['total_reviews']
                
                # Track business mentions
                if business_id:
                    self.business_mentions[business_id] += 1
                
                # Add to sliding windows
                current_time = datetime.now()
                
                self.review_window.append({
                    'timestamp': timestamp,
                    'added_at': current_time,
                    'sentiment': sentiment_result,
                    'stars': stars,
                    'business_id': business_id
                })
                
                self.word_window.append({
                    'words': words,
                    'timestamp': timestamp,
                    'added_at': current_time
                })
                
                self.rating_window.append({
                    'stars': stars,
                    'timestamp': timestamp,
                    'added_at': current_time
                })
                
                # Clean old entries
                self.clean_windows(current_time)
                
                # Print processing result
                print(f"\n[Review #{self.stats['total_reviews']}] {timestamp}")
                print(f"Rating: {'⭐' * int(stars)}")
                print(f"Sentiment: {sentiment_result['final']} (confidence: {sentiment_result['confidence']:.3f})")
                print(f"Preview: {review_text[:100]}...")
                
        except Exception as e:
            print(f"Error processing record: {e}")
    
    def clean_windows(self, current_time):
        """Remove entries older than window_size"""
        cutoff_time = current_time - timedelta(seconds=self.window_size)
        
        # Clean all windows
        for window in [self.review_window, self.word_window, self.rating_window]:
            while window and window[0]['added_at'] < cutoff_time:
                window.popleft()
    
    def get_trending_words(self, top_n=10):
        """Get top N trending words in current window"""
        all_words = []
        for entry in self.word_window:
            all_words.extend(entry['words'])
        
        return Counter(all_words).most_common(top_n)
    
    def get_trending_businesses(self, top_n=5):
        """Get top N most reviewed businesses"""
        recent_businesses = [r['business_id'] for r in self.review_window if r['business_id']]
        return Counter(recent_businesses).most_common(top_n)
    
    def get_sentiment_trends(self):
        """Analyze sentiment trends in window"""
        if not self.review_window:
            return None
            
        sentiments = [r['sentiment']['final'] for r in self.review_window]
        sentiment_counts = Counter(sentiments)
        
        # Calculate average star rating in window
        avg_stars = sum(r['stars'] for r in self.review_window) / len(self.review_window)
        
        return {
            'distribution': dict(sentiment_counts),
            'window_size': len(self.review_window),
            'average_stars': avg_stars,
            'positive_ratio': sentiment_counts['positive'] / len(self.review_window) if self.review_window else 0
        }
    
    def print_analytics(self):
        """Print comprehensive analytics"""
        with self.lock:
            print("\n" + "="*80)
            print("YELP REVIEW STREAM ANALYTICS (5-minute window)")
            print("="*80)
            
            # Overall statistics
            print(f"\nOVERALL STATISTICS:")
            print(f"  Total Reviews Processed: {self.stats['total_reviews']:,}")
            if self.stats['avg_review_length'] > 0:
                print(f"  Average Review Length: {self.stats['avg_review_length']:.0f} words")
            
            # Rating distribution
            if self.stats['rating_distribution']:
                print(f"\n  Rating Distribution:")
                for stars in sorted(self.stats['rating_distribution'].keys()):
                    count = self.stats['rating_distribution'][stars]
                    percentage = (count / self.stats['total_reviews'] * 100) if self.stats['total_reviews'] > 0 else 0
                    print(f"    {stars}⭐: {count:,} ({percentage:.1f}%)")
            
            # Sentiment distribution
            if self.stats['sentiment_distribution']:
                print(f"\n  Sentiment Distribution:")
                for sentiment, count in self.stats['sentiment_distribution'].items():
                    percentage = (count / self.stats['total_reviews'] * 100) if self.stats['total_reviews'] > 0 else 0
                    print(f"    {sentiment.capitalize()}: {count:,} ({percentage:.1f}%)")
            
            # Window analytics
            print(f"\nREAL-TIME WINDOW ANALYTICS:")
            
            # Trending words
            trending_words = self.get_trending_words()
            if trending_words:
                print(f"\n  Top 10 Trending Words:")
                for i, (word, count) in enumerate(trending_words, 1):
                    print(f"    {i}. {word}: {count} occurrences")
            
            # Sentiment trends
            trends = self.get_sentiment_trends()
            if trends:
                print(f"\n  Window Sentiment Analysis:")
                print(f"    Reviews in window: {trends['window_size']}")
                print(f"    Average rating: {trends['average_stars']:.2f}⭐")
                print(f"    Positive ratio: {trends['positive_ratio']:.1%}")
                print(f"    Distribution: {trends['distribution']}")
            
            # Trending businesses
            trending_biz = self.get_trending_businesses()
            if trending_biz:
                print(f"\n  Top 5 Most Reviewed Businesses:")
                for i, (biz_id, count) in enumerate(trending_biz, 1):
                    print(f"    {i}. {biz_id[:20]}...: {count} reviews")
            
            print("="*80)
    
    def start_analytics_thread(self):
        """Start background thread for periodic analytics"""
        if self.analytics_started:
            return
            
        def analytics_loop():
            while True:
                time.sleep(30)  # Print every 30 seconds
                self.print_analytics()
        
        thread = threading.Thread(target=analytics_loop, daemon=True)
        thread.start()
        self.analytics_started = True
    
    def process_shard(self, shard_id):
        """Process a single shard"""
        print(f"Starting to process shard: {shard_id}")
        
        try:
            # Get shard iterator from beginning
            shard_iterator_response = self.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'  # Start from beginning
            )
            
            shard_iterator = shard_iterator_response['ShardIterator']
            empty_record_count = 0
            records_found = False
            
            while True:
                try:
                    # Get records
                    response = self.kinesis_client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=100
                    )
                    
                    records = response['Records']
                    
                    if records:
                        if not records_found:
                            print(f"Found records in shard {shard_id}!")
                            records_found = True
                            
                        empty_record_count = 0
                        for record in records:
                            self.process_record(record)
                    else:
                        empty_record_count += 1
                        if empty_record_count % 30 == 0 and not records_found:
                            print(f"Still no records in shard {shard_id}...")
                        time.sleep(0.5)
                    
                    # Update iterator
                    shard_iterator = response.get('NextShardIterator')
                    if not shard_iterator:
                        print(f"Shard {shard_id} has been closed")
                        break
                        
                except Exception as e:
                    print(f"Error in shard {shard_id}: {e}")
                    time.sleep(5)
                    # Try to get new iterator
                    try:
                        shard_iterator_response = self.kinesis_client.get_shard_iterator(
                            StreamName=self.stream_name,
                            ShardId=shard_id,
                            ShardIteratorType='LATEST'
                        )
                        shard_iterator = shard_iterator_response['ShardIterator']
                    except:
                        break
                        
        except Exception as e:
            print(f"Failed to process shard {shard_id}: {e}")
    
    def consume_stream(self):
        """Main consumer loop - processes all shards in parallel"""
        # Get all shards
        response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
        shards = response['StreamDescription']['Shards']
        
        print(f"Starting Yelp review stream consumer for: {self.stream_name}")
        print(f"Found {len(shards)} shards: {[s['ShardId'] for s in shards]}")
        print("Processing all shards in parallel...")
        
        # Start analytics printer
        self.start_analytics_thread()
        
        # Create threads for each shard
        threads = []
        for shard in shards:
            thread = threading.Thread(
                target=self.process_shard,
                args=(shard['ShardId'],),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print("\nShutting down consumer...")

if __name__ == "__main__":
    processor = YelpReviewStreamProcessor('yelp-review-stream')
    processor.consume_stream()
