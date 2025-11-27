import requests
import json
import time
from datetime import datetime, timedelta
import csv
import os
from collections import defaultdict
import logging
from typing import List, Dict, Optional
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bluesky_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BlueskyModerationCollector:
    def __init__(self, handle, password):
        """Initialize the Bluesky API collector with authentication."""
        self.base_url = "https://bsky.social/xrpc"
        self.handle = handle
        self.password = password
        self.session = None
        self.request_count = 0
        self.rate_limit_window = 300  # 5 minutes
        self.max_requests_per_window = 3000  # Conservative estimate
        self.window_start = time.time()
        self.authenticate()
    
    def authenticate(self):
        """Authenticate with Bluesky and get session token."""
        url = f"{self.base_url}/com.atproto.server.createSession"
        data = {
            "identifier": self.handle,
            "password": self.password
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=data, timeout=30)
                response.raise_for_status()
                self.session = response.json()
                logger.info(f"✓ Authenticated as {self.handle}")
                return True
            except requests.exceptions.RequestException as e:
                logger.error(f"Authentication attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error("✗ Authentication failed after all retries")
                    return False
    
    def check_rate_limit(self):
        """Check and enforce rate limiting."""
        current_time = time.time()
        
        # Reset counter if window expired
        if current_time - self.window_start > self.rate_limit_window:
            self.request_count = 0
            self.window_start = current_time
        
        # If approaching limit, wait
        if self.request_count >= self.max_requests_per_window:
            sleep_time = self.rate_limit_window - (current_time - self.window_start)
            if sleep_time > 0:
                logger.warning(f"Rate limit approaching, sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time + 1)
                self.request_count = 0
                self.window_start = time.time()
        
        self.request_count += 1
    
    def get_headers(self):
        """Return authorization headers for API requests."""
        if not self.session:
            raise Exception("Not authenticated")
        return {
            "Authorization": f"Bearer {self.session['accessJwt']}"
        }
    
    def make_request_with_retry(self, method, url, max_retries=3, **kwargs):
        """Make an API request with retry logic and rate limiting."""
        self.check_rate_limit()
        
        for attempt in range(max_retries):
            try:
                if method.lower() == 'get':
                    response = requests.get(url, timeout=30, **kwargs)
                elif method.lower() == 'post':
                    response = requests.post(url, timeout=30, **kwargs)
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Rate limited
                    wait_time = min(60 * (2 ** attempt), 300)  # Max 5 min
                    logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                elif response.status_code == 401:  # Auth expired
                    logger.info("Session expired, re-authenticating...")
                    self.authenticate()
                    kwargs['headers'] = self.get_headers()
                else:
                    logger.error(f"HTTP error {response.status_code}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))
                    else:
                        return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                else:
                    return None
        
        return None
    
    def search_posts(self, query, limit=100, cursor=None):
        """Search for posts containing specific terms."""
        url = f"{self.base_url}/app.bsky.feed.searchPosts"
        params = {
            "q": query,
            "limit": min(limit, 100)  # API max is 100
        }
        
        if cursor:
            params["cursor"] = cursor
        
        return self.make_request_with_retry('get', url, params=params, headers=self.get_headers())
    
    def get_author_feed(self, actor, limit=50, cursor=None):
        """Get posts from a specific user."""
        url = f"{self.base_url}/app.bsky.feed.getAuthorFeed"
        params = {
            "actor": actor,
            "limit": min(limit, 100)
        }
        
        if cursor:
            params["cursor"] = cursor
        
        return self.make_request_with_retry('get', url, params=params, headers=self.get_headers())
    
    def get_profile(self, actor):
        """Get profile information for a user."""
        url = f"{self.base_url}/app.bsky.actor.getProfile"
        params = {"actor": actor}
        
        return self.make_request_with_retry('get', url, params=params, headers=self.get_headers())
    
    def analyze_moderation_labels(self, post):
        """Extract moderation labels from a post."""
        labels = []
        
        # Check for labels on the post itself
        if 'labels' in post:
            for label in post['labels']:
                labels.append({
                    'type': 'post',
                    'value': label.get('val', 'unknown'),
                    'created': label.get('cts', 'unknown'),
                    'source': label.get('src', 'unknown')
                })
        
        # Check for labels on embedded content
        if 'embed' in post and 'labels' in post['embed']:
            for label in post['embed']['labels']:
                labels.append({
                    'type': 'embed',
                    'value': label.get('val', 'unknown'),
                    'created': label.get('cts', 'unknown'),
                    'source': label.get('src', 'unknown')
                })
        
        # Check author labels
        if 'author' in post and 'labels' in post['author']:
            for label in post['author']['labels']:
                labels.append({
                    'type': 'author',
                    'value': label.get('val', 'unknown'),
                    'created': label.get('cts', 'unknown'),
                    'source': label.get('src', 'unknown')
                })
        
        return labels
    
    def load_checkpoint(self, checkpoint_file):
        """Load collection progress from checkpoint file."""
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                logger.info(f"✓ Loaded checkpoint: {checkpoint['posts_collected']} posts collected")
                return checkpoint
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                return None
        return None
    
    def save_checkpoint(self, checkpoint_data, checkpoint_file):
        """Save collection progress to checkpoint file."""
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            logger.debug(f"Checkpoint saved: {checkpoint_data['posts_collected']} posts")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def collect_moderation_data_large_scale(
        self, 
        search_terms, 
        target_per_term=1000,
        output_file="moderation_data_large.csv",
        checkpoint_file="collection_checkpoint.json",
        checkpoint_interval=100
    ):
        """
        Collect large-scale moderation data with checkpointing.
        
        Args:
            search_terms: List of terms to search
            target_per_term: Target number of posts per search term
            output_file: Output CSV file
            checkpoint_file: Checkpoint file for crash recovery
            checkpoint_interval: Save checkpoint every N posts
        """
        # Load existing checkpoint
        checkpoint = self.load_checkpoint(checkpoint_file)
        
        if checkpoint:
            all_data = checkpoint.get('collected_data', [])
            completed_terms = checkpoint.get('completed_terms', [])
            term_progress = checkpoint.get('term_progress', {})
        else:
            all_data = []
            completed_terms = []
            term_progress = {}
        
        logger.info(f"Starting large-scale collection: {target_per_term} posts per term")
        logger.info(f"Search terms: {search_terms}")
        
        for term in search_terms:
            if term in completed_terms:
                logger.info(f"⊙ Skipping completed term: '{term}'")
                continue
            
            logger.info(f"\n→ Collecting posts for: '{term}'")
            term_start = term_progress.get(term, 0)
            collected_this_term = 0
            cursor = None
            consecutive_failures = 0
            max_consecutive_failures = 5
            
            while collected_this_term < target_per_term:
                # Calculate remaining posts needed
                remaining = target_per_term - collected_this_term
                batch_size = min(100, remaining)
                
                logger.info(f"  Batch request: {collected_this_term}/{target_per_term} posts collected")
                
                results = self.search_posts(term, limit=batch_size, cursor=cursor)
                
                if not results or 'posts' not in results:
                    consecutive_failures += 1
                    logger.warning(f"  No results (failure {consecutive_failures}/{max_consecutive_failures})")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logger.warning(f"  Max failures reached for '{term}', moving on")
                        break
                    
                    time.sleep(10)
                    continue
                
                consecutive_failures = 0
                posts = results['posts']
                
                if not posts:
                    logger.info(f"  No more posts available for '{term}'")
                    break
                
                logger.info(f"  Processing {len(posts)} posts from this batch")
                
                for post in posts:
                    # Extract post data
                    author = post.get('author', {})
                    record = post.get('record', {})
                    
                    # Get moderation labels
                    labels = self.analyze_moderation_labels(post)
                    
                    # Compile data
                    data_point = {
                        'timestamp': datetime.now().isoformat(),
                        'search_term': term,
                        'post_uri': post.get('uri', ''),
                        'post_cid': post.get('cid', ''),
                        'post_text': record.get('text', '')[:1000],  # Limit text length
                        'author_handle': author.get('handle', ''),
                        'author_did': author.get('did', ''),
                        'author_display_name': author.get('displayName', ''),
                        'author_followers': author.get('followersCount', 0),
                        'author_following': author.get('followsCount', 0),
                        'author_posts': author.get('postsCount', 0),
                        'likes_count': post.get('likeCount', 0),
                        'repost_count': post.get('repostCount', 0),
                        'reply_count': post.get('replyCount', 0),
                        'quote_count': post.get('quoteCount', 0),
                        'has_moderation': len(labels) > 0,
                        'moderation_labels': json.dumps(labels),
                        'label_count': len(labels),
                        'created_at': record.get('createdAt', ''),
                        'indexed_at': post.get('indexedAt', ''),
                        'has_embed': 'embed' in post,
                        'embed_type': post.get('embed', {}).get('$type', '') if 'embed' in post else ''
                    }
                    
                    all_data.append(data_point)
                    collected_this_term += 1
                    
                    # Checkpoint periodically
                    if len(all_data) % checkpoint_interval == 0:
                        checkpoint_data = {
                            'posts_collected': len(all_data),
                            'completed_terms': completed_terms,
                            'term_progress': {**term_progress, term: collected_this_term},
                            'collected_data': all_data,
                            'last_updated': datetime.now().isoformat()
                        }
                        self.save_checkpoint(checkpoint_data, checkpoint_file)
                        
                        # Also save to CSV periodically
                        self.save_to_csv(all_data, output_file)
                        logger.info(f"  ✓ Checkpoint saved: {len(all_data)} total posts")
                
                # Get cursor for pagination
                cursor = results.get('cursor')
                if not cursor or collected_this_term >= target_per_term:
                    break
                
                # Rate limiting between batches
                time.sleep(random.uniform(2, 4))
            
            logger.info(f"  ✓ Completed '{term}': {collected_this_term} posts collected")
            completed_terms.append(term)
            term_progress[term] = collected_this_term
            
            # Save after completing each term
            checkpoint_data = {
                'posts_collected': len(all_data),
                'completed_terms': completed_terms,
                'term_progress': term_progress,
                'collected_data': all_data,
                'last_updated': datetime.now().isoformat()
            }
            self.save_checkpoint(checkpoint_data, checkpoint_file)
            self.save_to_csv(all_data, output_file)
        
        # Final save
        self.save_to_csv(all_data, output_file)
        logger.info(f"\n✓ Collection complete! Total posts: {len(all_data)}")
        logger.info(f"✓ Saved to {output_file}")
        
        # Clean up checkpoint file
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            logger.info("✓ Checkpoint file cleaned up")
        
        return all_data
    
    def collect_stratified_sample(
        self,
        categories,
        target_per_category=1000,
        output_file="stratified_sample.csv"
    ):
        """
        Collect a stratified sample across different content categories.
        
        Args:
            categories: Dict of {category_name: [search_terms]}
            target_per_category: Posts to collect per category
            output_file: Output file
        """
        all_data = []
        
        for category, search_terms in categories.items():
            logger.info(f"\n{'='*60}")
            logger.info(f"CATEGORY: {category}")
            logger.info(f"{'='*60}")
            
            target_per_term = target_per_category // len(search_terms)
            
            category_data = self.collect_moderation_data_large_scale(
                search_terms=search_terms,
                target_per_term=target_per_term,
                output_file=f"{category}_temp.csv",
                checkpoint_file=f"{category}_checkpoint.json"
            )
            
            # Add category label
            for item in category_data:
                item['content_category'] = category
            
            all_data.extend(category_data)
        
        # Save combined data
        self.save_to_csv(all_data, output_file)
        logger.info(f"\n✓ Stratified sample complete: {len(all_data)} total posts")
        logger.info(f"✓ Saved to {output_file}")
        
        return all_data
    
    def collect_temporal_data(
        self,
        search_terms,
        days=7,
        collections_per_day=4,
        posts_per_collection=100,
        output_file="temporal_data.csv"
    ):
        """
        Collect data over multiple days to analyze temporal patterns.
        
        Args:
            search_terms: Terms to search
            days: Number of days to collect
            collections_per_day: How many times to collect per day
            posts_per_collection: Posts per collection
            output_file: Output file
        """
        all_data = []
        hours_between = 24 // collections_per_day
        
        logger.info(f"Starting temporal collection: {days} days, {collections_per_day} collections/day")
        
        for day in range(days):
            for collection in range(collections_per_day):
                timestamp = datetime.now()
                logger.info(f"\n→ Day {day+1}/{days}, Collection {collection+1}/{collections_per_day}")
                logger.info(f"  Time: {timestamp.isoformat()}")
                
                for term in search_terms:
                    results = self.search_posts(term, limit=posts_per_collection)
                    
                    if not results or 'posts' not in results:
                        continue
                    
                    for post in results['posts']:
                        author = post.get('author', {})
                        record = post.get('record', {})
                        labels = self.analyze_moderation_labels(post)
                        
                        data_point = {
                            'collection_timestamp': timestamp.isoformat(),
                            'collection_day': day + 1,
                            'collection_number': collection + 1,
                            'search_term': term,
                            'post_uri': post.get('uri', ''),
                            'post_text': record.get('text', '')[:1000],
                            'author_handle': author.get('handle', ''),
                            'author_followers': author.get('followersCount', 0),
                            'likes_count': post.get('likeCount', 0),
                            'repost_count': post.get('repostCount', 0),
                            'reply_count': post.get('replyCount', 0),
                            'has_moderation': len(labels) > 0,
                            'moderation_labels': json.dumps(labels),
                            'created_at': record.get('createdAt', ''),
                            'indexed_at': post.get('indexedAt', '')
                        }
                        
                        all_data.append(data_point)
                    
                    time.sleep(2)
                
                # Save after each collection
                self.save_to_csv(all_data, output_file)
                logger.info(f"  ✓ Saved: {len(all_data)} total posts")
                
                # Wait until next collection time (except for last collection of the day)
                if collection < collections_per_day - 1:
                    wait_seconds = hours_between * 3600
                    logger.info(f"  Waiting {hours_between} hours until next collection...")
                    time.sleep(wait_seconds)
        
        logger.info(f"\n✓ Temporal collection complete: {len(all_data)} posts over {days} days")
        return all_data
    
    def analyze_user_activity(self, handles, output_file="user_analysis.csv"):
        """Analyze moderation patterns across different user types."""
        user_data = []
        
        logger.info(f"Analyzing {len(handles)} user accounts...")
        
        for i, handle in enumerate(handles, 1):
            logger.info(f"\n→ [{i}/{len(handles)}] Analyzing user: {handle}")
            
            # Get profile
            profile = self.get_profile(handle)
            if not profile:
                logger.warning(f"  Could not fetch profile for {handle}")
                continue
            
            # Get recent posts
            feed = self.get_author_feed(handle, limit=100)
            if not feed or 'feed' not in feed:
                logger.warning(f"  Could not fetch feed for {handle}")
                continue
            
            posts_with_labels = 0
            total_labels = []
            label_types = defaultdict(int)
            total_engagement = {'likes': 0, 'reposts': 0, 'replies': 0}
            
            for item in feed['feed']:
                post = item.get('post', {})
                labels = self.analyze_moderation_labels(post)
                
                if labels:
                    posts_with_labels += 1
                    total_labels.extend(labels)
                    for label in labels:
                        label_types[label['value']] += 1
                
                # Track engagement
                total_engagement['likes'] += post.get('likeCount', 0)
                total_engagement['reposts'] += post.get('repostCount', 0)
                total_engagement['replies'] += post.get('replyCount', 0)
            
            posts_analyzed = len(feed['feed'])
            
            user_data.append({
                'handle': handle,
                'did': profile.get('did', ''),
                'display_name': profile.get('displayName', ''),
                'followers': profile.get('followersCount', 0),
                'following': profile.get('followsCount', 0),
                'total_posts': profile.get('postsCount', 0),
                'posts_analyzed': posts_analyzed,
                'posts_with_moderation': posts_with_labels,
                'moderation_rate': posts_with_labels / posts_analyzed if posts_analyzed > 0 else 0,
                'total_labels': len(total_labels),
                'unique_labels': len(set([l['value'] for l in total_labels])),
                'label_types': json.dumps(dict(label_types)),
                'avg_likes': total_engagement['likes'] / posts_analyzed if posts_analyzed > 0 else 0,
                'avg_reposts': total_engagement['reposts'] / posts_analyzed if posts_analyzed > 0 else 0,
                'avg_replies': total_engagement['replies'] / posts_analyzed if posts_analyzed > 0 else 0,
                'all_labels': json.dumps(total_labels),
                'analyzed_at': datetime.now().isoformat()
            })
            
            logger.info(f"  ✓ Posts: {posts_analyzed}, Moderated: {posts_with_labels} ({posts_with_labels/posts_analyzed*100:.1f}%)")
            
            time.sleep(random.uniform(2, 4))
        
        if user_data:
            self.save_to_csv(user_data, output_file)
            logger.info(f"\n✓ Saved analysis of {len(user_data)} users to {output_file}")
        
        return user_data
    
    def save_to_csv(self, data, filename):
        """Save data to CSV file."""
        if not data:
            logger.warning("No data to save")
            return
        
        keys = data[0].keys()
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)
            logger.debug(f"Saved {len(data)} records to {filename}")
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")


def main():
    """Main execution function with enhanced collection strategies."""
    
    # CONFIGURATION - Set your credentials
    BLUESKY_HANDLE = "amokkapati.bsky.social"
    BLUESKY_PASSWORD = ""
    
    if not BLUESKY_HANDLE or not BLUESKY_PASSWORD:
        logger.error("Please set BLUESKY_HANDLE and BLUESKY_PASSWORD environment variables")
        return
    
    print("=" * 70)
    print("BLUESKY CONTENT MODERATION - ENHANCED DATA COLLECTION")
    print("=" * 70)
    print()
    
    # Initialize collector
    collector = BlueskyModerationCollector(BLUESKY_HANDLE, BLUESKY_PASSWORD)
    
    # STRATEGY 1: Large-scale collection for specific categories
    print("\n[STRATEGY 1] Large-Scale Collection")
    print("-" * 70)
    
    # Separate NSFW and non-NSFW to get better balance
    nsfw_terms = ["nsfw", "nudity", "gore", "sexual", "porn"]
    non_nsfw_terms = ["spam", "scam", "misinformation", "fake news", "harassment", 
                      "hate speech", "violence", "bullying"]
    
    # Collect 1000 posts per term (adjust as needed)
    nsfw_data = collector.collect_moderation_data_large_scale(
        search_terms=nsfw_terms,
        target_per_term=1000,
        output_file="nsfw_moderation_data.csv",
        checkpoint_file="nsfw_checkpoint.json"
    )
    
    non_nsfw_data = collector.collect_moderation_data_large_scale(
        search_terms=non_nsfw_terms,
        target_per_term=1000,
        output_file="non_nsfw_moderation_data.csv",
        checkpoint_file="non_nsfw_checkpoint.json"
    )
    
    # STRATEGY 2: Stratified sampling across content categories
    print("\n[STRATEGY 2] Stratified Sampling")
    print("-" * 70)
    
    categories = {
        'adult_content': ['nsfw', 'nudity', 'sexual'],
        'violence': ['gore', 'violence', 'graphic'],
        'misinformation': ['fake news', 'misinformation', 'conspiracy'],
        'harassment': ['harassment', 'bullying', 'hate speech'],
        'spam': ['spam', 'scam', 'phishing']
    }
    
    stratified_data = collector.collect_stratified_sample(
        categories=categories,
        target_per_category=500,
        output_file="stratified_moderation_data.csv"
    )
    
    # STRATEGY 3: Temporal analysis (optional - takes time)
    # Uncomment if you want to collect data over multiple days
    """
    print("\n[STRATEGY 3] Temporal Analysis")
    print("-" * 70)
    
    temporal_data = collector.collect_temporal_data(
        search_terms=['nsfw', 'spam', 'misinformation'],
        days=7,
        collections_per_day=4,
        posts_per_collection=100,
        output_file="temporal_moderation_data.csv"
    )
    """
    
    # SUMMARY
    print("\n" + "=" * 70)
    print("COLLECTION SUMMARY")
    print("=" * 70)
    print(f"NSFW posts collected: {len(nsfw_data)}")
    print(f"Non-NSFW posts collected: {len(non_nsfw_data)}")
    print(f"Stratified sample collected: {len(stratified_data)}")
    print(f"\nTotal posts: {len(nsfw_data) + len(non_nsfw_data) + len(stratified_data)}")
    print("\nOutput files:")
    print("  - nsfw_moderation_data.csv")
    print("  - non_nsfw_moderation_data.csv")
    print("  - stratified_moderation_data.csv")
    print("\nLog file: bluesky_collection.log")


if __name__ == "__main__":
    main()