import requests
import json
import time
from datetime import datetime
import csv
import os
import logging
import random

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
        self.base_url = "https://bsky.social/xrpc"
        self.handle = handle
        self.password = password
        self.session = None
        self.request_count = 0
        self.rate_limit_window = 300
        self.max_requests_per_window = 3000
        self.window_start = time.time()
        self.authenticate()
    
    def authenticate(self):
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
                    time.sleep(5 * (attempt + 1))
                else:
                    logger.error("✗ Authentication failed after all retries")
                    return False
    
    def check_rate_limit(self):
        current_time = time.time()
        
        if current_time - self.window_start > self.rate_limit_window:
            self.request_count = 0
            self.window_start = current_time
        
        if self.request_count >= self.max_requests_per_window:
            sleep_time = self.rate_limit_window - (current_time - self.window_start)
            if sleep_time > 0:
                logger.warning(f"Rate limit approaching, sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time + 1)
                self.request_count = 0
                self.window_start = time.time()
        
        self.request_count += 1
    
    def get_headers(self):
        if not self.session:
            raise Exception("Not authenticated")
        return {
            "Authorization": f"Bearer {self.session['accessJwt']}"
        }
    
    def make_request_with_retry(self, method, url, max_retries=3, **kwargs):
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
                if response.status_code == 429:
                    wait_time = min(60 * (2 ** attempt), 300)
                    logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                elif response.status_code == 401:
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
        url = f"{self.base_url}/app.bsky.feed.searchPosts"
        params = {
            "q": query,
            "limit": min(limit, 100)
        }
        
        if cursor:
            params["cursor"] = cursor
        
        return self.make_request_with_retry('get', url, params=params, headers=self.get_headers())
    
    def analyze_moderation_labels(self, post):
        labels = []
        
        if 'labels' in post:
            for label in post['labels']:
                labels.append({
                    'type': 'post',
                    'value': label.get('val', 'unknown'),
                    'created': label.get('cts', 'unknown'),
                    'source': label.get('src', 'unknown')
                })
        
        if 'embed' in post and 'labels' in post['embed']:
            for label in post['embed']['labels']:
                labels.append({
                    'type': 'embed',
                    'value': label.get('val', 'unknown'),
                    'created': label.get('cts', 'unknown'),
                    'source': label.get('src', 'unknown')
                })
        
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
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            logger.debug(f"Checkpoint saved: {checkpoint_data['posts_collected']} posts")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def collect_moderation_data(
        self, 
        search_terms, 
        target_per_term=2000,
        output_file="moderation_data.csv",
        checkpoint_file="checkpoint.json",
        checkpoint_interval=100
    ):
        checkpoint = self.load_checkpoint(checkpoint_file)
        
        if checkpoint:
            all_data = checkpoint.get('collected_data', [])
            completed_terms = checkpoint.get('completed_terms', [])
            term_progress = checkpoint.get('term_progress', {})
        else:
            all_data = []
            completed_terms = []
            term_progress = {}
        
        logger.info(f"Starting collection: {target_per_term} posts per term")
        logger.info(f"Search terms: {search_terms}")
        
        for term in search_terms:
            if term in completed_terms:
                logger.info(f"⊙ Skipping completed term: '{term}'")
                continue
            
            logger.info(f"\n→ Collecting posts for: '{term}'")
            collected_this_term = 0
            cursor = None
            consecutive_failures = 0
            max_consecutive_failures = 5
            
            while collected_this_term < target_per_term:
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
                    author = post.get('author', {})
                    record = post.get('record', {})
                    labels = self.analyze_moderation_labels(post)
                    
                    data_point = {
                        'timestamp': datetime.now().isoformat(),
                        'search_term': term,
                        'post_uri': post.get('uri', ''),
                        'post_cid': post.get('cid', ''),
                        'post_text': record.get('text', '')[:1000],
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
                    
                    if len(all_data) % checkpoint_interval == 0:
                        checkpoint_data = {
                            'posts_collected': len(all_data),
                            'completed_terms': completed_terms,
                            'term_progress': {**term_progress, term: collected_this_term},
                            'collected_data': all_data,
                            'last_updated': datetime.now().isoformat()
                        }
                        self.save_checkpoint(checkpoint_data, checkpoint_file)
                        self.save_to_csv(all_data, output_file)
                        logger.info(f"  ✓ Checkpoint saved: {len(all_data)} total posts")
                
                cursor = results.get('cursor')
                if not cursor or collected_this_term >= target_per_term:
                    break
                
                time.sleep(random.uniform(2, 4))
            
            logger.info(f"  ✓ Completed '{term}': {collected_this_term} posts collected")
            completed_terms.append(term)
            term_progress[term] = collected_this_term
            
            checkpoint_data = {
                'posts_collected': len(all_data),
                'completed_terms': completed_terms,
                'term_progress': term_progress,
                'collected_data': all_data,
                'last_updated': datetime.now().isoformat()
            }
            self.save_checkpoint(checkpoint_data, checkpoint_file)
            self.save_to_csv(all_data, output_file)
        
        self.save_to_csv(all_data, output_file)
        logger.info(f"\n✓ Collection complete! Total posts: {len(all_data)}")
        logger.info(f"✓ Saved to {output_file}")
        
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            logger.info("✓ Checkpoint file cleaned up")
        
        return all_data
    
    def save_to_csv(self, data, filename):
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
    BLUESKY_HANDLE = "amokkapati.bsky.social"
    BLUESKY_PASSWORD = ""
    
    if not BLUESKY_PASSWORD:
        logger.error("Please set password")
        return
    
    collector = BlueskyModerationCollector(BLUESKY_HANDLE, BLUESKY_PASSWORD)
    
    nsfw_terms = ["nsfw", "nudity", "porn", "sexual", "gore"]
    non_nsfw_terms = ["spam", "scam", "harassment", "misinformation", "violence"]
    
    nsfw_data = collector.collect_moderation_data(
        search_terms=nsfw_terms,
        target_per_term=2000,
        output_file="nsfw_moderation_10k.csv",
        checkpoint_file="nsfw_checkpoint.json"
    )
    
    non_nsfw_data = collector.collect_moderation_data(
        search_terms=non_nsfw_terms,
        target_per_term=2000,
        output_file="non_nsfw_moderation_10k.csv",
        checkpoint_file="non_nsfw_checkpoint.json"
    )
    
    print(f"\nNSFW posts collected: {len(nsfw_data)}")
    print(f"Non-NSFW posts collected: {len(non_nsfw_data)}")
    print(f"Total posts: {len(nsfw_data) + len(non_nsfw_data)}")


if __name__ == "__main__":
    main()
