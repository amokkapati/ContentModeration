import requests
import json
import time
from datetime import datetime
import csv
import os

class BlueskyModerationCollector:
    def __init__(self, handle, password):
        """Initialize the Bluesky API collector with authentication."""
        self.base_url = "https://bsky.social/xrpc"
        self.handle = handle
        self.password = password
        self.session = None
        self.authenticate()
    
    def authenticate(self):
        """Authenticate with Bluesky and get session token."""
        url = f"{self.base_url}/com.atproto.server.createSession"
        data = {
            "identifier": self.handle,
            "password": self.password
        }
        
        try:
            response = requests.post(url, json=data)
            response.raise_for_status()
            self.session = response.json()
            print(f"✓ Authenticated as {self.handle}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"✗ Authentication failed: {e}")
            return False
    
    def get_headers(self):
        """Return authorization headers for API requests."""
        if not self.session:
            raise Exception("Not authenticated")
        return {
            "Authorization": f"Bearer {self.session['accessJwt']}"
        }
    
    def search_posts(self, query, limit=100):
        """Search for posts containing specific terms."""
        url = f"{self.base_url}/app.bsky.feed.searchPosts"
        params = {
            "q": query,
            "limit": limit
        }
        
        try:
            response = requests.get(url, params=params, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ Search failed: {e}")
            return None
    
    def get_author_feed(self, actor, limit=50):
        """Get posts from a specific user."""
        url = f"{self.base_url}/app.bsky.feed.getAuthorFeed"
        params = {
            "actor": actor,
            "limit": limit
        }
        
        try:
            response = requests.get(url, params=params, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ Failed to get author feed: {e}")
            return None
    
    def get_profile(self, actor):
        """Get profile information for a user."""
        url = f"{self.base_url}/app.bsky.actor.getProfile"
        params = {"actor": actor}
        
        try:
            response = requests.get(url, params=params, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ Failed to get profile: {e}")
            return None
    
    def analyze_moderation_labels(self, post):
        """Extract moderation labels from a post."""
        labels = []
        
        # Check for labels on the post itself
        if 'labels' in post:
            for label in post['labels']:
                labels.append({
                    'type': 'post',
                    'value': label.get('val', 'unknown'),
                    'created': label.get('cts', 'unknown')
                })
        
        # Check for labels on embedded content
        if 'embed' in post and 'labels' in post['embed']:
            for label in post['embed']['labels']:
                labels.append({
                    'type': 'embed',
                    'value': label.get('val', 'unknown'),
                    'created': label.get('cts', 'unknown')
                })
        
        return labels
    
    def collect_moderation_data(self, search_terms, output_file="moderation_data.csv"):
        """Collect posts and analyze moderation patterns."""
        all_data = []
        
        for term in search_terms:
            print(f"\n→ Searching for: '{term}'")
            results = self.search_posts(term, limit=100)
            
            if not results or 'posts' not in results:
                print(f"  No results found")
                continue
            
            print(f"  Found {len(results['posts'])} posts")
            
            for post in results['posts']:
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
                    'post_text': record.get('text', ''),
                    'author_handle': author.get('handle', ''),
                    'author_display_name': author.get('displayName', ''),
                    'author_followers': author.get('followersCount', 0),
                    'author_following': author.get('followsCount', 0),
                    'author_posts': author.get('postsCount', 0),
                    'likes_count': post.get('likeCount', 0),
                    'repost_count': post.get('repostCount', 0),
                    'reply_count': post.get('replyCount', 0),
                    'has_moderation': len(labels) > 0,
                    'moderation_labels': json.dumps(labels),
                    'created_at': record.get('createdAt', '')
                }
                
                all_data.append(data_point)
            
            # Rate limiting
            time.sleep(2)
        
        # Save to CSV
        if all_data:
            self.save_to_csv(all_data, output_file)
            print(f"\n✓ Saved {len(all_data)} posts to {output_file}")
        
        return all_data
    
    def analyze_user_activity(self, handles, output_file="user_analysis.csv"):
        """Analyze moderation patterns across different user types."""
        user_data = []
        
        for handle in handles:
            print(f"\n→ Analyzing user: {handle}")
            
            # Get profile
            profile = self.get_profile(handle)
            if not profile:
                continue
            
            # Get recent posts
            feed = self.get_author_feed(handle, limit=50)
            if not feed or 'feed' not in feed:
                continue
            
            posts_with_labels = 0
            total_labels = []
            
            for item in feed['feed']:
                post = item.get('post', {})
                labels = self.analyze_moderation_labels(post)
                if labels:
                    posts_with_labels += 1
                    total_labels.extend(labels)
            
            user_data.append({
                'handle': handle,
                'display_name': profile.get('displayName', ''),
                'followers': profile.get('followersCount', 0),
                'following': profile.get('followsCount', 0),
                'total_posts': profile.get('postsCount', 0),
                'posts_analyzed': len(feed['feed']),
                'posts_with_moderation': posts_with_labels,
                'moderation_rate': posts_with_labels / len(feed['feed']) if feed['feed'] else 0,
                'unique_labels': len(set([l['value'] for l in total_labels])),
                'all_labels': json.dumps(total_labels)
            })
            
            time.sleep(2)
        
        if user_data:
            self.save_to_csv(user_data, output_file)
            print(f"\n✓ Saved analysis of {len(user_data)} users to {output_file}")
        
        return user_data
    
    def save_to_csv(self, data, filename):
        if not data:
            return
        
        keys = data[0].keys()
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)


def main():
    """Main execution function."""
    BLUESKY_HANDLE = "" 
    BLUESKY_PASSWORD = "" 
    
    SEARCH_TERMS = [
    "spam",
    "scam",
    "fake news",
    "misinformation",
    "harassment"
    ]
    
    print("=" * 60)
    print("Bluesky Content Moderation Data Collection")
    print("=" * 60)
    
    # Initialize collector
    collector = BlueskyModerationCollector(BLUESKY_HANDLE, BLUESKY_PASSWORD)
    
    # Collect moderation data
    print("\n[1] Collecting posts with potential moderation...")
    moderation_data = collector.collect_moderation_data(SEARCH_TERMS)
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total posts collected: {len(moderation_data)}")
    print(f"Posts with moderation labels: {sum(1 for d in moderation_data if d['has_moderation'])}")
    print("\nData saved to:")
    print("  - moderation_data.csv")
    print("  - user_analysis.csv")


if __name__ == "__main__":
    main()