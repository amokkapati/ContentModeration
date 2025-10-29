import praw
import pandas as pd
from datetime import datetime, timezone
import time
import os
import json
from dotenv import load_dotenv
from prawcore.exceptions import NotFound, Forbidden

user_info_cache = {}

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    username=os.getenv('REDDIT_USERNAME'),
    password=os.getenv('REDDIT_PASSWORD'),
    user_agent='moderation_research_bot v1.0 by /u/' + os.getenv('REDDIT_USERNAME')
)

print("✓ Connected to Reddit as:", reddit.user.me())

def collect_user_info(author):
    """Safely collect user demographic information with caching"""
    if author is None:
        return {
            'username': '[deleted]',
            'account_age_days': None,
            'link_karma': None,
            'comment_karma': None,
            'total_karma': None,
            'is_verified': None,
            'author_unavailable': True
        }
    
    username = None

    try:
        username = str(author)
        
        if username == '[deleted]':
            return {
                'username': '[deleted]',
                'account_age_days': None,
                'link_karma': None,
                'comment_karma': None,
                'total_karma': None,
                'is_verified': None,
                'author_unavailable': True
            }
        
        if username in user_info_cache:
            return user_info_cache[username]
        
        if not hasattr(author, 'created_utc') or author.created_utc is None:
            raise AttributeError("Account data unavailable (likely suspended)")
        
        account_age_days = (datetime.now(timezone.utc) - datetime.fromtimestamp(author.created_utc, tz=timezone.utc)).days

        user_data = {
            'username': username,
            'account_age_days': account_age_days,
            'link_karma': author.link_karma,
            'comment_karma': author.comment_karma,
            'total_karma': author.link_karma + author.comment_karma,
            'is_verified': author.is_verified if hasattr(author, 'is_verified') else False,
            'author_unavailable': False
        }

        time.sleep(0.6)
        
        user_info_cache[username] = user_data
        return user_data
        
    except (NotFound, Forbidden, AttributeError) as e:  
        print(f"  (User {username} unavailable: {e}. Caching failure.)")
        user_info_cache[username] = {
            'username': username if username else '[error]',
            'account_age_days': None,
            'link_karma': None,
            'comment_karma': None,
            'total_karma': None,
            'is_verified': None,
            'author_unavailable': True
        }
        return user_info_cache[username]
    except Exception as e:
        print(f"  (Unexpected error collecting user {username}: {e}. Caching failure.)")
        user_info_cache[username] = {
            'username': '[error]',
            'account_age_days': None,
            'link_karma': None,
            'comment_karma': None,
            'total_karma': None,
            'is_verified': None,
            'author_unavailable': True
        }
        return user_info_cache[username]

def collect_post_data(post):
    """Collects the *initial* state of the post."""
    try:
        author_info = collect_user_info(post.author)
        body = getattr(post, "selftext", None)
        created_dt = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
        captured_at_utc = datetime.now(timezone.utc)
        
        return {
            'post_fullname': post.fullname, 
            
            'created_utc': created_dt,
            'captured_at_utc': captured_at_utc,
            'post_age_seconds_at_capture': (captured_at_utc - created_dt).total_seconds(),
            
            'subreddit': str(post.subreddit),
            'title': post.title,
            'selftext': body[:500] if body else '',
            'domain': post.domain,
            'initial_score': post.score, 
            'initial_num_comments': post.num_comments,
            'initial_upvote_ratio': post.upvote_ratio,
            'link_flair_text': post.link_flair_text,
            
            'author_username': author_info['username'],
            'author_account_age_days': author_info['account_age_days'],
            'author_total_karma': author_info['total_karma'],
            'author_link_karma': author_info['link_karma'],
            'author_comment_karma': author_info['comment_karma'],
            'author_is_verified': author_info['is_verified'],
            'author_unavailable': author_info['author_unavailable'],
        }
    except Exception as e:
        print(f"Error processing post {post.id}: {e}")
        return None


def collect_initial_cohort(subreddit_name, limit=100):
    """
    PASS 1: Collect fresh posts from /new before moderation
    Returns: DataFrame and list of post IDs
    """
    print(f"\n📊 [PASS 1] Collecting fresh posts from r/{subreddit_name}...")
    
    subreddit = reddit.subreddit(subreddit_name)
    posts_data = []
    post_fullnames = []
    
    posts = subreddit.new(limit=limit)
    
    for i, post in enumerate(posts, 1):
        if i % 100 == 0:
            print(f"  Processed {i} posts...")
        
        post_data = collect_post_data(post)
        if post_data:
            posts_data.append(post_data)
            post_fullnames.append(post_data['post_fullname'])
            
    print(f"✓ Collected {len(posts_data)} posts from r/{subreddit_name}")
    return pd.DataFrame(posts_data), post_fullnames

if __name__ == "__main__":
    start_time = time.time()
   
    subreddits = [
        # 'politics',      
        #'news',         
        'AskReddit',     
        # 'unpopularopinion',  
        # 'changemyview',  
        # 'worldnews',     
        # 'science',       
        # 'technology'     
    ]
    
    all_initial_data = []
    cohort_ids = {} 
    
    for subreddit in subreddits:
        try:
            df, post_fullnames = collect_initial_cohort(subreddit, limit=100)
            if df.empty:
                print(f"  No data collected for r/{subreddit}. Skipping.")
                continue
            all_initial_data.append(df)
            cohort_ids[subreddit] = post_fullnames
            
            os.makedirs('data/pass1', exist_ok=True)
            filename = f'data/pass1/{subreddit}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
            df.to_csv(filename, index=False)
            print(f"💾 Saved to {filename}")
            
            time.sleep(5)
            
        except Exception as e:
            print(f"❌ Error with r/{subreddit}: {e}")
            continue
    
    if all_initial_data:
        combined_df = pd.concat(all_initial_data, ignore_index=True)
        combined_filename = f'data/combined_pass1_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        combined_df.to_csv(combined_filename, index=False)

        cohort_filename = f'data/cohort_ids_{datetime.now().strftime("%Y%m%d_%H%M")}.json'
        with open(cohort_filename, 'w') as f:
            json.dump(cohort_ids, f, indent=2)

        
        end_time = time.time()
        
        print(f"\n✅ PASS 1 COMPLETE")
        print(f"Total posts collected: {len(combined_df)}")
        print(f"Cohort IDs saved to: {cohort_filename}")
        print(f"Total time taken: {((end_time - start_time) / 60):.2f} minutes")
    else:
        print("\n❌ No data collected")
