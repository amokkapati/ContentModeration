import praw
import pandas as pd
from datetime import datetime, timezone
import time
import os
import json
from dotenv import load_dotenv
import glob

load_dotenv()
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    username=os.getenv('REDDIT_USERNAME'),
    password=os.getenv('REDDIT_PASSWORD'),
    user_agent='moderation_research_bot v2.0 by /u/' + (os.getenv('REDDIT_USERNAME') or 'unknown_user')
)
print("✓ Connected to Reddit as:", reddit.user.me())

def get_final_post_status(post_fullnames):
    """
    Takes a list of post fullnames (e.g., ['t3_abc', 't3_def'])
    and returns a dictionary of their final status.
    This is 100x faster than checking one by one.
    """
    print(f"  Fetching final status for {len(post_fullnames)} posts in batches...")
    status_map = {}
    
    for i in range(0, len(post_fullnames), 100):
        batch_ids = post_fullnames[i:i+100]
        
        try:
            for post in reddit.info(fullnames=batch_ids):
                body = getattr(post, "selftext", None)
                
                is_removed_official = post.removed_by_category is not None
                is_removed_content = (body == '[removed]') or (post.title == '[removed]')
                is_deleted_content = (body == '[deleted]')
                is_deleted_author = post.author is None or str(post.author) == '[deleted]'
                
                is_deleted = is_deleted_author or is_deleted_content
                is_removed_inferred = (
                    is_removed_official
                    or is_removed_content
                    or (hasattr(post, 'is_robot_indexable') and post.is_robot_indexable is False)
                )
                
                status_map[post.fullname] = {
                    'final_score': post.score,
                    'final_num_comments': post.num_comments,
                    'final_upvote_ratio': post.upvote_ratio,
                    'is_removed_official': is_removed_official,
                    'is_removed_content': is_removed_content,
                    'is_removed_inferred': is_removed_inferred,
                    'removed_by_category': post.removed_by_category,
                    'is_deleted': is_deleted,
                    'is_deleted_author': is_deleted_author,
                    'is_deleted_content': is_deleted_content,
                    'is_locked': post.locked,
                    'is_archived': post.archived,
                    'rechecked_at_utc': datetime.now(timezone.utc) 
                }

            if i + 100 < len(post_fullnames):  
                time.sleep(2)

        except Exception as e:
            print(f"    Error in batch {i}-{i+100}: {e}")

    print(f"  ✓ Found status for {len(status_map)} posts.")
    return status_map


if __name__ == "__main__":
    
   
    id_files = glob.glob("data/cohort_ids_*.json")
    if not id_files:
        print("❌ No 'cohort_ids_*.json' file found in 'data/'.")
        print("Run '1_collect_pending_posts.py' first.")
        exit()
        
    latest_id_file = max(id_files, key=os.path.getctime)
    print(f"Found cohort ID file: {latest_id_file}")
    
    with open(latest_id_file, 'r') as f:
        cohort_ids_by_sub = json.load(f)

    print(f"Found {len(cohort_ids_by_sub)} subreddits to check...")
    os.makedirs('data/pass2_complete', exist_ok=True)
    
    all_final_data = []

    for subreddit, post_fullnames in cohort_ids_by_sub.items():
        print(f"\nProcessing r/{subreddit}...")
        
        try:
            if not post_fullnames:
                print(f"  SKIPPING: No post IDs found for r/{subreddit}.")
                continue
                
            pass1_files = glob.glob(f"data/pass1/{subreddit}_*.csv")
            if not pass1_files:
                print(f"  SKIPPING: No Pass 1 data file found for r/{subreddit}.")
                continue
                
            latest_pass1_file = max(pass1_files, key=os.path.getctime)
            print(f"  Loading initial data from: {latest_pass1_file}")
            df = pd.read_csv(latest_pass1_file)
            
            status_data = get_final_post_status(post_fullnames)
            
            status_df = pd.DataFrame.from_dict(status_data, orient='index')
            status_df.index.name = 'post_fullname'
            
            
            final_df = df.set_index('post_fullname').join(status_df)
            final_df.reset_index(inplace=True) 
            
            new_filename = f"{subreddit}_complete_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            new_filepath = os.path.join('data/pass2_complete', new_filename)
            final_df.to_csv(new_filepath, index=False)
            
            print(f"  💾 Saved final data to {new_filepath}")
            all_final_data.append(final_df)
            
        except Exception as e:
            print(f"  ❌ Failed to process 'r/{subreddit}': {e}")
            
    if all_final_data:
        print("\nCombining all processed data...")
        combined_df = pd.concat(all_final_data, ignore_index=True)
        combined_filename = f'data/combined_complete_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        combined_df.to_csv(combined_filename, index=False)
        print(f"  💾 Saved combined data to {combined_filename}")
        
        print("\n✅ FINAL CHECK COMPLETE")
        print(f"Total posts processed: {len(combined_df)}")
        
        print(f"\nQuick stats (from combined data):")
        if 'is_removed_official' in combined_df.columns:
            print(f"  Official removal rate: {combined_df['is_removed_official'].mean()*100:.1f}%")
            print(f"  Inferred removal rate: {combined_df['is_removed_inferred'].mean()*100:.1f}%")
            print(f"  Locked rate: {combined_df['is_locked'].mean()*100:.1f}%")
            print(f"  Subreddits: {combined_df['subreddit'].nunique()}")
        else:
            print("  Could not calculate stats. Check for missing columns (e.g., 'is_removed_official').")
    else:
        print("\n❌ No data was processed.")

