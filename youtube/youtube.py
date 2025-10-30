import os
import json
from googleapiclient.discovery import build
from datetime import datetime

# Set your YouTube API key here
YOUTUBE_API_KEY = "api_key"
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# Controversial terms for testing
controversial_terms = [
    "vaccine side effects",
    "election fraud claims",
    "conspiracy theory",
    "political protest",
    "mental health crisis",
    "climate change debate",
    "racial justice",
    "religious extremism",
    "medical misinformation",
    "political violence"
]

def search_videos(query, max_results=10):
    """Search for videos using a controversial term"""
    try:
        request = youtube.search().list(
            q=query,
            part='snippet',
            type='video',
            maxResults=max_results,
            order='relevance',
            regionCode='US'
        )
        response = request.execute()
        return response.get('items', [])
    except Exception as e:
        print(f"Error searching for '{query}': {e}")
        return []

def get_video_details(video_id):
    """Get detailed information about a video"""
    try:
        request = youtube.videos().list(
            part='contentDetails,statistics,status',
            id=video_id
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]
        return None
    except Exception as e:
        print(f"Error fetching details for video {video_id}: {e}")
        return None

def extract_moderation_signals(video_details):
    """Extract visible moderation signals from video data"""
    signals = {
        'embeddable': video_details.get('status', {}).get('embeddable', None),
        'public_stats_viewable': video_details.get('status', {}).get('publicStatsViewable', None),
        'made_for_kids': video_details.get('status', {}).get('madeForKids', None),
    }
    return signals

def collect_sample_data():
    """Main function to collect sample data"""
    all_data = []
    timestamp = datetime.now().isoformat()
    
    print(f"\n{'='*70}")
    print("YouTube Content Moderation Data Collection - Sample Study")
    print(f"Collection Date: {timestamp}")
    print(f"{'='*70}\n")
    
    for term in controversial_terms:
        print(f"Searching for: '{term}'")
        videos = search_videos(term, max_results=10)
        
        term_data = {
            'search_term': term,
            'videos_found': len(videos),
            'video_details': []
        }
        
        for idx, video in enumerate(videos, 1):
            video_id = video['id']['videoId']
            title = video['snippet']['title']
            channel = video['snippet']['channelTitle']
            
            # Get detailed video information
            details = get_video_details(video_id)
            
            if details:
                stats = details.get('statistics', {})
                moderation = extract_moderation_signals(details)
                
                video_record = {
                    'rank': idx,
                    'video_id': video_id,
                    'title': title,
                    'channel': channel,
                    'view_count': stats.get('viewCount', 'N/A'),
                    'like_count': stats.get('likeCount', 'N/A'),
                    'comment_count': stats.get('commentCount', 'N/A'),
                    'embeddable': moderation['embeddable'],
                    'public_stats_viewable': moderation['public_stats_viewable'],
                    'made_for_kids': moderation['made_for_kids'],
                    'url': f"https://www.youtube.com/watch?v={video_id}"
                }
                term_data['video_details'].append(video_record)
                print(f"  ✓ Video {idx}: {title[:50]}...")
            else:
                print(f"  ✗ Could not retrieve details for video {idx}")
        
        all_data.append(term_data)
        print()
    
    return all_data, timestamp

def analyze_data(all_data):
    """Analyze the collected data for moderation patterns"""
    print(f"\n{'='*70}")
    print("PRELIMINARY ANALYSIS")
    print(f"{'='*70}\n")
    
    total_videos = sum(term['videos_found'] for term in all_data)
    print(f"Total Videos Found: {total_videos}")
    print(f"Controversial Terms Searched: {len(all_data)}\n")
    
    non_embeddable = 0
    stats_hidden = 0
    made_for_kids = 0
    
    for term_data in all_data:
        term = term_data['search_term']
        videos = term_data['video_details']
        
        if videos:
            non_embed = sum(1 for v in videos if v['embeddable'] == False)
            hidden_stats = sum(1 for v in videos if v['public_stats_viewable'] == False)
            kids_flag = sum(1 for v in videos if v['made_for_kids'] == True)
            
            non_embeddable += non_embed
            stats_hidden += hidden_stats
            made_for_kids += kids_flag
            
            print(f"Term: '{term}'")
            print(f"  Videos found: {len(videos)}")
            print(f"  Non-embeddable: {non_embed} ({non_embed/len(videos)*100:.1f}%)")
            print(f"  Hidden stats: {hidden_stats} ({hidden_stats/len(videos)*100:.1f}%)")
            print(f"  Marked for kids: {kids_flag} ({kids_flag/len(videos)*100:.1f}%)")
            print()
    
    print(f"Overall Moderation Signals Detected:")
    if total_videos > 0:
        print(f"  Total non-embeddable videos: {non_embeddable}/{total_videos} ({non_embeddable/total_videos*100:.1f}%)")
        print(f"  Total with hidden stats: {stats_hidden}/{total_videos} ({stats_hidden/total_videos*100:.1f}%)")
        print(f"  Total marked for kids: {made_for_kids}/{total_videos} ({made_for_kids/total_videos*100:.1f}%)")
    else:
        print(f"  No videos found. Check your API key and quota.")

def save_results(all_data, timestamp):
    """Save collected data to JSON file"""
    filename = f"youtube_moderation_data_{timestamp.split('T')[0]}.json"
    with open(filename, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"\n✓ Data saved to: {filename}")

if __name__ == "__main__":
    # Step 1: Collect data
    data, ts = collect_sample_data()
    
    # Step 2: Analyze data
    analyze_data(data)
    
    # Step 3: Save results
    save_results(data, ts)
    
    print(f"\n{'='*70}")
    print("Sample collection complete! Review the JSON file for detailed data.")
    print("For your full project, increase max_results and add more search terms.")
    print(f"{'='*70}\n")