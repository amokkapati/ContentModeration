import os
import json
import csv
from googleapiclient.discovery import build
from datetime import datetime, timezone
import time
import random

# ⚠️ REPLACE WITH YOUR OWN API KEY (never share or commit this)
YOUTUBE_API_KEY = "AIzaSyDHknkDr53q1v-pzf7mdsPOC_dfU6uOBdo"  # ← CHANGE THIS!
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# 6 CATEGORIES × 10 EXTREME QUERIES EACH = 60 TOTAL (30 original + 30 new for 2025-2026 research)
guideline_searches = {
    "spam_deceptive_scams": [
        "free vbucks generator 2025 no human verification",
        "whatsapp hack spy tool 2025 download",
        "elon musk gives away bitcoin live now",
        "mrbeast $100000 giveaway enter now",
        "taylor swift concert tickets free claim now",
        "free psn codes 2026 no survey bypass",
        "roblox robux gift card generator 2026 working",
        "onlyfans premium account hack 2026 no verification",
        "netflix account generator 2026 free premium",
        "cashapp money flip live proof 2026 $50 to $5000"
    ],
    "sensitive_nudity_child_safety": [
        "teen bikini haul try on 2025",
        "asmr massage no bra full body oil",
        "how to get skinny in one week no eating tips",
        "mom and son roleplay gone wrong",
        "kids frozen elsa spiderman real life prank",
        "13 year old try on haul sheer lingerie 2026",
        "schoolgirl outfit try on no bra 2026",
        "extreme starvation challenge 7 days no food live",
        "step sister caught me challenge 2026 tiktok compilation",
        "little girl dress up frozen elsa real life 2026"
    ],
    "violent_dangerous_graphic": [
        "how to make a homemade silencer 3d printed",
        "cartel execution full video no blur 2025",
        "school fight knockout compilation 2025",
        "isis execution full footage 2024",
        "how to join a street gang 2025 guide",
        "funky town gore full video no blur 2026",
        "russian soldier ukraine war crimes full footage 2025",
        "how to make zip gun from hardware store 2026",
        "brazil favela execution compilation 2026",
        "one punch knockout street fights 2026 brutal"
    ],
    "hate_speech_harassment": [
        "why hitler was right full documentary",
        "great replacement theory full proof",
        "feminists deserve to be punished debate",
        "trump 2024 election was stolen full evidence",
        "all blacks are criminals rant 2025",
        "jews control the world full documentary 2026",
        "white genocide is real south africa farmer murders",
        "andrew tate women are property full speech 2026",
        "lgbt grooming kids proof compilation 2026",
        "islam is a death cult full rant 2026"
    ],
    "misinformation_medical_election": [
        "covid vaccine causes sudden death proof 2025",
        "drinking bleach cures cancer testimony",
        "5g towers cause cancer full documentary",
        "flat earth final proof 2025 nasa lies",
        "ivermectin cures cancer full story",
        "turbo cancer after mrna vaccine autopsy proof 2026",
        "chemtrails are real full documentary 2026",
        "2024 election machines hacked live proof dominion",
        "adrenochrome harvesting hollywood full exposé 2026",
        "pine needle tea cures all vaccine shedding 2026"
    ],
    "regulated_goods_firearms_drugs": [
        "buy untraceable ghost gun kit 2025",
        "how to buy fentanyl online dark web guide 2025",
        "3d printed liberator pistol full build",
        "buy steroids no prescription 2025",
        "make ricin at home full tutorial",
        "buy glock switch auto sear 2026 no background check",
        "how to cook meth breaking bad method 2026 full guide",
        "printable ar15 lower stl file 2026 download",
        "order xylazine tranq online 2026 veterinary source",
        "diy tannerite explosive recipe 2026 binary"
    ]
}

def search_videos_by_guideline(category, query, max_results=5):
    """Search YouTube – only top 5 results per query"""
    try:
        request = youtube.search().list(
            q=query,
            part='snippet',
            type='video',
            maxResults=max_results,
            order='date',            # Most recent first → catches fresh re-uploads
            regionCode='US',
            relevanceLanguage='en'
        )
        response = request.execute()
        return response.get('items', []), category
    except Exception as e:
        print(f"Error searching '{query}': {e}")
        return [], category

def get_video_details(video_id):
    try:
        request = youtube.videos().list(
            part='statistics,status,snippet,contentDetails',
            id=video_id
        )
        response = request.execute()
        return response['items'][0] if response.get('items') else None
    except Exception as e:
        return None

def get_channel_details(channel_id):
    try:
        request = youtube.channels().list(
            part='statistics,snippet,status',
            id=channel_id
        )
        response = request.execute()
        return response['items'][0] if response.get('items') else None
    except Exception as e:
        return None

def assess_moderation_status(video_data, channel_data):
    stats = video_data.get('statistics', {})
    status = video_data.get('status', {})
    snippet = video_data.get('snippet', {})
    content_details = video_data.get('contentDetails', {})

    views = int(stats.get('viewCount', 0)) if stats.get('viewCount') else 0
    comments = int(stats.get('commentCount', 0)) if stats.get('commentCount') else 0
    likes = int(stats.get('likeCount', 0)) if stats.get('likeCount') else 0

    # === Calculate exact age of the video ===
    published_at_str = snippet.get('publishedAt')  # e.g. "2025-11-28T14:22:10Z"
    if published_at_str:
        published_dt = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
        days_since_upload = (datetime.now(timezone.utc) - published_dt).days
        upload_date = published_dt.strftime("%Y-%m-%d")
    else:
        days_since_upload = None
        upload_date = None

    engagement_rate = round(((likes + comments) / views) * 100, 2) if views > 0 else 0

    channel_subs = int(channel_data.get('statistics', {}).get('subscriberCount', 0)) if channel_data else 0

    is_restricted = (
        not status.get('embeddable', True) or
        not status.get('publicStatsViewable', True) or
        status.get('privacyStatus') != 'public' or
        video_data.get('status', {}).get('uploadStatus') in ['failed', 'rejected'] or
        content_details.get('contentRating', {}).get('ytRating') == 'ytAgeRestricted'
    )

    return {
        'views': views,
        'likes': likes,
        'comments': comments,
        'engagement_rate': engagement_rate,
        'channel_subscribers': channel_subs,
        'is_embeddable': status.get('embeddable', True),
        'stats_public': status.get('publicStatsViewable', True),
        'privacy_status': status.get('privacyStatus', 'unknown'),
        'made_for_kids': status.get('madeForKids', False),
        'appears_restricted_or_removed': is_restricted,
        'published_at': snippet.get('publishedAt'),
        'upload_date': upload_date,                    # ← NEW: Human-readable date
        'days_since_upload': days_since_upload,        # ← NEW: Integer days old
        'duration': content_details.get('duration', 'N/A'),  # ← Bonus: ISO duration
        'category_id': snippet.get('categoryId')
    }

def collect_guideline_data():
    all_data = []
    timestamp = datetime.now().isoformat()

    print("="*90)
    print("YOUTUBE EXTREME VIOLATIONS MONITORING TOOL – Academic Research Only")
    print(f"Start Time: {timestamp}")
    print("="*90)

    total_searches = sum(len(queries) for queries in guideline_searches.values())
    completed = 0

    for category, queries in guideline_searches.items():
        print(f"\n[{category.upper().replace('_', ' ')}]")

        for query in queries:
            completed += 1
            print(f"  [{completed:2d}/{total_searches}] Searching: {query}")

            videos, _ = search_videos_by_guideline(category, query, max_results=5)

            if not videos:
                print("    No results (likely all removed or heavily suppressed)")

            for idx, video in enumerate(videos, 1):
                video_id = video['id']['videoId']
                title = video['snippet']['title']
                channel = video['snippet']['channelTitle']
                channel_id = video['snippet']['channelId']

                video_data = get_video_details(video_id)
                if not video_data:
                    status = "REMOVED OR PRIVATE"
                    print(f"    {idx}. REMOVED OR PRIVATE | {title[:60]}...")
                    all_data.append({
                        'collection_date': timestamp,
                        'guideline_category': category,
                        'search_query': query,
                        'search_rank': idx,
                        'video_id': video_id,
                        'title': title,
                        'channel': channel,
                        'status': 'removed_or_private_unavailable',
                        'url': f"https://www.youtube.com/watch?v={video_id}"
                    })
                    continue

                channel_data = get_channel_details(channel_id)
                assessment = assess_moderation_status(video_data, channel_data)

                restriction_marker = "SURVIVED" if not assessment['appears_restricted_or_removed'] else "RESTRICTED"

                # === Pretty age string for console ===
                days = assessment['days_since_upload']
                age_str = f"{days}d ago" if days is not None else "???"
                print(f"    {idx}. {restriction_marker} | Views: {assessment['views']:>10,} | {age_str:>8} | {title[:58]}...")

                all_data.append({
                    'collection_date': timestamp,
                    'guideline_category': category,
                    'search_query': query,
                    'search_rank': idx,
                    'video_id': video_id,
                    'title': title,
                    'channel': channel,
                    'channel_id': channel_id,
                    'published_at': assessment['published_at'],
                    'upload_date': assessment['upload_date'],
                    'days_since_upload': assessment['days_since_upload'],
                    'duration': assessment['duration'],
                    'views': assessment['views'],
                    'likes': assessment['likes'],
                    'comments': assessment['comments'],
                    'engagement_rate': assessment['engagement_rate'],
                    'channel_subscribers': assessment['channel_subscribers'],
                    'is_embeddable': assessment['is_embeddable'],
                    'stats_public': assessment['stats_public'],
                    'privacy_status': assessment['privacy_status'],
                    'appears_restricted_or_removed': assessment['appears_restricted_or_removed'],
                    'url': f"https://www.youtube.com/watch?v={video_id}"
                })

            time.sleep(random.uniform(1.0, 2.5))  # Be kind to quota

    return all_data, timestamp

def save_data(all_data, timestamp):
    date_str = timestamp.split('T')[0]
    json_file = f"youtube_extreme_violations_{date_str}.json"
    csv_file = f"youtube_extreme_violations_{date_str}.csv"

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    print(f"\nData saved → {json_file}")

    if all_data:
        keys = all_data[0].keys()
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_data)
        print(f"Data saved → {csv_file}")

if __name__ == "__main__":
    print("Starting scan for extreme Community Guidelines violations...\n")
    data, ts = collect_guideline_data()

    survived = sum(1 for x in data if not x.get('appears_restricted_or_removed') and x.get('status') != 'removed_or_private_unavailable')
    total_found = len([x for x in data if 'video_id' in x])

    print("\n" + "="*90)
    print("SUMMARY")
    print("="*90)
    print(f"Total videos returned (including removed): {len(data)}")
    print(f"Still publicly viewable & not restricted : {survived} ← This is your key finding!")
    print(f"Percentage surviving detection             : {survived/total_found*100:.1f}%")
    print("="*90)

    save_data(data, ts)
    print("\nScan complete. Use this data to show real-world enforcement inconsistencies.")
