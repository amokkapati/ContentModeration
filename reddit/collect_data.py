import os
import re
import json
import time
import asyncio
from datetime import datetime, timedelta
from textblob import TextBlob
from typing import Dict, Any, List, Optional, Tuple, Set
import logging
import random
import pandas as pd
from dotenv import load_dotenv
from collections import defaultdict, OrderedDict
import ssl, certifi, aiohttp
import math
import asyncpraw
from asyncprawcore.exceptions import NotFound, Forbidden, ServerError, TooManyRequests
MAX_RETRIES = 3
INITIAL_BACKOFF = 4
PER_ITEM_TIMEOUT = 20
MAX_CONCURRENCY = 4
GENTLE_DELAY_SEC = 0.25
MAX_CACHE_SIZE = 100000
RATE_LIMIT_CRITICAL_THRESHOLD = 10
MAX_RETRY_JITTER_SEC = 2
TARGET_POSTS_PER_SUB = 500
MIN_ACCEPTABLE_POSTS = 50
SAMPLING_METHOD = 'multi_sort'
SUBREDDIT_LIST_FILE = 'data/top_500_subreddits.json'
PASS1_DIR = 'data/pass1'
COMBINED_DIR = 'data'
COHORT_IDS_DIR = 'data'
PROGRESS_FILE = 'data/collection_progress.json'
USER_CACHE_FILE = 'data/user_cache.json'


def load_user_cache():
    if os.path.exists(USER_CACHE_FILE):
        try:
            with open(USER_CACHE_FILE, 'r') as f:
                cache = json.load(f, object_pairs_hook=OrderedDict)
                print(f'✓ Loaded user cache from disk ({len(cache):,} entries)'
                    )
                return cache
        except Exception as e:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            os.rename(USER_CACHE_FILE, f'{USER_CACHE_FILE}.corrupt_{timestamp}'
                )
            print(
                f'⚠️ Cache corruption detected: {e}. Renamed file and starting with empty cache.'
                )
    return OrderedDict()


def get_comment_sentiment(text):
    if not text:
        return 0
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return 0


async def save_user_cache(cache):
    try:
        async with _cache_lock:
            cache_snapshot = dict(cache)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _write_cache_to_disk, cache_snapshot)
    except Exception as e:
        print(f'⚠️ Could not save user cache: {e}')


def _write_cache_to_disk(data):
    temp_file = f'{USER_CACHE_FILE}.tmp'
    try:
        with open(temp_file, 'w') as f:
            json.dump(data, f)
        os.replace(temp_file, USER_CACHE_FILE)
    except Exception as e:
        print(f'⚠️ Disk write failed: {e}')
        if os.path.exists(temp_file):
            os.remove(temp_file)


user_info_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
_user_locks = defaultdict(asyncio.Lock)
subreddit_meta_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
_cache_lock = asyncio.Lock()
sem = asyncio.Semaphore(MAX_CONCURRENCY)


class ProgressTracker:

    def __init__(self, total_subreddits: int):
        self.total_subreddits = total_subreddits
        self.completed_subreddits = 0
        self.total_posts_collected = 0
        self.total_posts_attempted = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.subreddit_times = []
        self.moving_avg_window = 20

    def update(self, posts_collected: int, posts_attempted: int):
        self.completed_subreddits += 1
        self.total_posts_collected += posts_collected
        self.total_posts_attempted += posts_attempted
        current_time = time.time()
        subreddit_duration = current_time - self.last_update_time
        self.subreddit_times.append(subreddit_duration)
        if len(self.subreddit_times) > self.moving_avg_window:
            self.subreddit_times.pop(0)
        self.last_update_time = current_time

    def get_eta(self) ->str:
        if not self.subreddit_times:
            return 'Calculating...'
        avg_time_per_sub = sum(self.subreddit_times) / len(self.subreddit_times
            )
        remaining_subs = self.total_subreddits - self.completed_subreddits
        eta_seconds = avg_time_per_sub * remaining_subs
        return str(timedelta(seconds=int(eta_seconds)))

    def get_progress_bar(self, width=40) ->str:
        completed = int(width * self.completed_subreddits / self.
            total_subreddits)
        bar = '█' * completed + '░' * (width - completed)
        percentage = self.completed_subreddits / self.total_subreddits * 100
        return f'[{bar}] {percentage:.1f}%'

    def print_status(self):
        elapsed = time.time() - self.start_time
        print('\n' + '=' * 70)
        print(f'📊 COLLECTION PROGRESS')
        print('=' * 70)
        print(
            f'Subreddits: {self.completed_subreddits}/{self.total_subreddits}')
        print(self.get_progress_bar())
        safe_attempted = max(self.total_posts_attempted, self.
            total_posts_collected, self.completed_subreddits *
            TARGET_POSTS_PER_SUB)
        print(
            f'Posts collected: {self.total_posts_collected:,} / {safe_attempted:,} attempted'
            )
        success_rate = (self.total_posts_collected / safe_attempted * 100 if
            safe_attempted > 0 else 0)
        print(f'Success rate: {success_rate:.1f}%')
        print(f'ETA: {self.get_eta()}')
        print(
            f'Avg per subreddit: {elapsed / self.completed_subreddits:.1f}s' if
            self.completed_subreddits > 0 else '')
        print('=' * 70 + '\n')


def save_progress(completed_subs: List[str], stats: Dict[str, Any]):
    progress = {'last_updated': datetime.now().isoformat(),
        'completed_subreddits': completed_subs, 'statistics': stats}
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def load_progress() ->Tuple[List[str], Dict[str, Any]]:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
            return progress.get('completed_subreddits', []), progress.get(
                'statistics', {})
        except Exception as e:
            print(f'⚠️ Could not load progress file: {e}. Starting fresh.')
    return [], {}


def setup_logging(subreddit_name=None):
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    if subreddit_name:
        log_file = f'{log_dir}/pass1_{subreddit_name}_{timestamp}.log'
    else:
        log_file = f'{log_dir}/pass1_main_{timestamp}.log'
    logging.basicConfig(level=logging.INFO, format=
        '%(asctime)s [%(levelname)s] %(message)s', handlers=[logging.
        FileHandler(log_file), logging.StreamHandler()])
    return logging.getLogger(__name__)


logger = setup_logging()


def analyze_title_features(title: str) ->Dict[str, Any]:
    if not title:
        return {'title_length': 0, 'title_word_count': 0,
            'title_has_question': False, 'title_has_exclamation': False,
            'title_all_caps_ratio': 0.0, 'title_has_non_ascii': False,
            'title_has_number': False}
    capitals = sum(1 for c in title if c.isupper())
    letters = sum(1 for c in title if c.isalpha())
    caps_ratio = capitals / letters if letters else 0.0
    return {'title_length': len(title), 'title_word_count': len(title.split
        ()), 'title_has_question': '?' in title, 'title_has_exclamation': 
        '!' in title, 'title_all_caps_ratio': caps_ratio,
        'title_has_non_ascii': bool(re.search('[^\\x00-\\x7F]', title)),
        'title_has_number': bool(re.search('\\d', title))}


def analyze_body_features(body: str) ->Dict[str, Any]:
    if not body:
        return {'body_length': 0, 'body_word_count': 0, 'body_has_url': 
            False, 'body_url_count': 0, 'body_paragraph_count': 0}
    urls = re.findall(
        'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\\\(\\\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        , body)
    paragraphs = [p.strip() for p in body.split('\n\n') if p.strip()]
    return {'body_length': len(body), 'body_word_count': len(body.split()),
        'body_has_url': len(urls) > 0, 'body_url_count': len(urls),
        'body_paragraph_count': len(paragraphs)}


def categorize_domain(domain: str) ->str:
    d = (domain or '').lower()
    if any(x in d for x in ['youtube.com', 'youtu.be', 'vimeo.com',
        'dailymotion.com', 'v.redd.it']):
        return 'video_platform'
    if any(x in d for x in ['imgur.com', 'i.redd.it', 'gfycat.com']):
        return 'image_host'
    if 'reddit.com' in d or d.startswith('self.'):
        return 'reddit_native'
    if any(x in d for x in ['twitter.com', 'x.com', 'facebook.com',
        'instagram.com', 'tiktok.com']):
        return 'social_media'
    if d.endswith('.gov') or d.endswith('.edu'):
        return 'authoritative'
    if any(x in d for x in ['wikipedia.org', 'wikimedia.org', 'archive.org',
        'ietf.org', 'w3.org']):
        return 'authoritative'
    if any(x in d for x in ['nytimes.com', 'bbc.com', 'cnn.com',
        'reuters.com', 'apnews.com', 'theguardian.com', 'wsj.com']):
        return 'news_major'
    if any(x in d for x in ['blogspot', 'wordpress', 'medium.com',
        'substack.com']):
        return 'blog'
    return 'other'


def extract_domain_info(submission) ->Dict[str, Any]:
    raw_domain = getattr(submission, 'domain', None)
    is_self = getattr(submission, 'is_self', False)
    domain = raw_domain or ''
    url = '' if is_self else getattr(submission, 'url', '') or ''
    return {'domain': domain, 'domain_type': categorize_domain(domain),
        'url': url, 'is_external_link': not is_self and 'reddit.com' not in
        domain}


def get_post_type(submission) ->str:
    if getattr(submission, 'is_self', False):
        return 'text'
    if getattr(submission, 'is_video', False):
        return 'video'
    domain = getattr(submission, 'domain', '') or ''
    if domain.startswith('i.redd.it') or 'imgur' in domain:
        return 'image'
    return 'link'


def calculate_controversy(score: Optional[int], upvote_ratio: Optional[float]
    ) ->float:
    if score is None or upvote_ratio is None:
        return 0.0
    return round((1 - abs(upvote_ratio - 0.5) * 2) * abs(score), 2)


def analyze_engagement(submission) ->Dict[str, Any]:
    score_raw = getattr(submission, 'score', 0) or 0
    score = max(score_raw, 0)
    num_comments = getattr(submission, 'num_comments', 0) or 0
    ratio = getattr(submission, 'upvote_ratio', None)
    eng_rate = num_comments / score if score > 0 else 0.0
    total_awards = getattr(submission, 'total_awards_received', None)
    if total_awards is None:
        try:
            raw_awards = getattr(submission, 'all_awardings', []) or []
            counts = []
            for a in raw_awards:
                if isinstance(a, dict):
                    counts.append(a.get('count', 1))
                else:
                    counts.append(getattr(a, 'count', 1))
            total_awards = sum(counts)
        except Exception:
            total_awards = 0
    if total_awards is None:
        total_awards = 0
    return {'initial_score': score, 'initial_score_raw': score_raw,
        'initial_num_comments': num_comments, 'initial_upvote_ratio': ratio,
        'num_awards': int(total_awards), 'engagement_rate': round(eng_rate,
        4), 'controversy_score': calculate_controversy(score, ratio)}


async def _list_subreddit_rules(subreddit):
    rules = []
    async for rule in subreddit.rules:
        rules.append(rule)
    return rules


async def with_retry(func, *args, max_attempts=4, base_delay=1.5):
    for attempt in range(1, max_attempts + 1):
        try:
            return await func(*args)
        except TooManyRequests as e:
            wait = getattr(e, 'sleep', base_delay * attempt)
            print(f'⏳ 429 for {func.__name__}, sleeping {wait:.1f}s...')
            await asyncio.sleep(wait)
        except (aiohttp.ClientError, ServerError) as e:
            wait = base_delay * attempt
            print(
                f'⚠️ {type(e).__name__} during {func.__name__}, retry in {wait:.1f}s...'
                )
            await asyncio.sleep(wait)
        except Exception:
            if attempt == max_attempts:
                raise
    return None


async def fetch_user_info(reddit: asyncpraw.Reddit, username: str) ->Dict[
    str, Any]:
    async with _cache_lock:
        if username in user_info_cache:
            user_info_cache.move_to_end(username)
            return user_info_cache[username]
    async with _user_locks[username]:
        async with _cache_lock:
            if username in user_info_cache:
                return user_info_cache[username]
        info = {'author_account_age_days': None, 'author_total_karma': None,
            'author_is_verified': False, 'author_is_gold': False}
        try:
            user = await reddit.redditor(username)
            await asyncio.wait_for(user.load(), timeout=3.0)
            created = getattr(user, 'created_utc', None)
            if created:
                info['author_account_age_days'] = round((time.time() -
                    created) / 86400, 2)
            info['author_total_karma'] = getattr(user, 'link_karma', 0
                ) + getattr(user, 'comment_karma', 0)
            info['author_is_verified'] = getattr(user, 'verified', False)
            info['author_is_gold'] = getattr(user, 'is_gold', False)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.warning(
                f'User fetch failed for {username}: {type(e).__name__}: {e}')
            pass
        async with _cache_lock:
            user_info_cache[username] = info
            if len(user_info_cache) > MAX_CACHE_SIZE:
                user_info_cache.popitem(last=False)
        return info


async def fetch_subreddit_metadata(subreddit) ->Dict[str, Any]:
    name = subreddit.display_name.lower()
    async with _cache_lock:
        if name in subreddit_meta_cache:
            return subreddit_meta_cache[name]
    try:
        subscribers = getattr(subreddit, 'subscribers', 0) or 0
        is_nsfw = getattr(subreddit, 'over18', False)
        created = getattr(subreddit, 'created_utc', None)
        age_days = (time.time() - created) / 86400 if created else None
    except Exception as e:
        print(f'⚠️ Error reading basic metadata for r/{name}: {e}')
        subscribers = 0
        is_nsfw = False
        age_days = None
    try:
        rules = await with_retry(_list_subreddit_rules, subreddit)
        rules_count = len(rules) if rules else 0
    except Exception as e:
        print(f'⚠️ Error fetching rules for r/{name}: {e}')
        rules_count = 0
    meta = {'subreddit': subreddit.display_name, 'subscribers': subscribers,
        'rules_count': rules_count, 'is_nsfw': is_nsfw, 'age_days': age_days}
    async with _cache_lock:
        subreddit_meta_cache[name] = meta
    return meta


async def collect_multi_sort_posts(reddit: asyncpraw.Reddit, subreddit_name:
    str, target_unique: int=500, include_hot: bool=True) ->List:
    sr = await reddit.subreddit(subreddit_name, fetch=True)
    seen_ids: set[str] = set()
    posts: List = []
    base_sorts = [('new', sr.new), ('rising', sr.rising)]
    if include_hot:
        base_sorts.append(('hot', sr.hot))
    OVERSAMPLE_FACTOR = 1.8
    TOPUP_OVERSAMPLE_FACTOR = 3.0
    MAX_LISTING_LIMIT = 1000
    per_sort_limit = min(MAX_LISTING_LIMIT, math.ceil(target_unique *
        OVERSAMPLE_FACTOR / len(base_sorts)))
    print(f'  Sampling from {len(base_sorts)} sort methods...')
    for sort_name, sort_fn in base_sorts:
        if sort_name == 'hot':
            limit = min(MAX_LISTING_LIMIT, max(200, per_sort_limit))
        else:
            limit = per_sort_limit
        added = 0
        try:
            async for submission in sort_fn(limit=limit):
                sid = getattr(submission, 'id', None)
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    posts.append(submission)
                    added += 1
                if len(posts) >= target_unique:
                    break
        except Exception as e:
            logger.warning(f'Error sampling from {sort_name}: {e}')
        print(f'    {sort_name}: added {added} unique posts')
        if len(posts) >= target_unique:
            break
    if len(posts) < target_unique:
        needed = target_unique - len(posts)
        topup_limit = min(MAX_LISTING_LIMIT, max(int(needed *
            TOPUP_OVERSAMPLE_FACTOR), needed))
        print(
            f'  Top-up from new() to reach target ({needed} more needed, limit={topup_limit})...'
            )
        added = 0
        try:
            async for submission in sr.new(limit=topup_limit):
                sid = getattr(submission, 'id', None)
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    posts.append(submission)
                    added += 1
                if len(posts) >= target_unique:
                    break
        except Exception as e:
            logger.warning(f'Error in new() top-up: {e}')
        print(f'    new top-up added {added} posts')
        if include_hot and len(posts) < target_unique:
            needed = target_unique - len(posts)
            hot_topup_limit = min(MAX_LISTING_LIMIT, max(int(needed *
                TOPUP_OVERSAMPLE_FACTOR), 200))
            print(
                f'  Secondary top-up from hot() ({needed} more needed, limit={hot_topup_limit})...'
                )
            added = 0
            try:
                async for submission in sr.hot(limit=hot_topup_limit):
                    sid = getattr(submission, 'id', None)
                    if sid and sid not in seen_ids:
                        seen_ids.add(sid)
                        posts.append(submission)
                        added += 1
                    if len(posts) >= target_unique:
                        break
            except Exception as e:
                logger.warning(f'Error in hot() secondary top-up: {e}')
            print(f'    hot top-up added {added} posts')
        if len(posts) < target_unique:
            missing = target_unique - len(posts)
            print(
                f'  Still short by {missing} posts; doing deep new() sweep...')
            try:
                async for submission in sr.new(limit=None):
                    sid = getattr(submission, 'id', None)
                    if not sid or sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    posts.append(submission)
                    if len(posts) >= target_unique:
                        break
            except Exception as e:
                logger.warning(f'Error in deep new() sweep: {e}')
        print(
            f'  Total unique posts collected: {len(posts)} (target={target_unique})'
            )
        return posts


async def safe_collect_one(reddit: asyncpraw.Reddit, submission, sr_meta:
    Dict[str, Any]) ->Optional[Dict[str, Any]]:

    async def _collect():
        post_id = getattr(submission, 'id', None)
        try:
            await submission.load()
        except Exception as e:
            logger.warning(f'Could not fully load submission {post_id}: {e}')
        created = getattr(submission, 'created_utc', None)
        title = getattr(submission, 'title', '')
        body = getattr(submission, 'selftext', '')
        if getattr(submission, 'is_video', False):
            body = re.sub('\\{.*?\\}', '', body, flags=re.DOTALL).strip()
        if 'fallback_url' in body or 'transcoding_status' in body:
            body = re.sub('\\{.*?\\}', '', body, flags=re.DOTALL).strip()
        author_obj = getattr(submission, 'author', None)
        author_name = getattr(author_obj, 'name', '[deleted]'
            ) if author_obj else '[deleted]'
        title_feats = analyze_title_features(title)
        body_feats = analyze_body_features(body)
        domain_info = extract_domain_info(submission)
        engagement = analyze_engagement(submission)
        user_info = {}
        if author_name not in ['[deleted]', 'AutoModerator']:
            try:
                user_info = await fetch_user_info(reddit, author_name)
            except Exception as e:
                logger.warning(f'User info fetch failed for {author_name}: {e}'
                    )
                user_info = {}
        row = {'post_id': post_id, 'post_fullname': f't3_{post_id}',
            'subreddit': submission.subreddit.display_name, 'created_utc':
            created, 'author_username': author_name, 'title': title, 'body':
            body, 'post_type': get_post_type(submission), 'is_stickied':
            getattr(submission, 'stickied', False), 'is_locked': getattr(
            submission, 'locked', False), 'link_flair_text': getattr(
            submission, 'link_flair_text', None), 'author_flair_text':
            getattr(submission, 'author_flair_text', None), **title_feats,
            **body_feats, **domain_info, **engagement, **user_info, **
            sr_meta, 'snapshot_timestamp': time.time()}
        await asyncio.sleep(GENTLE_DELAY_SEC)
        return row
    try:
        return await asyncio.wait_for(_collect(), timeout=PER_ITEM_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error(
            f"Timeout while collecting post {getattr(submission, 'id', 'UNKNOWN')}"
            )
        return None
    except Exception as e:
        logger.error(
            f"Error collecting post {getattr(submission, 'id', 'UNKNOWN')}: {e}"
            , exc_info=True)
        return None


SUBREDDIT_PATTERN = '^[A-Za-z0-9_]{3,21}$'


async def collect_subreddit(reddit: asyncpraw.Reddit, subreddit_name: str,
    target_posts: int=TARGET_POSTS_PER_SUB, sampling_method: str='multi_sort'
    ) ->Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    if not re.match(SUBREDDIT_PATTERN, subreddit_name):
        print(f'  ❌ Invalid subreddit name format: r/{subreddit_name}')
        return pd.DataFrame(), [], {'attempted': 0, 'succeeded': 0,
            'failed': 0, 'error':
            'Invalid name format (sanitization failure)', 'sampling_method':
            sampling_method, 'skipped': True}
    print(
        f"""
📊 [PASS 1] Collecting from r/{subreddit_name} (target={target_posts}, method={sampling_method})..."""
        )
    try:
        sr = await reddit.subreddit(subreddit_name, fetch=True)
        sr_meta = await fetch_subreddit_metadata(sr)
    except (NotFound, Forbidden) as e:
        print(f'  ❌ Cannot access r/{subreddit_name}: {type(e).__name__}')
        return pd.DataFrame(), [], {'attempted': 0, 'succeeded': 0,
            'failed': 0, 'error':
            f'Subreddit inaccessible: {type(e).__name__}',
            'sampling_method': sampling_method, 'skipped': True}
    posts = []
    try:
        if sampling_method == 'multi_sort':
            posts = await collect_multi_sort_posts(reddit, subreddit_name,
                target_unique=target_posts, include_hot=True)
        else:
            async for submission in sr.new(limit=target_posts):
                posts.append(submission)
    except Exception as e:
        print(f'  ⚠️  Error fetching posts: {e}')
        if not posts:
            return pd.DataFrame(), [], {'attempted': 0, 'succeeded': 0,
                'failed': 0, 'error': str(e), 'sampling_method':
                sampling_method, 'skipped': True, **sr_meta}
    actual_posts = len(posts)
    print(f'  Retrieved {actual_posts} unique posts', end='')
    if actual_posts < MIN_ACCEPTABLE_POSTS:
        print(f' - SKIPPING (below minimum {MIN_ACCEPTABLE_POSTS})')
        return pd.DataFrame(), [], {'attempted': actual_posts, 'succeeded':
            0, 'failed': 0, 'skipped': True, 'reason':
            f'Only {actual_posts} posts available (min: {MIN_ACCEPTABLE_POSTS})'
            , 'sampling_method': sampling_method, **sr_meta}
    if actual_posts < target_posts:
        print(f' (target was {target_posts})')
    else:
        print()
    print(f'  Processing async with concurrency={MAX_CONCURRENCY}...')
    stats = {'attempted': actual_posts, 'succeeded': 0, 'failed': 0,
        'failed_ids': [], 'skipped': False, 'posts_requested': target_posts,
        'posts_available': actual_posts, 'sampling_method': sampling_method}
    results: List[Optional[Dict[str, Any]]] = []
    tasks = [safe_collect_one(reddit, s, sr_meta) for s in posts]
    processed = 0

    async def _bounded_collect(task):
        async with sem:
            return await task
    bounded_tasks = [_bounded_collect(t) for t in tasks]
    for future in asyncio.as_completed(bounded_tasks):
        try:
            result = await future
            if result is None:
                stats['failed'] += 1
            else:
                stats['succeeded'] += 1
                results.append(result)
        except Exception as e:
            stats['failed'] += 1
            print(f'  ⚠️ Task error: {e}')
        processed += 1
        if processed % 50 == 0:
            percentage = processed / len(tasks) * 100
            print(f'  Progress: {processed}/{len(tasks)} ({percentage:.1f}%)')
    rows = [r for r in results if r is not None]
    df = pd.DataFrame(rows)
    post_fullnames = [r['post_fullname'] for r in rows if r and r.get(
        'post_fullname')]
    if not df.empty:
        if 'author_total_karma' in df.columns:
            df['author_total_karma'] = pd.to_numeric(df[
                'author_total_karma'], errors='coerce')
        if 'author_account_age_days' in df.columns:
            df['author_account_age_days'] = pd.to_numeric(df[
                'author_account_age_days'], errors='coerce')
        for col in ('author_is_verified', 'author_is_gold'):
            if col in df.columns:
                df[col] = df[col].fillna(False).infer_objects(copy=False
                    ).astype(bool)
        if 'domain' in df.columns:
            df['domain'] = df['domain'].astype('object').fillna('')
        if 'url' in df.columns:
            df['url'] = df['url'].astype('object').fillna('')
        if 'initial_upvote_ratio' in df.columns:
            df['initial_upvote_ratio'] = pd.to_numeric(df[
                'initial_upvote_ratio'], errors='coerce').fillna(0.0)
    print(f'✓ Collected {len(df)} rows from r/{subreddit_name}')
    if stats['failed'] > 0:
        print(
            f"  ⚠️  {stats['failed']}/{stats['attempted']} posts failed to process"
            )
    return df, post_fullnames, stats


def estimate_runtime(num_subreddits: int, posts_per_sub: int) ->Dict[str, Any]:
    requests_per_post = 4
    total_posts = num_subreddits * posts_per_sub
    total_requests = total_posts * requests_per_post
    requests_per_second = 1.0
    effective_posts_per_second = requests_per_second / requests_per_post
    overhead_factor = 1.5
    estimated_seconds = (total_posts / effective_posts_per_second *
        overhead_factor)
    estimated_hours = estimated_seconds / 3600
    return {'total_subreddits': num_subreddits, 'posts_per_subreddit':
        posts_per_sub, 'total_posts_target': total_posts,
        'estimated_api_requests': total_requests, 'estimated_hours': round(
        estimated_hours, 2), 'estimated_time_str': str(timedelta(seconds=
        int(estimated_seconds))), 'note':
        'This is a conservative estimate. Actual time may vary based on network conditions and API availability.'
        }


async def main():
    load_dotenv()
    print('=' * 70)
    print('🚀 REDDIT DATA COLLECTION - SEQUENTIAL MODE')
    print('=' * 70)
    os.makedirs(PASS1_DIR, exist_ok=True)
    os.makedirs(COMBINED_DIR, exist_ok=True)
    start_time = time.time()
    if not os.path.exists(SUBREDDIT_LIST_FILE):
        print(f'\n❌ Error: Subreddit list not found at {SUBREDDIT_LIST_FILE}')
        return
    with open(SUBREDDIT_LIST_FILE, 'r') as f:
        subreddits = json.load(f)
    global user_info_cache
    user_info_cache = load_user_cache()
    completed_subs, prev_stats = load_progress()
    completed_subs = set(completed_subs)
    completed_subs, prev_stats = load_progress()
    completed_subs = set(completed_subs)
    queue_subs = [s for s in subreddits if s not in completed_subs]
    print(
        f'✓ Found {len(completed_subs)} completed. Queueing {len(queue_subs)} remaining.'
        )
    if not queue_subs:
        print('All subreddits collected!')
        return
    tracker = ProgressTracker(len(subreddits))
    tracker.completed_subreddits = len(completed_subs)
    tracker.total_posts_collected = sum(s.get('succeeded', 0) for s in
        prev_stats.values())
    tracker.total_posts_attempted = sum(s.get('attempted', s.get(
        'posts_requested', TARGET_POSTS_PER_SUB)) for s in prev_stats.values())
    cohort_ids: Dict[str, List[str]] = {}
    collection_stats: Dict[str, Dict[str, Any]] = prev_stats.copy()
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    async with aiohttp.ClientSession(connector=connector) as session:
        reddit = asyncpraw.Reddit(client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'), username=os.
            getenv('REDDIT_USERNAME'), password=os.getenv('REDDIT_PASSWORD'
            ), user_agent=
            f"research_bot v3.0 by /u/{os.getenv('REDDIT_USERNAME', 'unknown')}"
            , requestor_kwargs={'session': session}, timeout=30)
        print(f'\n✓ Connected to Reddit as: {await reddit.user.me()}')
        for i, sub in enumerate(queue_subs):
            try:
                limits = reddit.auth.limits
                remaining = float(limits.get('remaining', 600))
                if remaining < RATE_LIMIT_CRITICAL_THRESHOLD:
                    reset_seconds = max(1, limits.get('reset_timestamp',
                        time.time()) - time.time())
                    print(
                        f'🛑 Rate limit low ({remaining}). Sleeping {reset_seconds:.1f}s...'
                        )
                    await asyncio.sleep(reset_seconds + 2)
            except Exception:
                pass
            sub_start = time.time()
            df, fullnames, stats = await collect_subreddit(reddit, sub,
                target_posts=TARGET_POSTS_PER_SUB, sampling_method=
                SAMPLING_METHOD)
            sub_end = time.time()
            duration = round(sub_end - sub_start, 2)
            stats['duration_sec'] = duration
            collection_stats[sub] = stats
            if not df.empty:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M')
                fname = os.path.join(PASS1_DIR, f'{sub}_{timestamp}.csv')
                df.to_csv(fname, index=False)
                print(f'  💾 Saved {len(df)} rows to {fname}')
                cohort_ids[sub] = list(set(fullnames))
                tracker.update(stats['succeeded'], stats['attempted'])
            else:
                tracker.update(0, stats.get('attempted', 0))
            print(f'  ⏱️  r/{sub} finished in {duration}s')
            completed_subs.add(sub)
            save_progress(list(completed_subs), collection_stats)
            if (i + 1) % 10 == 0:
                await save_user_cache(user_info_cache)
            tracker.print_status()
            del df
            del fullnames
    await save_user_cache(user_info_cache)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    with open(os.path.join(COHORT_IDS_DIR,
        f'cohort_ids_final_{timestamp}.json'), 'w') as f:
        json.dump(cohort_ids, f, indent=2)
    total_duration = time.time() - start_time
    print(
        f'\n⏱️  Total Run Time: {str(timedelta(seconds=int(total_duration)))}')
    print('\n' + '=' * 70)
    print('✅ COLLECTION COMPLETE')
    print('=' * 70)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n\n👋 Shutting down gracefully on Keyboard Interrupt...')
        pass
