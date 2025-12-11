import os
import re
import sys
import json
import glob
import asyncio
from datetime import datetime, timezone
import random
import pandas as pd
from dotenv import load_dotenv
import asyncpraw
from asyncprawcore.exceptions import NotFound, Forbidden, ServerError, TooManyRequests
import logging
import argparse
BATCH_SIZE = 100
BATCH_DELAY = 3.5
COMMENT_DELAY = 2.3
MAX_CONCURRENCY = 1
FETCH_MOD_COMMENTS = True
MAX_RETRIES = 3
INITIAL_BACKOFF = 60
load_dotenv()


def extract_rule_mentions(text):
    if not text:
        return None
    word_to_digit = {'one': '1', 'two': '2', 'three': '3', 'four': '4',
        'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9',
        'ten': '10', 'first': '1', 'second': '2', 'third': '3'}
    patterns = ['(?:rule|violation)\\s*#?(\\d+)', 'R(\\d+)',
        'rule\\s+([a-zA-Z]+)', 'violates?\\s+(\\d+)', 'broke\\s+rule\\s*(\\d+)'
        ]
    raw_matches = []
    for p in patterns:
        raw_matches.extend(re.findall(p, text, re.IGNORECASE))
    final_rules = []
    for m in raw_matches:
        m_clean = m.lower().strip()
        if m_clean.isdigit():
            final_rules.append(m_clean)
        elif m_clean in word_to_digit:
            final_rules.append(word_to_digit[m_clean])
    return sorted(list(set(final_rules))) if final_rules else None


def _latest_cohort_json():
    id_files = glob.glob('data/cohort_ids_*.json')
    return max(id_files, key=os.path.getctime) if id_files else None


def _latest_pass1_csv(subreddit):
    files = glob.glob(f'data/pass1/{subreddit}_*.csv')
    return max(files, key=os.path.getctime) if files else None


def _latest_pass2_complete_csv(subreddit):
    files = glob.glob(f'data/pass2_complete/{subreddit}_complete_*.csv')
    return max(files, key=os.path.getctime) if files else None


log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s [%(levelname)s] %(message)s', handlers=[logging.
    FileHandler(
    f"{log_dir}/pass2_{datetime.now().strftime('%Y%m%d_%H%M')}.log"),
    logging.StreamHandler()])
logger = logging.getLogger(__name__)


async def retry_with_backoff(coro_func, *args, max_retries=MAX_RETRIES, **
    kwargs):
    for attempt in range(max_retries):
        try:
            return await coro_func(*args, **kwargs)
        except TooManyRequests as e:
            if attempt == max_retries - 1:
                print(f'    ❌ Rate limit exceeded after {max_retries} attempts'
                    )
                raise
            wait_time = INITIAL_BACKOFF * 2 ** attempt + random.uniform(0, 10)
            print(
                f'    ⚠️  Rate limit hit. Waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}'
                )
            await asyncio.sleep(wait_time)
        except ServerError as e:
            if attempt == max_retries - 1:
                print(f'    ❌ Server error after {max_retries} attempts')
                raise
            wait_time = 5 * 2 ** attempt
            print(f'    ⚠️  Server error. Retrying in {wait_time}s...')
            await asyncio.sleep(wait_time)
        except (asyncio.TimeoutError, ConnectionError) as e:
            if attempt == max_retries - 1:
                print(f'    ❌ Connection failed after {max_retries} attempts')
                raise
            wait_time = 3 * 2 ** attempt
            print(f'    ⚠️  Connection error. Retrying in {wait_time}s...')
            await asyncio.sleep(wait_time)
        except Exception as e:
            print(f'    ❌ Unexpected error: {type(e).__name__}: {e}')
            raise
    return None


async def check_rate_limit(reddit):
    try:
        limits = reddit.auth.limits
        remaining = float(limits.get('remaining', 600))
        if remaining < 50:
            print(f'    🛑 Rate limit low ({remaining}). Pausing for 30s...')
            await asyncio.sleep(30)
    except Exception:
        pass


async def fetch_status_async(reddit, post_fullnames):
    post_fullnames = list(dict.fromkeys(post_fullnames))
    logger.info(f'Fetching final status for {len(post_fullnames)} posts')
    print(f'  After deduplication: {len(post_fullnames)} unique posts.')
    status_map = {}
    failed_batches = []
    found_count = 0

    async def _fetch_and_process_batch(batch):
        batch_map = {}
        batch_found = 0
        async for post in reddit.info(fullnames=batch):
            body = getattr(post, 'selftext', None)
            score = post.score
            num_comments = post.num_comments
            is_removed_official = post.removed_by_category is not None
            is_removed_content = (body == '[removed]' or post.title ==
                '[removed]')
            is_deleted_content = body == '[deleted]'
            author_username = str(post.author) if post.author else '[deleted]'
            is_deleted_author = author_username == '[deleted]'
            is_deleted = is_deleted_author or is_deleted_content
            is_removed_inferred = (is_removed_official or
                is_removed_content or hasattr(post, 'is_robot_indexable') and
                post.is_robot_indexable is False)
            batch_map[post.fullname] = {'selftext': body, 'final_score':
                post.score, 'final_num_comments': post.num_comments,
                'final_upvote_ratio': post.upvote_ratio,
                'final_link_flair_text': post.link_flair_text,
                'final_link_flair_css': post.link_flair_css_class,
                'is_removed_official': is_removed_official,
                'is_removed_content': is_removed_content,
                'is_removed_inferred': is_removed_inferred,
                'removed_by_category': post.removed_by_category,
                'is_deleted': is_deleted, 'author_username':
                author_username, 'is_deleted_author': is_deleted_author,
                'is_deleted_content': is_deleted_content, 'is_locked': post
                .locked, 'is_stickied': post.stickied, 'is_archived': post.
                archived, 'rechecked_at_utc': datetime.now(timezone.utc),
                'removal_reason': getattr(post, 'removal_reason', None),
                'mod_note': getattr(post, 'mod_note', None), 'banned_by':
                getattr(post, 'banned_by', None),
                'author_is_currently_active': post.author is not None,
                'author_name_final': str(post.author) if post.author else
                '[deleted]', 'low_comment_engagement': post.num_comments < 
                post.score * 0.1 if post.score > 0 else False,
                'comment_to_score_ratio': round(post.num_comments / post.
                score, 3) if post.score > 0 else 0.0}
            batch_found += 1
        return batch_map, batch_found
    for i in range(0, len(post_fullnames), BATCH_SIZE):
        await check_rate_limit(reddit)
        batch_fullnames = post_fullnames[i:i + BATCH_SIZE]
        try:
            batch_map, batch_found_count = await retry_with_backoff(
                _fetch_and_process_batch, batch_fullnames)
            status_map.update(batch_map)
            found_count += batch_found_count
            logger.debug(
                f'Batch {i // BATCH_SIZE}: Found {batch_found_count}/{len(batch_fullnames)} posts'
                )
        except Exception as e:
            logger.error(
                f'Batch {i}-{i + BATCH_SIZE} failed after retries: {type(e).__name__}: {e}'
                )
            failed_batches.append({'batch_start': i, 'batch_end': i +
                BATCH_SIZE, 'fullnames': batch_fullnames, 'error': str(e)})
        await asyncio.sleep(BATCH_DELAY)
    logger.info(
        f'Found {found_count}/{len(post_fullnames)} posts ({found_count / len(post_fullnames) * 100:.1f}%)'
        )
    return status_map, failed_batches


async def fetch_one_mod_comment(reddit, post_id):
    default_data = {'mod_comment_text': None, 'mod_comment_author': None,
        'mod_comment_is_distinguished': False, 'mod_comment_is_stickied': 
        False, 'mod_comment_rule_mentions': None, 'mod_comment_rule_count':
        0, 'thread_deleted_comments': 0, 'thread_total_scanned_comments': 0,
        'thread_deletion_ratio': None, 'thread_graveyard_flag': False}
    MIN_COMMENTS_FOR_RATIO = 5
    MAX_COMMENTS_SCANNED = 30
    GRAVEYARD_RATIO_THRESHOLD = 0.5

    def is_deleted_comment(c) ->bool:
        if c is None:
            return True
        if getattr(c, 'author', None) is None:
            return True
        text = (getattr(c, 'body', '') or '').strip().lower()
        if not text:
            return False
        if text in ('[deleted]', '[removed]'):
            return True
        if text.startswith('[deleted by') or text.startswith('[removed by'):
            return True
        return False

    async def _fetch():
        submission = await reddit.submission(id=post_id)
        await submission.load()
        await submission.comments.replace_more(limit=0)
        all_comments = submission.comments
        top_comments = all_comments[:MAX_COMMENTS_SCANNED]
        if not top_comments:
            return default_data
        deleted_count = sum(1 for c in top_comments if is_deleted_comment(c))
        total_scanned = len(top_comments)
        if total_scanned >= MIN_COMMENTS_FOR_RATIO:
            deletion_ratio = round(deleted_count / total_scanned, 2)
        else:
            deletion_ratio = None
        found_mod_data = default_data.copy()
        found_mod_data.update({'thread_deleted_comments': deleted_count,
            'thread_total_scanned_comments': total_scanned,
            'thread_deletion_ratio': deletion_ratio,
            'thread_graveyard_flag': deletion_ratio is not None and 
            deletion_ratio >= GRAVEYARD_RATIO_THRESHOLD})
        for c in top_comments:
            try:
                is_mod = getattr(c, 'distinguished', None) == 'moderator'
                is_stickied = bool(c.stickied)
                author_name = str(c.author) if c.author else ''
                bot_names = {'automoderator', 'modbot', 'botdefense',
                    'reddit', 'safestbot', 'moderationbot', 'anti-evil',
                    'reddit-request-bot'}
                is_bot = author_name.lower() in bot_names
                if is_mod or is_stickied or is_bot:
                    text = getattr(c, 'body', '') or ''
                    found_mod_data.update({'mod_comment_text': text,
                        'mod_comment_author': author_name,
                        'mod_comment_is_distinguished': bool(is_mod),
                        'mod_comment_is_stickied': is_stickied,
                        'mod_comment_rule_mentions': ','.join(
                        extract_rule_mentions(text) or []),
                        'mod_comment_rule_count': len(extract_rule_mentions
                        (text) or []), 'mod_comment_created_utc': c.
                        created_utc})
                    return found_mod_data
            except Exception:
                continue
        return found_mod_data
    try:
        data = await asyncio.wait_for(retry_with_backoff(_fetch), timeout=60)
        return {f't3_{post_id}': data}
    except asyncio.TimeoutError:
        logger.warning(f'Timeout fetching comments for {post_id}')
        return {f't3_{post_id}': default_data}
    except (NotFound, Forbidden):
        return {f't3_{post_id}': default_data}
    except Exception as e:
        logger.error(f'Failed to fetch mod comments for {post_id}: {e}')
        return {f't3_{post_id}': default_data}


async def fetch_mod_comments_async(reddit, target_ids):
    print(
        f'\nFetching mod comments for {len(target_ids)} removed/locked/stickied posts…'
        )
    if not target_ids:
        return {}
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def worker(pid):
        async with sem:
            result = await fetch_one_mod_comment(reddit, pid)
            await asyncio.sleep(COMMENT_DELAY)
            return result
    tasks = [asyncio.create_task(worker(pid)) for pid in target_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    ok = 0
    for r in results:
        if isinstance(r, dict):
            out.update(r)
            ok += 1
    print(f'  ✓ Checked {ok} posts for mod comments')
    return out


async def main(args):
    reddit = asyncpraw.Reddit(client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'), username=os.getenv
        ('REDDIT_USERNAME'), password=os.getenv('REDDIT_PASSWORD'),
        user_agent='moderation_research_bot v2.0 (async) by /u/' + (os.
        getenv('REDDIT_USERNAME') or 'unknown_user'), timeout=30)
    try:
        me = await reddit.user.me()
        print(f'✓ Connected to Reddit as: {me}')
    except Exception as e:
        print(f'❌ Could not verify Reddit identity: {e}')
        await reddit.close()
        sys.exit(1)
    if args.cohort:
        cohort_file = args.cohort
        if not os.path.exists(cohort_file):
            print(f'❌ Specified cohort file not found: {cohort_file}')
            await reddit.close()
            sys.exit(1)
    else:
        cohort_file = _latest_cohort_json()
        if not cohort_file:
            print(
                "❌ No 'cohort_ids_*.json' file found in 'data/'. Run Pass 1 first."
                )
            await reddit.close()
            sys.exit(1)
    print(f'📂 Loading cohort from: {cohort_file}')
    with open(cohort_file, 'r') as f:
        cohort_ids_by_sub = json.load(f)
    print(f'Found {len(cohort_ids_by_sub)} subreddit(s) to re-check.')
    os.makedirs('data/pass2_complete', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    all_final = []
    pass2_summary = {}
    for subreddit, post_fullnames in cohort_ids_by_sub.items():
        print('\n' + '=' * 60)
        logger.info(f'Processing r/{subreddit}')
        print('=' * 60)
        if not post_fullnames:
            print(f'  SKIPPING: No post IDs found for r/{subreddit}.')
            continue
        base_path = _latest_pass2_complete_csv(subreddit)
        if base_path:
            print(f'  Resuming from last pass2 file: {base_path}')
            base_df = pd.read_csv(base_path)
        else:
            p1 = _latest_pass1_csv(subreddit)
            if not p1:
                print(f'  SKIPPING: No Pass 1 CSV for r/{subreddit}.')
                continue
            print(f'  Loading Pass 1 base: {p1}')
            base_df = pd.read_csv(p1)
        if 'post_fullname' not in base_df.columns:
            print(
                f"❌ Base file for r/{subreddit} missing 'post_fullname'. Re-run Pass 1."
                )
            continue
        t12_files = glob.glob(f'data/pass1_5/{subreddit}_t12_*.json')
        t12_data = {}
        if t12_files:
            print(f'  Found {len(t12_files)} T+12h file(s). Merging...')
            for fpath in sorted(t12_files, key=os.path.getctime):
                try:
                    with open(fpath, 'r') as f:
                        partial_data = json.load(f)
                        t12_data.update(partial_data)
                except Exception as e:
                    print(f'    ⚠️ Error loading {fpath}: {e}')
            print(f'  ✓ Loaded T+12h data for {len(t12_data)} posts.')
        if t12_data:
            t12_df = pd.DataFrame.from_dict(t12_data, orient='index')
            t12_df.index.name = 'post_fullname'
            base_df = base_df.set_index('post_fullname')
            for col in t12_df.columns:
                if col not in base_df.columns:
                    base_df[col] = pd.NA
            base_df.update(t12_df)
            base_df = base_df.reset_index()
        print('  PASS 2A: Fetching final status…')
        info_map, failed_batches = await fetch_status_async(reddit,
            post_fullnames)
        n_rem = sum(1 for p in info_map.values() if p.get(
            'is_removed_inferred'))
        n_lock = sum(1 for p in info_map.values() if p.get('is_locked'))
        n_stick = sum(1 for p in info_map.values() if p.get('is_stickied'))
        print(
            f'  📊 Status: {n_rem} Removed | {n_lock} Locked | {n_stick} Stickied'
            )
        expected = set(post_fullnames)
        found = set(info_map.keys())
        missing = expected - found
        validation = {'subreddit': subreddit, 'expected_count': len(
            expected), 'found_count': len(found), 'missing_count': len(
            missing), 'coverage_rate': len(found) / len(expected) * 100 if
            expected else 0, 'failed_batches': len(failed_batches)}
        if missing:
            logger.warning(f'  Missing {len(missing)} posts from r/{subreddit}'
                )
            validation['missing_sample'] = list(missing)[:10]
            for fullname in missing:
                info_map[fullname] = {'final_score': None,
                    'final_num_comments': None, 'final_upvote_ratio': None,
                    'is_removed_official': None, 'is_removed_content': None,
                    'is_removed_inferred': None, 'removed_by_category':
                    None, 'is_deleted': None, 'is_deleted_author': None,
                    'is_deleted_content': None, 'is_locked': None,
                    'is_archived': None, 'rechecked_at_utc': datetime.now(
                    timezone.utc), 'removal_reason': None, 'mod_note': None,
                    'banned_by': None, 'pass2_not_found': True}
        else:
            print(f'  ✓ All {len(expected)} posts found')
        val_file = (
            f"data/pass2_complete/{subreddit}_validation_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            )
        with open(val_file, 'w') as f:
            json.dump(validation, f, indent=2)
        pass2_summary[subreddit] = validation
        status_df = pd.DataFrame.from_dict(info_map, orient='index')
        status_df.index.name = 'post_fullname'
        if 'rechecked_at_utc' in status_df.columns:
            status_df['rechecked_at_utc'] = status_df['rechecked_at_utc'
                ].astype(str)
        string_cols = ['removed_by_category', 'removal_reason', 'mod_note',
            'banned_by']
        for col in string_cols:
            if col in status_df.columns:
                status_df[col] = status_df[col].astype('object')
        bool_cols = ['is_removed_official', 'is_removed_content',
            'is_removed_inferred', 'is_deleted', 'is_deleted_author',
            'is_deleted_content', 'is_locked', 'is_stickied', 'is_archived',
            'pass2_not_found', 'exists']
        for col in bool_cols:
            if col in status_df.columns:
                status_df[col] = status_df[col].fillna(False).astype('bool')
        numeric_cols = ['final_score', 'final_num_comments',
            'final_upvote_ratio']
        for col in numeric_cols:
            if col in status_df.columns:
                status_df[col] = pd.to_numeric(status_df[col], errors='coerce')
        base = base_df.set_index('post_fullname')
        new = status_df
        for col in new.columns:
            if col not in base.columns:
                base[col] = pd.Series(dtype=new[col].dtype)
        base.update(new)
        final_df = base.reset_index()
        if FETCH_MOD_COMMENTS:
            cols_to_fix = ['is_removed_inferred', 'is_locked', 'is_stickied']
            for c in cols_to_fix:
                if c not in final_df.columns:
                    final_df[c] = False
                final_df[c] = final_df[c].infer_objects(copy=False).fillna(
                    False)
            if 'final_num_comments' not in final_df.columns:
                final_df['final_num_comments'] = 0
            final_df['final_num_comments'] = final_df['final_num_comments'
                ].fillna(0).infer_objects(copy=False)
            final_df['is_removed_and_locked'] = final_df['is_removed_inferred'
                ].astype(bool) & final_df['is_locked'].astype(bool)
            final_df['is_removed_and_unlocked'] = final_df[
                'is_removed_inferred'].astype(bool) & ~final_df['is_locked'
                ].astype(bool)
            locked_posts = final_df[(final_df['is_locked'] == True) & (
                final_df['final_num_comments'] > 0)]
            non_removed = final_df[(final_df['is_removed_inferred'] == 
                False) & (final_df['final_num_comments'] >= 5)]
            non_removed = non_removed.copy()
            scores = non_removed['final_score'].fillna(0)
            non_removed['score_band'] = pd.cut(scores, bins=[-1000000000.0,
                0, 5, 50, 500, 1000000000000.0], labels=['<=0', '1-5',
                '6-50', '51-500', '500+'])
            non_removed['engagement_band'] = pd.cut(non_removed[
                'final_num_comments'], bins=[0, 10, 50, 150, 10000], labels
                =['low', 'medium', 'high', 'very_high'], right=True)
            fraction = 0.2
            max_per_sub = 50
            min_per_sub = 10
            strata = []
            for _, group in non_removed.groupby(['score_band',
                'engagement_band']):
                if len(group) == 0:
                    continue
                band_target = max(1, int(len(group) * fraction))
                strata.append(group.sample(n=min(band_target, len(group)),
                    random_state=42))
            if strata:
                sampled_non_removed = pd.concat(strata, ignore_index=False)
            else:
                sampled_non_removed = non_removed
            if len(sampled_non_removed) > max_per_sub:
                sampled_non_removed = sampled_non_removed.sample(n=
                    max_per_sub, random_state=42)
            elif len(sampled_non_removed) < min_per_sub and len(non_removed
                ) >= min_per_sub:
                remaining = non_removed.drop(sampled_non_removed.index,
                    errors='ignore')
                needed = min(min_per_sub - len(sampled_non_removed), len(
                    remaining))
                if needed > 0:
                    sampled_non_removed = pd.concat([sampled_non_removed,
                        remaining.sample(n=needed, random_state=42)])
            removed_unlocked = final_df[(final_df['is_removed_and_unlocked'
                ] == True) & (final_df['final_num_comments'] > 0)]
            MAX_REMOVED_FOR_MODSCAN = 50
            if len(removed_unlocked) > MAX_REMOVED_FOR_MODSCAN:
                removed_unlocked = removed_unlocked.sample(n=
                    MAX_REMOVED_FOR_MODSCAN, random_state=42)
            graveyard_targets = pd.concat([locked_posts,
                sampled_non_removed, removed_unlocked], ignore_index=False
                ).drop_duplicates(subset=['post_fullname'])
            mod_df = None
            if len(graveyard_targets) > 0:
                print(
                    f'  PASS 2B: Fetching comments for {len(graveyard_targets)} posts (locked + sampled non-removed + removed-unlocked)…'
                    )
                target_ids = [(fn.replace('t3_', '') if fn.startswith('t3_'
                    ) else fn) for fn in graveyard_targets['post_fullname']
                    .astype(str)]
                mod_map = await fetch_mod_comments_async(reddit, target_ids)
                if mod_map:
                    mod_df = pd.DataFrame.from_dict(mod_map, orient='index')
                    mod_df.index.name = 'post_fullname'
                else:
                    print('  (PASS 2B) No mod comments returned.')
            else:
                print(
                    '  No locked/non-removed posts selected for graveyard check — skipping mod comment collection.'
                    )
            mod_cols = ['mod_comment_text', 'mod_comment_author',
                'mod_comment_is_distinguished', 'mod_comment_is_stickied',
                'mod_comment_rule_mentions', 'mod_comment_rule_count',
                'thread_deletion_ratio', 'mod_comment_created_utc']
            base = final_df.set_index('post_fullname')
            cols_to_drop = [c for c in mod_cols if c in base.columns]
            if cols_to_drop:
                base = base.drop(columns=cols_to_drop)
            if mod_df is not None:
                for c in mod_cols:
                    if c not in mod_df.columns:
                        mod_df[c] = pd.NA
                final_df = base.join(mod_df[mod_cols], how='left')
            else:
                final_df = base
                for c in mod_cols:
                    if c not in final_df.columns:
                        final_df[c] = pd.NA
            final_df = final_df.reset_index()
            final_df['mod_comment_is_distinguished'] = final_df[
                'mod_comment_is_distinguished'].astype('boolean').fillna(False)
            final_df['mod_comment_is_stickied'] = final_df[
                'mod_comment_is_stickied'].astype('boolean').fillna(False)
        desired_order = ['post_fullname', 'subreddit', 'title', 'selftext',
            'author_username', 'author_name_final',
            'author_is_currently_active', 'created_utc', 'final_score',
            'final_upvote_ratio', 'final_num_comments',
            'final_link_flair_text', 'final_link_flair_css',
            'is_removed_inferred', 'is_removed_official',
            'is_removed_content', 'removed_by_category',
            'is_removed_and_locked', 'is_removed_and_unlocked',
            'is_deleted', 'is_deleted_author', 'is_deleted_content',
            'is_locked', 'is_stickied', 'is_archived', 'rechecked_at_utc',
            'pass2_not_found', 'mod_comment_text', 'mod_comment_author',
            'mod_comment_is_distinguished', 'mod_comment_is_stickied',
            'mod_comment_rule_mentions', 'mod_comment_rule_count',
            'mod_comment_created_utc', 'thread_total_scanned_comments',
            'thread_deleted_comments', 'thread_deletion_ratio',
            'low_comment_engagement', 'comment_to_score_ratio']
        safe_order = [c for c in desired_order if c in final_df.columns]
        remaining_cols = [c for c in final_df.columns if c not in safe_order]
        final_df = final_df[safe_order + remaining_cols]
        out_path = (
            f"data/pass2_complete/{subreddit}_complete_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            )
        final_df.to_csv(out_path, index=False)
        print(f'  💾 Saved: {out_path}')
        all_final.append(final_df)
        await asyncio.sleep(0.5)
    if pass2_summary:
        print('\n' + '=' * 60)
        print('PASS 2 VALIDATION SUMMARY')
        print('=' * 60)
        total_expected = sum(s['expected_count'] for s in pass2_summary.
            values())
        total_found = sum(s['found_count'] for s in pass2_summary.values())
        total_missing = sum(s['missing_count'] for s in pass2_summary.values())
        print(f'Total posts in cohort: {total_expected:,}')
        print(
            f'Found in Pass 2: {total_found:,} ({total_found / total_expected * 100:.1f}%)'
            )
        print(
            f'Missing/deleted: {total_missing:,} ({total_missing / total_expected * 100:.1f}%)'
            )
        print('\nPer subreddit:')
        for sub, stats in pass2_summary.items():
            print(
                f"  r/{sub}: {stats['coverage_rate']:.1f}% coverage ({stats['found_count']}/{stats['expected_count']})"
                )
        print('=' * 60)
    else:
        print('\n❌ No data was processed.')
    await reddit.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
        'Pass 2: Final status + mod comment / graveyard collection')
    parser.add_argument('--cohort', type=str, help=
        'Path to a specific cohort_ids_*.json file (e.g., from rolling_analysis_helper.py). If omitted, uses the latest data/cohort_ids_*.json.'
        )
    args = parser.parse_args()
    asyncio.run(main(args))
