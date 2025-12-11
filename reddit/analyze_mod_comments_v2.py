import pandas as pd
import numpy as np
import re
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob
import psutil
import json
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = 14, 10


def check_memory(min_gb=2):
    available = psutil.virtual_memory().available / 1024 ** 3
    if available < min_gb:
        print(
            f'⚠️  Low memory: {available:.1f}GB available (recommended: {min_gb}GB)'
            )
        response = input('Continue? (y/n): ')
        if response.lower() != 'y':
            return False
    return True


if not check_memory(min_gb=2):
    exit(0)
OUTPUT_DIR = '../results/mod_comments'
DEFAULT_REMOVAL_PATTERNS = {'spam': ['(?i)\\bspam\\b', '(?i)self[- ]promo',
    '(?i)advertis(e|ing|ement)', '(?i)referral\\s+link',
    '(?i)affiliate\\s+link'], 'rule_violation': ['(?i)\\brule\\s*\\d+\\b',
    '(?i)violat(ed|es)\\s+rule', '(?i)read\\s+the\\s+rules',
    '(?i)break(s|ing)\\s+the\\s+rules'], 'low_quality': [
    '(?i)low\\s+effort', '(?i)not\\s+enough\\s+context',
    '(?i)needs\\s+more\\s+details', '(?i)shitpost', '(?i)low\\s+quality',
    '(?i)does\\s+not\\s+meet\\s+quality\\s+standards'], 'repost': [
    '(?i)\\brepost\\b', '(?i)already\\s+posted', '(?i)duplicate\\s+post',
    '(?i)search\\s+before\\s+posting'], 'off_topic': ['(?i)off[- ]topic',
    '(?i)does\\s+not\\s+belong\\s+here', '(?i)wrong\\s+subreddit',
    '(?i)not\\s+about\\s+'], 'formatting': ['(?i)flair\\s+your\\s+post',
    '(?i)missing\\s+flair', '(?i)use\\s+the\\s+proper\\s+flair',
    '(?i)format(ting)?\\s+issue', '(?i)title\\s+must\\s+include',
    '(?i)link\\s+description'], 'civility': ['(?i)be\\s+civil',
    '(?i)personal\\s+attack', '(?i)harass(ment|ing)', '(?i)hate\\s+speech',
    '(?i)slur', '(?i)no\\s+insults'], 'misinformation': [
    '(?i)misinformation', '(?i)false\\s+information', '(?i)misleading',
    '(?i)unverified\\s+claim', '(?i)source\\s+required',
    '(?i)citation\\s+needed'], 'crowd_control': [
    '(?i)account\\s+too\\s+new', '(?i)need\\s+more\\s+karma',
    '(?i)karma\\s+requirement', '(?i)participation\\s+requirement',
    '(?i)you\\s+must\\s+be\\s+an\\s+active\\s+member'], 'megathread': [
    '(?i)megathread', '(?i)daily\\s+thread', '(?i)sticky\\s+thread',
    '(?i)weekly\\s+thread'], 'politics': ['(?i)politic(s|al)',
    '(?i)election', '(?i)partisan', '(?i)current\\s+events']}


def load_removal_patterns(config_path: str='config/removal_patterns.json'):
    if not os.path.exists(config_path):
        print(
            f'⚠️  No config file found at {config_path}. Using default removal patterns.'
            )
        return DEFAULT_REMOVAL_PATTERNS
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        cleaned = {}
        for k, v in data.items():
            if not isinstance(v, list):
                continue
            cleaned[str(k)] = [str(pat) for pat in v]
        if not cleaned:
            print(
                '⚠️  Config file is empty or invalid. Using default removal patterns.'
                )
            return DEFAULT_REMOVAL_PATTERNS
        print(f'✓ Loaded removal patterns from {config_path}')
        return cleaned
    except Exception as e:
        print(
            f'⚠️  Error loading {config_path}: {e}. Using default removal patterns.'
            )
        return DEFAULT_REMOVAL_PATTERNS


REMOVAL_PATTERNS = load_removal_patterns()
print('⚙️  Compiling regex patterns for performance...')
COMPILED_PATTERNS = {}
for category, patterns in REMOVAL_PATTERNS.items():
    combined_pattern = '|'.join(patterns)
    try:
        COMPILED_PATTERNS[category] = re.compile(combined_pattern, re.
            IGNORECASE)
    except re.error as e:
        print(f"⚠️  Regex error in category '{category}': {e}")


class AutomatedTemplateDetector:

    @staticmethod
    def is_template(text):
        if pd.isna(text) or not text:
            return True
        text = str(text).strip()
        text_lower = text.lower()
        indicators = {'very_long': len(text) > 800, 'many_urls': text.count
            ('http') > 2, 'has_automod': 'automoderator' in text_lower or 
            'bot' in text_lower, 'has_sidebar': 'sidebar' in text_lower and
            'rules' in text_lower, 'has_wiki': 'wiki' in text_lower and (
            'read' in text_lower or 'check' in text_lower), 'has_modmail': 
            'modmail' in text_lower or 'message the moderators' in
            text_lower, 'has_contact_mod': 'contact' in text_lower and 
            'moderators' in text_lower, 'has_appeals': 'appeal' in
            text_lower and ('message' in text_lower or 'contact' in
            text_lower), 'has_placeholder': bool(re.search(
            '\\[.*?\\]|\\{.*?\\}', text)), 'has_multiple_bullets': text.
            count('* ') > 3 or text.count('- ') > 3, 'very_short': len(text
            ) < 30, 'boilerplate_start': any(text_lower.startswith(phrase) for
            phrase in ['your post has been removed',
            'your submission has been removed',
            'this post has been removed', 'your comment has been removed',
            'thank you for your submission']), 'action_performed': 
            'this action was performed' in text_lower or 'i am a bot' in
            text_lower}
        score = sum([indicators['has_automod'] * 3, indicators[
            'has_modmail'] * 2, indicators['has_contact_mod'] * 2, 
            indicators['boilerplate_start'] * 2, indicators[
            'action_performed'] * 3, indicators['very_long'] * 1, 
            indicators['many_urls'] * 1, indicators['has_sidebar'] * 1, 
            indicators['has_wiki'] * 1, indicators['has_appeals'] * 1, 
            indicators['has_placeholder'] * 1, indicators[
            'has_multiple_bullets'] * 1, indicators['very_short'] * 1])
        return score >= 3


def categorize_removal_reason(comment_text):
    if pd.isna(comment_text) or comment_text == '':
        return []
    comment_lower = str(comment_text).lower()
    categories = []
    for category, pattern_obj in COMPILED_PATTERNS.items():
        if pattern_obj.search(comment_lower):
            categories.append(category)
    if not categories:
        categories.append('other')
    return categories


def extract_rule_numbers(comment_text):
    if pd.isna(comment_text):
        return []
    matches = re.findall('\\b(?:rule|r)[.\\s]*(\\d+)', str(comment_text),
        re.IGNORECASE)
    unique_rules = sorted(list(set([int(m) for m in matches])))
    return unique_rules


def clean_comment_for_display(comment_text, max_length=200):
    if pd.isna(comment_text):
        return ''
    text = str(comment_text)
    text = re.sub('\\n+', ' | ', text)
    text = re.sub('\\s+', ' ', text)
    return text[:max_length] + '...' if len(text) > max_length else text.strip(
        )


def extract_keywords_from_comments(comments, n=30):
    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'is', 'are', 'was',
        'were', 'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
        'would', 'should', 'could', 'to', 'of', 'in', 'for', 'on', 'with',
        'at', 'by', 'from', 'as', 'this', 'that', 'your', 'you', 'our',
        'be', 'it', 'not', 'been', 'please', 'thank', 'thanks'}
    all_words = []
    for comment in comments:
        if pd.notna(comment):
            words = re.findall('\\b[a-z]{4,}\\b', str(comment).lower())
            all_words.extend([w for w in words if w not in stopwords])
    return Counter(all_words).most_common(n)


def select_primary_mod_comment(row):
    raw = row.get('mod_comments_json', None)
    if not isinstance(raw, str) or not raw.strip():
        return None, None, None
    try:
        comments = json.loads(raw)
        if not isinstance(comments, list) or not comments:
            return None, None, None
    except Exception:
        return None, None, None
    parsed = []
    for c in comments:
        if isinstance(c, dict):
            parsed.append({'body': c.get('body', ''), 'author': c.get(
                'author', 'Unknown'), 'created_utc': c.get('created_utc', 0
                ) or 0, 'is_removal': bool(c.get('is_removal', False)),
                'is_distinguished': c.get('is_distinguished', False)})
    if not parsed:
        return None, None, None
    parsed.sort(key=lambda x: x['created_utc'])
    removals = [c for c in parsed if c['is_removal']]
    if removals:
        target = removals[-1]
        return target['body'], target['author'], target['created_utc']
    removal_keywords = re.compile(
        '\\b(removed|violation|review|unfortunately|rule|sorry)\\b', re.
        IGNORECASE)
    likely_removals = [c for c in parsed if c['body'] and removal_keywords.
        search(c['body'])]
    if likely_removals:
        target = likely_removals[-1]
        return target['body'], target['author'], target['created_utc']
    target = parsed[-1]
    return target['body'], target['author'], target['created_utc']


print('🔍 Looking for data files...')
master_file = '../data/linguistics_boolean_fixed.csv'
if os.path.exists(master_file):
    print(f'✓ Found Master Dataset: {master_file}')
    df = pd.read_csv(master_file)
else:
    print('⚠️  Master file not found. Falling back to raw data.')
    data_files = glob.glob('data/combined_complete_*.csv')
    if not data_files:
        print('❌ No data found.')
        exit(1)
    latest_file = max(data_files, key=os.path.getctime)
    df = pd.read_csv(latest_file)
if 'mod_comments_json' in df.columns:
    print(
        "Detected 'mod_comments_json' column. Extracting primary mod comment per post..."
        )
    texts = []
    authors = []
    createds = []
    for _, row in df.iterrows():
        text, author, created = select_primary_mod_comment(row)
        texts.append(text)
        authors.append(author)
        createds.append(created)
    if 'mod_comment_text' not in df.columns:
        df['mod_comment_text'] = texts
    else:
        df['mod_comment_text'] = df['mod_comment_text'].fillna(pd.Series(texts)
            )
    if 'mod_comment_author' not in df.columns:
        df['mod_comment_author'] = authors
    else:
        df['mod_comment_author'] = df['mod_comment_author'].fillna(pd.
            Series(authors))
    if 'mod_comment_created_utc' not in df.columns:
        df['mod_comment_created_utc'] = createds
    else:
        df['mod_comment_created_utc'] = df['mod_comment_created_utc'].fillna(pd
            .Series(createds))
    print("✓ Primary mod comments extracted from 'mod_comments_json'")
print(f'✓ Loaded {len(df)} posts')
os.makedirs('../results/mod_comments', exist_ok=True)
if 'mod_comment_text' not in df.columns:
    print(
        "❌ 'mod_comment_text' column missing. Run Pass 2 with mod comment collection enabled."
        )
    exit(1)
posts_with_comments = df[df['mod_comment_text'].notna()].copy()
metrics_available = {'age': 'author_account_age_days' in
    posts_with_comments.columns, 'karma': 'author_total_karma' in
    posts_with_comments.columns, 'toxicity': 'thread_deletion_ratio' in
    posts_with_comments.columns, 'title_toxicity': 'title_toxicity' in
    posts_with_comments.columns, 'selftext_toxicity': 'selftext_toxicity' in
    posts_with_comments.columns, 'title_sentiment': 
    'title_sentiment_polarity' in posts_with_comments.columns,
    'selftext_sentiment': 'selftext_sentiment_polarity' in
    posts_with_comments.columns, 'title_readability': 
    'title_readability_flesch_reading_ease' in posts_with_comments.columns,
    'selftext_readability': 'selftext_readability_flesch_reading_ease' in
    posts_with_comments.columns}
print(f"""
📊 Data Availability:""")
print(f"   Account Age: {'✓' if metrics_available['age'] else '✗'}")
print(f"   Total Karma: {'✓' if metrics_available['karma'] else '✗'}")
print(f"   Thread Toxicity: {'✓' if metrics_available['toxicity'] else '✗'}")
print(f"""
📌 Initial Statistics:""")
print(f'   Total posts: {len(df)}')
print(f"   Removed posts: {df['is_removed_inferred'].sum()}")
print(
    f'   Posts with mod comments: {len(posts_with_comments)} ({len(posts_with_comments) / len(df) * 100:.1f}%)'
    )
if len(posts_with_comments) == 0:
    print('\n⚠️  No mod comments found in data!')
    exit(1)
time_cols_present = ('created_utc' in posts_with_comments.columns and 
    'mod_comment_created_utc' in posts_with_comments.columns)
if time_cols_present:
    posts_with_comments['created_utc'] = pd.to_numeric(posts_with_comments[
        'created_utc'], errors='coerce')
    posts_with_comments['mod_comment_created_utc'] = pd.to_numeric(
        posts_with_comments['mod_comment_created_utc'], errors='coerce')
    posts_with_comments['time_to_mod_comment_min'] = (posts_with_comments[
        'mod_comment_created_utc'] - posts_with_comments['created_utc']) / 60.0
    valid_ttc_mask = (posts_with_comments['time_to_mod_comment_min'] >= 0) & (
        posts_with_comments['time_to_mod_comment_min'] <= 7 * 24 * 60)
    posts_with_comments.loc[~valid_ttc_mask, 'time_to_mod_comment_min'
        ] = np.nan
    print('\n=== Time-to-mod-comment summary (minutes) ===')
    print(posts_with_comments['time_to_mod_comment_min'].describe())
else:
    print(
        """
⚠️  created_utc and/or mod_comment_created_utc missing. Time-to-comment analyses will be skipped."""
        )
if 'mod_comment_author' in posts_with_comments.columns:
    KNOWN_BOTS = {'qualityvote', 'botdefense', 'magic_eye_bot',
        'assistantbot', 'repostsleuthbot', 'moderator-bot', 'visualmod',
        'autotheme'}

    def is_automod(author):
        if not isinstance(author, str):
            return False
        author_lower = author.lower()
        return ('automod' in author_lower or author_lower in KNOWN_BOTS or
            author_lower.endswith('bot'))
    posts_with_comments['agent_type'] = posts_with_comments[
        'mod_comment_author'].apply(lambda a: 'AutoModerator' if is_automod
        (a) else 'HumanMod')
    agent_counts = posts_with_comments['agent_type'].value_counts(dropna=False)
    print('\n=== Mod agent types ===')
    print(agent_counts)
else:
    posts_with_comments['agent_type'] = np.nan
    print(
        "\n⚠️  'mod_comment_author' column missing. AutoMod vs human analyses limited."
        )
print('\n' + '=' * 60)
print('PROCESSING MODERATOR COMMENTS')
print('=' * 60)
posts_with_comments['removal_categories'] = posts_with_comments[
    'mod_comment_text'].apply(categorize_removal_reason)
posts_with_comments['rule_numbers'] = posts_with_comments['mod_comment_text'
    ].apply(extract_rule_numbers)
rule_exploded = posts_with_comments.explode('rule_numbers')
rule_numbers = rule_exploded['rule_numbers'].dropna()


def count_non_other(categories):
    if not isinstance(categories, (list, tuple)):
        return 0
    return sum(1 for c in categories if c != 'other')


posts_with_comments['num_categories'] = posts_with_comments[
    'removal_categories'].apply(lambda cats: len(cats) if isinstance(cats,
    (list, tuple)) else 0)
posts_with_comments['num_non_other'] = posts_with_comments['removal_categories'
    ].apply(count_non_other)
total_commented = len(posts_with_comments)
zero_match = (posts_with_comments['num_non_other'] == 0).sum()
one_match = (posts_with_comments['num_non_other'] == 1).sum()
multi_match = (posts_with_comments['num_non_other'] >= 2).sum()
print("""
=== Classification coverage ===""")
print(f'Total posts with mod comments: {total_commented}')
print(
    f'0 pattern matches (other-only): {zero_match} ({zero_match / total_commented:.1%})'
    )
print(
    f'1 pattern match:                {one_match} ({one_match / total_commented:.1%})'
    )
print(
    f'2+ pattern matches:             {multi_match} ({multi_match / total_commented:.1%})'
    )
other_only = posts_with_comments[posts_with_comments['num_non_other'] == 0
    ].copy()
if not other_only.empty:
    sample_size = min(100, len(other_only))
    other_sample = other_only.sample(sample_size, random_state=42)
    other_sample['cleaned_comment'] = other_sample['mod_comment_text'].apply(
        lambda s: clean_comment_for_display(s, max_length=300))
    cols_for_manual = [col for col in ['subreddit', 'post_fullname',
        'mod_comment_author', 'mod_comment_text', 'cleaned_comment',
        'removal_categories'] if col in other_sample.columns]
    other_sample[cols_for_manual].to_csv(os.path.join(OUTPUT_DIR,
        'sample_other.csv'), index=False)
    print(
        f"✓ Saved a sample of {sample_size} 'other-only' comments to sample_other.csv"
        )
else:
    print("No 'other-only' comments to sample.")
exploded_df = posts_with_comments.explode('removal_categories')
all_categories = []
for categories in posts_with_comments['removal_categories']:
    all_categories.extend(categories)
category_counts = Counter(all_categories)
total_category_assignments = sum(category_counts.values())
print(f"""
✓ Categorized {len(posts_with_comments)} mod comments""")
print(f'✓ Found {len(category_counts)} distinct categories')
print('\n' + '=' * 60)
print('ANALYSIS 1: CATEGORY DISTRIBUTION')
print('=' * 60)
print(f"""
{'Category':<25} {'Count':<10} {'Percentage'}""")
print('-' * 50)
for category, count in category_counts.most_common():
    pct = count / total_category_assignments * 100
    print(f'{category:<25} {count:<10} {pct:.1f}%')
if time_cols_present:
    ttc_by_cat = exploded_df.dropna(subset=['time_to_mod_comment_min']
        ).groupby('removal_categories')['time_to_mod_comment_min'].median(
        ).sort_values()
    print('\n=== Median time-to-mod-comment by removal category (minutes) ===')
    print(ttc_by_cat)
    ttc_by_cat.to_csv(os.path.join(OUTPUT_DIR,
        'time_to_comment_by_category.csv'), header=[
        'median_time_to_mod_comment_min'])
    print('✓ Saved time_to_comment_by_category.csv')
print('\n' + '=' * 60)
print('ANALYSIS 2: RULE VIOLATIONS')
print('=' * 60)
rule_numbers = posts_with_comments['rule_numbers'].explode().dropna()
if len(rule_numbers) > 0:
    rule_counts = rule_numbers.value_counts().sort_index()
    print(f"\n{'Rule':<10} {'Count':<10} {'Percentage'}")
    print('-' * 35)
    for rule, count in rule_counts.head(10).items():
        pct = count / len(rule_numbers) * 100
        print(f'Rule {rule:<5} {count:<10} {pct:.1f}%')
else:
    print('\n⚠️  No rule numbers found in mod comments')
print('\n' + '=' * 60)
print('ANALYSIS 3: REMOVAL REASONS vs USER METRICS')
print('=' * 60)
agg_dict = {'post_fullname': 'count'}
if metrics_available['age']:
    agg_dict['author_account_age_days'] = 'median'
if metrics_available['karma']:
    agg_dict['author_total_karma'] = 'median'
if metrics_available['toxicity']:
    agg_dict['thread_deletion_ratio'] = 'mean'
metric_stats = exploded_df.groupby('removal_categories').agg(agg_dict)
metric_stats.rename(columns={'post_fullname': 'count'}, inplace=True)
metric_stats = metric_stats[metric_stats['count'] > 5].sort_values('count',
    ascending=False)
print("""
📊 Metrics per Removal Category:""")
print(metric_stats.round(2).to_string())
print('\n' + '=' * 60)
print('ANALYSIS 4: COMMUNITY FINGERPRINTS')
print('=' * 60)
crosstab = pd.crosstab(exploded_df['subreddit'], exploded_df[
    'removal_categories'])
top_subs = crosstab.sum(axis=1).nlargest(20).index
top_cats = crosstab.sum(axis=0).nlargest(10).index
crosstab_filtered = crosstab.loc[top_subs, top_cats]
heatmap_data = crosstab_filtered.div(crosstab_filtered.sum(axis=1), axis=0
    ) * 100
print(
    f"""
✓ Created fingerprint matrix: {len(top_subs)} subreddits × {len(top_cats)} categories"""
    )
print(
    '   (Data normalized by row: % of removals in a subreddit belonging to a category)'
    )
print('\n' + '=' * 60)
print('ANALYSIS 5: AUTOMOD vs HUMAN MODERATORS')
print('=' * 60)
if 'mod_comment_author' in posts_with_comments.columns:
    automod_comments = posts_with_comments[posts_with_comments[
        'mod_comment_author'].str.lower().str.contains('automod', na=False)]
    human_mod_comments = posts_with_comments[~posts_with_comments[
        'mod_comment_author'].str.lower().str.contains('automod', na=False) &
        posts_with_comments['mod_comment_author'].notna()]
    print(f'\n   AutoMod comments: {len(automod_comments)}')
    print(f'   Human Mod comments: {len(human_mod_comments)}')
else:
    automod_comments = pd.DataFrame()
    human_mod_comments = pd.DataFrame()
    print('\n⚠️  mod_comment_author column not available')
print('\n' + '=' * 60)
print('ANALYSIS 6: SUBREDDIT-BY-SUBREDDIT BREAKDOWN')
print('=' * 60)
subreddit_results = []
for subreddit in sorted(df['subreddit'].unique()):
    sub_df = df[df['subreddit'] == subreddit]
    sub_comments = posts_with_comments[posts_with_comments['subreddit'] ==
        subreddit]
    if len(sub_comments) > 0:
        comment_rate = len(sub_comments) / len(sub_df) * 100
        all_cats = []
        for cats in sub_comments['removal_categories']:
            if isinstance(cats, (list, tuple)):
                all_cats.extend(cats)
        if all_cats:
            top_reason = Counter(all_cats).most_common(1)[0][0]
        else:
            top_reason = 'other'
        subreddit_results.append({'subreddit': subreddit, 'total_posts':
            len(sub_df), 'comments': len(sub_comments), 'comment_rate':
            comment_rate, 'top_reason': top_reason})
subreddit_df = pd.DataFrame(subreddit_results).sort_values('comment_rate',
    ascending=False)
print(f"""
{'Subreddit':<25} {'Comment Rate':<15} {'Top Reason'}""")
print('-' * 60)
for _, row in subreddit_df.head(10).iterrows():
    print(
        f"{row['subreddit']:<25} {row['comment_rate']:>6.1f}%         {row['top_reason']}"
        )
print('\n' + '=' * 60)
print('ANALYSIS 7: COMMON KEYWORDS IN MOD COMMENTS')
print('=' * 60)
keywords = extract_keywords_from_comments(posts_with_comments[
    'mod_comment_text'])
print(f"""
Top 20 Keywords:""")
print(f"{'Keyword':<20} {'Count'}")
print('-' * 35)
for word, count in keywords[:20]:
    print(f'{word:<20} {count}')
print('\n' + '=' * 60)
print('CREATING VISUALIZATIONS')
print('=' * 60)
print("""
📊 Creating visualizations...""")
fig, ax = plt.subplots(figsize=(12, 8))
sorted_cats = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
cats = [k for k, v in sorted_cats]
counts = [v for k, v in sorted_cats]
pcts = [(v / len(posts_with_comments) * 100) for v in counts]
bars = ax.barh(cats[::-1], pcts[::-1], color=plt.cm.Set3(np.linspace(0, 1,
    len(cats))), edgecolor='black')
ax.set_xlabel('Percentage of Removed Posts (%)', fontsize=12, fontweight='bold'
    )
ax.set_title(
    """Distribution of Removal Categories
(Note: Sums > 100% due to multiple reasons per post)"""
    , fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)
for bar in bars:
    width = bar.get_width()
    ax.text(width + 0.5, bar.get_y() + bar.get_height() / 2,
        f'{width:.1f}%', ha='left', va='center', fontsize=10)
plt.tight_layout()
plt.savefig('../results/mod_comments/1_removal_categories_bar.png', dpi=300,
    bbox_inches='tight')
print('✓ Saved: 1_removal_categories_bar.png (Replaced Pie Chart)')
plt.close()
fig, ax = plt.subplots(figsize=(12, 8))
top_categories = category_counts.most_common(10)
cats = [cat for cat, _ in top_categories]
cnts = [count for _, count in top_categories]
bars = ax.barh(cats[::-1], cnts[::-1], color='steelblue', edgecolor='black')
ax.set_xlabel('Number of Posts', fontsize=12, fontweight='bold')
ax.set_title('Top 10 Removal Reason Categories', fontsize=14, fontweight='bold'
    )
ax.grid(axis='x', alpha=0.3)
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax.text(width + 5, bar.get_y() + bar.get_height() / 2, f'{int(width)}',
        ha='left', va='center', fontweight='bold')
plt.tight_layout()
plt.savefig('../results/mod_comments/2_top_removal_categories.png', dpi=300,
    bbox_inches='tight')
print('✓ Saved: 2_top_removal_categories.png')
plt.close()
plt.figure(figsize=(14, 10))
sns.heatmap(heatmap_data, annot=True, fmt='.0%', cmap='YlGnBu', linewidths=0.5)
plt.title(
    """Community Removal Fingerprints
(What % of removals in each subreddit are due to each category?)"""
    , fontsize=14, fontweight='bold')
plt.ylabel('Subreddit')
plt.xlabel('Removal Category')
plt.tight_layout()
plt.savefig('../results/mod_comments/3_heatmap_fingerprints.png', dpi=300)
print('✓ Saved: 3_heatmap_fingerprints.png ⭐ (KEY INSIGHT)')
plt.close()
if len(rule_numbers) > 0:
    fig, ax = plt.subplots(figsize=(12, 6))
    rule_counts_chart = rule_numbers.value_counts().sort_index().head(15)
    bars = ax.bar(rule_counts_chart.index.astype(str), rule_counts_chart.
        values, color='coral', edgecolor='black')
    ax.set_xlabel('Rule Number', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Violations', fontsize=12, fontweight='bold')
    ax.set_title('Most Frequently Violated Rules', fontsize=14, fontweight=
        'bold')
    ax.grid(axis='y', alpha=0.3)
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, height,
            f'{int(height)}', ha='center', va='bottom', fontweight='bold')
    plt.tight_layout()
    plt.savefig('../results/mod_comments/4_rule_violations.png', dpi=300,
        bbox_inches='tight')
    print('✓ Saved: 4_rule_violations.png')
    plt.close()
if 'rule_numbers' in posts_with_comments.columns:
    rule_agent_df = posts_with_comments.dropna(subset=['rule_numbers',
        'agent_type']).explode('rule_numbers')
    if not rule_agent_df.empty:
        rule_agent_counts = rule_agent_df.groupby(['agent_type',
            'rule_numbers'])['post_fullname'].count().reset_index(name='count')
        rule_agent_counts.rename(columns={'rule_numbers': 'rule_number'},
            inplace=True)
        print('\n=== Rule violations by agent type ===')
        for agent in rule_agent_counts['agent_type'].unique():
            sub = rule_agent_counts[rule_agent_counts['agent_type'] == agent]
            total = sub['count'].sum()
            print(f'\n{agent}:')
            for _, row in sub.sort_values('count', ascending=False).iterrows():
                pct = row['count'] / total if total > 0 else 0
                print(
                    f"  Rule {int(row['rule_number'])}: {row['count']} ({pct:.1%})"
                    )
        rule_agent_counts.to_csv(os.path.join(OUTPUT_DIR,
            'rule_violations_by_agent_type.csv'), index=False)
        print('✓ Saved rule_violations_by_agent_type.csv')
    else:
        print('\n⚠️  No rule numbers + agent_type data available.')
fig, ax = plt.subplots(figsize=(14, 8))
subreddit_df_sorted = subreddit_df.sort_values('comment_rate', ascending=True)
y_pos = range(len(subreddit_df_sorted))
bars = ax.barh(y_pos, subreddit_df_sorted['comment_rate'], color=
    'lightgreen', edgecolor='black')
ax.set_yticks(y_pos)
ax.set_yticklabels(subreddit_df_sorted['subreddit'])
ax.set_xlabel('Mod Comment Rate (%)', fontsize=12, fontweight='bold')
ax.set_title('Mod Comment Rate by Subreddit', fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax.text(width + 0.2, bar.get_y() + bar.get_height() / 2,
        f'{width:.1f}%', ha='left', va='center', fontsize=9)
plt.tight_layout()
plt.savefig('../results/mod_comments/5_comment_rate_by_subreddit.png', dpi=
    300, bbox_inches='tight')
print('✓ Saved: 5_comment_rate_by_subreddit.png')
plt.close()
if metrics_available['toxicity']:
    plt.figure(figsize=(12, 6))
    tox_data = exploded_df.groupby('removal_categories')[
        'thread_deletion_ratio'].mean().sort_values()
    bars = plt.barh(tox_data.index, tox_data.values, color='salmon',
        edgecolor='black')
    plt.title('Thread Toxicity (Graveyard Effect) by Removal Category',
        fontsize=14, fontweight='bold')
    plt.xlabel('Avg Thread Deletion Ratio (Higher = More Toxic)')
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('../results/mod_comments/6_toxicity_by_category.png', dpi=300)
    print('✓ Saved: 6_toxicity_by_category.png')
    plt.close()
if metrics_available['age']:
    plt.figure(figsize=(12, 6))
    age_data = exploded_df.groupby('removal_categories')[
        'author_account_age_days'].median().sort_values()
    plt.barh(age_data.index, age_data.values, color='skyblue', edgecolor=
        'black')
    plt.title('Median Account Age by Removal Category', fontsize=14,
        fontweight='bold')
    plt.xlabel('Median Days')
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('../results/mod_comments/7_age_by_category.png', dpi=300)
    print('✓ Saved: 7_age_by_category.png')
    plt.close()
if len(automod_comments) > 0 and len(human_mod_comments) > 0:
    fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharex=True)
    ax1 = axes[0]
    automod_categories = []
    for categories in automod_comments['mod_comment_text'].apply(
        categorize_removal_reason):
        automod_categories.extend(categories)
    automod_counter = Counter(automod_categories).most_common(8)
    if automod_counter:
        cats = [cat for cat, _ in automod_counter]
        cnts = [count for _, count in automod_counter]
        ax1.barh(cats[::-1], cnts[::-1], color='#3498db', edgecolor='black')
        ax1.set_title(f'AutoModerator (n={len(automod_comments)})',
            fontsize=12, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)
    ax2 = axes[1]
    human_categories = []
    for categories in human_mod_comments['mod_comment_text'].apply(
        categorize_removal_reason):
        human_categories.extend(categories)
    human_counter = Counter(human_categories).most_common(8)
    if human_counter:
        cats = [cat for cat, _ in human_counter]
        cnts = [count for _, count in human_counter]
        ax2.barh(cats[::-1], cnts[::-1], color='#e74c3c', edgecolor='black')
        ax2.set_title(f'Human Moderators (n={len(human_mod_comments)})',
            fontsize=12, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)
    fig.suptitle('Removal Reasons: Bots vs Humans', fontsize=14)
    plt.tight_layout()
    plt.savefig('../results/mod_comments/8_automod_vs_human.png', dpi=300,
        bbox_inches='tight')
    print('✓ Saved: 8_automod_vs_human.png')
    plt.close()
linguistic_cols = []
if metrics_available.get('title_toxicity', False):
    linguistic_cols.append('title_toxicity')
if metrics_available.get('selftext_toxicity', False):
    linguistic_cols.append('selftext_toxicity')
if metrics_available.get('title_sentiment', False):
    linguistic_cols.append('title_sentiment_polarity')
if metrics_available.get('selftext_sentiment', False):
    linguistic_cols.append('selftext_sentiment_polarity')
if metrics_available.get('title_readability', False):
    linguistic_cols.append('title_readability_flesch_reading_ease')
if metrics_available.get('selftext_readability', False):
    linguistic_cols.append('selftext_readability_flesch_reading_ease')
if linguistic_cols:
    ling_df = exploded_df.copy()
    agg_dict_ling = {'post_fullname': 'count'}
    for col in linguistic_cols:
        agg_dict_ling[col] = 'median'
    linguistic_by_category = ling_df.groupby('removal_categories').agg(
        agg_dict_ling).rename(columns={'post_fullname': 'count'}).reset_index()
    linguistic_by_category = linguistic_by_category[linguistic_by_category[
        'count'] >= 10]
    print('\n=== Linguistic metrics by removal category ===')
    print(linguistic_by_category.sort_values('count', ascending=False).head(20)
        )
    linguistic_by_category.to_csv(os.path.join(OUTPUT_DIR,
        'linguistic_metrics_by_category.csv'), index=False)
    print('✓ Saved linguistic_metrics_by_category.csv')
if metrics_available.get('title_toxicity', False) or metrics_available.get(
    'selftext_toxicity', False):
    tox_df = exploded_df.copy()
    cat_counts_tox = tox_df['removal_categories'].value_counts()
    keep_cats_tox = cat_counts_tox[cat_counts_tox >= 30].index
    tox_df = tox_df[tox_df['removal_categories'].isin(keep_cats_tox)]
    if not tox_df.empty:
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        if metrics_available.get('title_toxicity', False):
            sns.boxplot(data=tox_df, x='removal_categories', y=
                'title_toxicity', ax=axes[0], showfliers=False)
            axes[0].set_title('Title Toxicity by Removal Category')
            axes[0].set_xlabel('Removal Category')
            axes[0].set_ylabel('Title Toxicity Score')
            axes[0].tick_params(axis='x', rotation=45)
        if metrics_available.get('selftext_toxicity', False):
            sns.boxplot(data=tox_df, x='removal_categories', y=
                'selftext_toxicity', ax=axes[1], showfliers=False)
            axes[1].set_title('Selftext Toxicity by Removal Category')
            axes[1].set_xlabel('Removal Category')
            axes[1].set_ylabel('Selftext Toxicity Score')
            axes[1].tick_params(axis='x', rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, '9_toxicity_by_category.png'),
            dpi=300, bbox_inches='tight')
        print('✓ Saved: 9_toxicity_by_category.png')
        plt.close()
if metrics_available.get('title_sentiment', False) or metrics_available.get(
    'selftext_sentiment', False):
    sent_df = exploded_df.copy()
    cat_counts_sent = sent_df['removal_categories'].value_counts()
    keep_cats_sent = cat_counts_sent[cat_counts_sent >= 30].index
    sent_df = sent_df[sent_df['removal_categories'].isin(keep_cats_sent)]
    if not sent_df.empty:
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        if metrics_available.get('title_sentiment', False):
            sns.boxplot(data=sent_df, x='removal_categories', y=
                'title_sentiment_polarity', ax=axes[0], showfliers=False)
            axes[0].set_title('Title Sentiment by Removal Category')
            axes[0].set_xlabel('Removal Category')
            axes[0].set_ylabel('Sentiment Polarity (-1 to 1)')
            axes[0].tick_params(axis='x', rotation=45)
            axes[0].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        if metrics_available.get('selftext_sentiment', False):
            sns.boxplot(data=sent_df, x='removal_categories', y=
                'selftext_sentiment_polarity', ax=axes[1], showfliers=False)
            axes[1].set_title('Selftext Sentiment by Removal Category')
            axes[1].set_xlabel('Removal Category')
            axes[1].set_ylabel('Sentiment Polarity (-1 to 1)')
            axes[1].tick_params(axis='x', rotation=45)
            axes[1].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, '10_sentiment_by_category.png'
            ), dpi=300, bbox_inches='tight')
        print('✓ Saved: 10_sentiment_by_category.png')
        plt.close()
metrics_cols = []
if metrics_available.get('age', False):
    metrics_cols.append('author_account_age_days')
if metrics_available.get('karma', False):
    metrics_cols.append('author_total_karma')
if metrics_available.get('toxicity', False):
    metrics_cols.append('thread_deletion_ratio')
if metrics_cols:
    auto_human_df = exploded_df.dropna(subset=['agent_type']).copy()
    agg_dict = {'post_fullname': 'count'}
    for col in metrics_cols:
        agg_dict[col] = 'median'
    metrics_by_agent_cat = auto_human_df.groupby(['agent_type',
        'removal_categories']).agg(agg_dict).rename(columns={
        'post_fullname': 'count'}).reset_index()
    metrics_by_agent_cat = metrics_by_agent_cat[metrics_by_agent_cat[
        'count'] >= 10]
    print('\n=== User metrics by agent type and removal category ===')
    print(metrics_by_agent_cat.sort_values(['agent_type', 'count'],
        ascending=[True, False]).head(30))
    metrics_by_agent_cat.to_csv(os.path.join(OUTPUT_DIR,
        'metrics_by_agent_and_category.csv'), index=False)
    print('✓ Saved metrics_by_agent_and_category.csv')
if time_cols_present:
    ttc_plot_df = exploded_df.dropna(subset=['time_to_mod_comment_min']).copy()
    cat_counts_ttc = ttc_plot_df['removal_categories'].value_counts()
    keep_cats = cat_counts_ttc[cat_counts_ttc >= 30].index
    ttc_plot_df = ttc_plot_df[ttc_plot_df['removal_categories'].isin(keep_cats)
        ]
    if not ttc_plot_df.empty:
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=ttc_plot_df, x='removal_categories', y=
            'time_to_mod_comment_min', showfliers=False)
        plt.xticks(rotation=45, ha='right')
        plt.ylabel('Time to first mod comment (minutes)')
        plt.xlabel('Removal category')
        plt.title('Time to Moderation by Removal Category')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR,
            'time_to_comment_boxplot_by_category.png'), dpi=300)
        plt.close()
        print('✓ Saved time_to_comment_boxplot_by_category.png')
if time_cols_present:
    ttc_agent_df = posts_with_comments.dropna(subset=[
        'time_to_mod_comment_min', 'agent_type']).copy()
    if not ttc_agent_df.empty:
        ttc_stats_agent = ttc_agent_df.groupby('agent_type')[
            'time_to_mod_comment_min'].describe()
        print('\n=== Time-to-mod-comment by agent type (minutes) ===')
        print(ttc_stats_agent)
        ttc_stats_agent.to_csv(os.path.join(OUTPUT_DIR,
            'time_to_comment_by_agent_type.csv'))
        print('✓ Saved time_to_comment_by_agent_type.csv')
        plt.figure(figsize=(6, 5))
        sns.boxplot(data=ttc_agent_df, x='agent_type', y=
            'time_to_mod_comment_min', showfliers=False)
        plt.ylabel('Time to first mod comment (minutes)')
        plt.xlabel('Moderator type')
        plt.title('Time to Moderation: AutoModerator vs Human Mods')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR,
            'time_to_comment_boxplot_by_agent_type.png'), dpi=300)
        plt.close()
        print('✓ Saved time_to_comment_boxplot_by_agent_type.png')
print('\n' + '=' * 70)
print('TEMPLATE VS HUMAN CLASSIFICATION')
print('=' * 70)
classified = posts_with_comments[posts_with_comments['mod_comment_text'].
    astype(str).str.strip() != ''].copy()
if len(classified) == 0:
    print(
        '⚠️  No non-empty moderator comments available for template detection.'
        )
else:
    classified['is_template'] = classified['mod_comment_text'].apply(
        AutomatedTemplateDetector.is_template)
    n_template = int(classified['is_template'].sum())
    n_human = int((~classified['is_template']).sum())
    total = len(classified)
    pct_template = 100.0 * n_template / total
    pct_human = 100.0 * n_human / total
    print(f'📊 Template-like comments: {n_template:,} ({pct_template:.1f}%)')
    print(f'👤 Human-like comments:    {n_human:,} ({pct_human:.1f}%)')
print('\n' + '=' * 60)
print('SAVING CSV RESULTS')
print('=' * 60)
category_df = pd.DataFrame(category_counts.most_common(), columns=[
    'Category', 'Count'])
total_category_assignments = category_df['Count'].sum()
category_df['Percentage'] = (category_df['Count'] /
    total_category_assignments * 100).round(2)
category_df.to_csv('../results/mod_comments/category_summary.csv', index=False)
print('✓ Saved: category_summary.csv')
if len(metric_stats) > 0:
    metric_stats.to_csv('../results/mod_comments/removal_reason_metrics.csv')
    print('✓ Saved: removal_reason_metrics.csv')
heatmap_data.to_csv('../results/mod_comments/subreddit_fingerprints.csv')
print('✓ Saved: subreddit_fingerprints.csv')
if len(rule_numbers) > 0:
    rule_df = pd.DataFrame(rule_numbers.value_counts().sort_index(),
        columns=['Count'])
    rule_df.index.name = 'Rule Number'
    rule_df.to_csv('../results/mod_comments/rule_violations.csv')
    print('✓ Saved: rule_violations.csv')
subreddit_df.to_csv('../results/mod_comments/subreddit_summary.csv', index=
    False)
print('✓ Saved: subreddit_summary.csv')
keywords_df = pd.DataFrame(keywords, columns=['Keyword', 'Count'])
keywords_df.to_csv('../results/mod_comments/common_keywords.csv', index=False)
print('✓ Saved: common_keywords.csv')
export_columns = ['post_fullname', 'subreddit', 'mod_comment_text']
if 'author_username' in posts_with_comments.columns:
    export_columns.append('author_username')
if metrics_available['age']:
    export_columns.append('author_account_age_days')
if metrics_available['karma']:
    export_columns.append('author_total_karma')
if 'mod_comment_author' in posts_with_comments.columns:
    export_columns.append('mod_comment_author')
if 'mod_comment_is_distinguished' in posts_with_comments.columns:
    export_columns.append('mod_comment_is_distinguished')
if 'removed_by_category' in posts_with_comments.columns:
    export_columns.append('removed_by_category')
if metrics_available.get('title_toxicity', False):
    export_columns.append('title_toxicity')
if metrics_available.get('selftext_toxicity', False):
    export_columns.append('selftext_toxicity')
if metrics_available.get('title_sentiment', False):
    export_columns.append('title_sentiment_polarity')
if metrics_available.get('selftext_sentiment', False):
    export_columns.append('selftext_sentiment_polarity')
if metrics_available.get('title_readability', False):
    export_columns.append('title_readability_flesch_reading_ease')
if metrics_available.get('selftext_readability', False):
    export_columns.append('selftext_readability_flesch_reading_ease')
export_df = posts_with_comments[export_columns].copy()
export_df['cleaned_comment'] = export_df['mod_comment_text'].apply(lambda x:
    clean_comment_for_display(x, max_length=500))
export_df['removal_categories'] = posts_with_comments['removal_categories'
    ].apply(lambda x: ', '.join(x) if x else '')
export_df['rule_numbers'] = posts_with_comments['rule_numbers'].apply(lambda
    x: ', '.join(map(str, x)) if isinstance(x, list) else '')
export_df.to_csv('../results/mod_comments/all_mod_comments.csv', index=False)
print('✓ Saved: all_mod_comments.csv')
print("""
✓ Saving sample comments by category...""")
for category in top_cats:
    if category == 'other':
        continue
    category_posts = posts_with_comments[posts_with_comments[
        'removal_categories'].apply(lambda x: category in x)]
    if len(category_posts) > 0:
        sample = category_posts[['subreddit', 'mod_comment_text']].head(10
            ).copy()
        sample['cleaned_comment'] = sample['mod_comment_text'].apply(lambda
            x: clean_comment_for_display(x, max_length=300))
        filename = f'../results/mod_comments/sample_{category}.csv'
        sample[['subreddit', 'cleaned_comment']].to_csv(filename, index=False)
        print(f'  ✓ Saved: sample_{category}.csv')
print('\n' + '=' * 60)
print('✅ ULTIMATE MOD COMMENT ANALYSIS COMPLETE!')
print('=' * 60)
print("""
📂 VISUALIZATIONS (results/mod_comments/):""")
print('   1. removal_categories_pie.png - Category distribution')
print('   2. top_removal_categories.png - Top 10 categories bar chart')
print('   3. heatmap_fingerprints.png ⭐ - Community moderation patterns')
print('   4. rule_violations.png - Most violated rules')
print('   5. comment_rate_by_subreddit.png - Subreddit comparison')
if metrics_available['toxicity']:
    print('   6. toxicity_by_category.png - Thread toxicity correlation')
if metrics_available['age']:
    print('   7. age_by_category.png - Account age correlation')
if metrics_available.get('title_toxicity', False) or metrics_available.get(
    'selftext_toxicity', False):
    print('   9. toxicity_by_category.png - Toxicity scores by removal reason')
if metrics_available.get('title_sentiment', False) or metrics_available.get(
    'selftext_sentiment', False):
    print(
        '   10. sentiment_by_category.png - Sentiment analysis by removal reason'
        )
if len(automod_comments) > 0 and len(human_mod_comments) > 0:
    print('   8. automod_vs_human.png - AutoMod vs Human moderators')
print('\n📊 CSV DATA:')
print('   • category_summary.csv - Category counts & percentages')
print('   • subreddit_fingerprints.csv - Community removal patterns matrix')
if len(metric_stats) > 0:
    print('   • removal_reason_metrics.csv - User metrics correlations')
if len(rule_numbers) > 0:
    print('   • rule_violations.csv - Rule violation counts')
if linguistic_cols:
    print(
        '   • linguistic_metrics_by_category.csv - Linguistic features by removal category'
        )
if len(automod_comments) > 0 and len(human_mod_comments) > 0:
    print(
        '   3. Compare AutoMod vs Human to understand moderation division of labor'
        )