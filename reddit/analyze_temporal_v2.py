import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob
import psutil
import argparse
import json
import warnings
from scipy import stats
from tqdm import tqdm
warnings.filterwarnings('ignore')
try:
    from lifelines import KaplanMeierFitter, CoxPHFitter
    from lifelines.statistics import logrank_test, multivariate_logrank_test
    LIFELINES_AVAILABLE = True
except ImportError:
    print('⚠️  lifelines not installed. Survival analysis will be limited.')
    print('   Install: pip install lifelines')
    LIFELINES_AVAILABLE = False
try:
    from statsmodels.stats.proportion import proportions_ztest
    from statsmodels.stats.multitest import multipletests
    STATSMODELS_AVAILABLE = True
except ImportError:
    print('⚠️  statsmodels not installed. Some statistical tests unavailable.')
    STATSMODELS_AVAILABLE = False
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = 14, 8
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
DATA_DIR = os.path.abspath(DATA_DIR)
parser = argparse.ArgumentParser(description='Enhanced temporal analysis')
parser.add_argument('--data-dir', type=str, default=DATA_DIR, help=
    f'Data directory (default: {DATA_DIR})')
parser.add_argument('--min-posts', type=int, default=50, help=
    'Minimum posts per subreddit for analysis (default: 50)')
parser.add_argument('--velocity-window', type=int, default=12, help=
    'Window for velocity calculations in hours (default: 12)')
parser.add_argument('--output-dir', type=str, default='results/temporal',
    help='Output directory (default: results/temporal)')
parser.add_argument('--force', action='store_true', help=
    'Force re-analysis even if output exists')
parser.add_argument('--no-memory-check', action='store_true', help=
    'Skip interactive memory check')
args = parser.parse_args()


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


def calculate_effect_size(group1, group2):
    n1, n2 = len(group1), len(group2)
    var1, var2 = group1.var(), group2.var()
    pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
    if pooled_std == 0:
        return 0
    return (group1.mean() - group2.mean()) / pooled_std


def calculate_confidence_interval(data, confidence=0.95):
    data = pd.Series(data).dropna()
    mean = data.mean()
    if len(data) < 2:
        return mean, mean
    sem = stats.sem(data)
    interval = sem * stats.t.ppf((1 + confidence) / 2.0, len(data) - 1)
    return mean - interval, mean + interval


if not args.no_memory_check and not check_memory(min_gb=2):
    exit(0)
print('=' * 70)
print('📊 ENHANCED TEMPORAL ANALYSIS')
print('=' * 70)
print(f"""
⚙️  Configuration:""")
print(f'   Data directory: {args.data_dir}')
print(f'   Min posts per subreddit: {args.min_posts}')
print(f'   Velocity window: {args.velocity_window}h')
print("""
📂 Looking for data files...""")
linguistics_file = f'{args.data_dir}/linguistics_boolean_fixed.csv'
if os.path.exists(linguistics_file):
    print(f'✓ Found linguistics data: {linguistics_file}')
    print('   (Chaining: Raw → Linguistics → Temporal)')
    df = pd.read_csv(linguistics_file)
else:
    print('⚠️  Linguistics data not found. Using raw Pass 2 data.')
    data_files = glob.glob(f'{args.data_dir}/master_pass2_combined*.csv')
    if not data_files:
        data_files = glob.glob('master_pass2_combined*.csv')
    if not data_files:
        print('❌ No data found. Run collect_pass2.py first.')
        exit(1)
    latest = max(data_files, key=os.path.getctime)
    df = pd.read_csv(latest)
    print(f'✓ Loaded: {latest}')
print(f'✓ Loaded {len(df):,} posts')
os.makedirs(args.output_dir, exist_ok=True)
output_file = f'{args.data_dir}/analyzed_temporal_patterns_enhanced.csv'
if os.path.exists(output_file) and not args.force:
    print(f'✓ Temporal analysis already exists: {output_file}')
    print('  Use --force to re-analyze')
    exit(0)
print('\n' + '=' * 70)
print('DATA VALIDATION & PREPROCESSING')
print('=' * 70)
required_cols = ['created_utc', 'subreddit']
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    print(f'❌ Missing required columns: {missing_cols}')
    exit(1)
if 'is_removed_inferred' not in df.columns:
    if 'is_removed' in df.columns:
        print('⚠️  is_removed_inferred missing, falling back to is_removed')
        df['is_removed_inferred'] = df['is_removed']
    else:
        print(
            "❌ Need either 'is_removed_inferred' or 'is_removed' in the dataset."
            )
        exit(1)
print("""
🕵️  DEBUGGING BAD TIMESTAMPS:""")
temp_numeric = pd.to_numeric(df['created_utc'], errors='coerce')
bad_rows = df[temp_numeric.isna()]
if len(bad_rows) > 0:
    print(f"   Found {len(bad_rows)} rows with non-numeric 'created_utc'")
    print('   Sample of bad values:')
    print(bad_rows[['subreddit', 'created_utc']].head(10))
    if 'created' in df.columns:
        print("   Note: 'created' column exists. We might be able to use that."
            )
print("""
🔍 Validating timestamps...""")
print("""
🧹 FORCE-CLEANING DATA TYPES...""")
for col in ['is_removed_inferred', 'is_removed', 't12_is_removed']:
    if col in df.columns:
        clean_col = df[col].astype(str).str.lower().str.strip()
        df[col] = clean_col.isin(['true', '1', '1.0', 'yes']).astype(int)
        print(f'   ✓ {col} forced to Integer (0/1)')
numeric_targets = ['initial_score', 't12_score', 'final_score',
    'initial_num_comments', 't12_num_comments', 'final_num_comments']
for col in numeric_targets:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
print('✓ Scores forced to Float')
for col in ['created_utc', 'removed_utc']:
    if col in df.columns:
        raw = df[col].copy()
        numeric = pd.to_numeric(raw, errors='coerce')
        df[col] = numeric
        dt = pd.to_datetime(numeric, unit='s', errors='coerce')
        mask_nat = dt.isna() & raw.notna()
        if mask_nat.any():
            dt.loc[mask_nat] = pd.to_datetime(raw[mask_nat], errors='coerce')
        dt_col_name = col.replace('_utc', '_dt')
        df[dt_col_name] = dt
if 'removed_dt' not in df.columns:
    df['removed_dt'] = pd.NaT
initial_count = len(df)
if 'created_dt' not in df.columns:
    print('❌ Failed to construct created_dt from created_utc')
    exit(1)
df = df[df['created_dt'].notna()].copy()
print(f'   ✓ Removed {initial_count - len(df)} posts with invalid timestamps')
if args.min_posts > 0:
    sub_counts = df['subreddit'].value_counts()
    valid_subs = sub_counts[sub_counts >= args.min_posts].index
    df = df[df['subreddit'].isin(valid_subs)].copy()
    print(
        f'   ✓ Filtered to {len(valid_subs)} subreddits with >={args.min_posts} posts'
        )
print(f'✓ {len(df):,} posts remaining after validation')
has_t12_data = 't12_is_removed' in df.columns and 't12_score' in df.columns
print(f"""
📊 Data coverage:""")
print(f"   T+12h data: {'✓ Available' if has_t12_data else '✗ Not available'}")
print(
    f"   Removal timestamps: {df['removed_dt'].notna().sum():,} / {len(df):,}")
print('\n' + '=' * 70)
print('TIME-TO-REMOVAL ANALYSIS')
print('=' * 70)
df['hours_to_removal'] = np.nan
df['removal_time_known'] = False
exact_removal = df['removed_dt'].notna()
df.loc[exact_removal, 'hours_to_removal'] = (df.loc[exact_removal,
    'removed_dt'] - df.loc[exact_removal, 'created_dt']) / pd.Timedelta(hours=1
    )
df.loc[exact_removal, 'removal_time_known'] = True
df.loc[df['hours_to_removal'] < 0, 'hours_to_removal'] = 0
print(
    f"""
✓ Exact removal times: {exact_removal.sum():,} posts ({exact_removal.sum() / len(df) * 100:.1f}%)"""
    )
if has_t12_data:
    early_removed = (df['t12_is_removed'] == True) & ~df['removal_time_known']
    df.loc[early_removed, 'removal_window_lower'] = 0
    df.loc[early_removed, 'removal_window_upper'] = 12
    mid_removed = (df['t12_is_removed'] == False) & (df[
        'is_removed_inferred'] == True) & ~df['removal_time_known']
    df.loc[mid_removed, 'removal_window_lower'] = 12
    df.loc[mid_removed, 'removal_window_upper'] = 48
    not_removed = df['is_removed_inferred'] == False
    df.loc[not_removed, 'removal_window_lower'] = 48
    df.loc[not_removed, 'removal_window_upper'] = np.inf
    print(f'✓ Interval-censored data:')
    print(
        f'   Early removal (0-12h): {early_removed.sum():,} ({early_removed.sum() / len(df) * 100:.1f}%)'
        )
    print(
        f'   Mid removal (12-48h): {mid_removed.sum():,} ({mid_removed.sum() / len(df) * 100:.1f}%)'
        )
    print(
        f'   Not removed by 48h: {not_removed.sum():,} ({not_removed.sum() / len(df) * 100:.1f}%)'
        )
else:
    print('⚠️  No T+12h data - using coarse intervals')
    removed_by_48h = df['is_removed_inferred'] == True
    df.loc[removed_by_48h & ~df['removal_time_known'], 'removal_window_lower'
        ] = 0
    df.loc[removed_by_48h & ~df['removal_time_known'], 'removal_window_upper'
        ] = 48
df['removal_window'] = 'not_removed'
if has_t12_data:
    df.loc[early_removed, 'removal_window'] = 'early_0-12h'
    df.loc[mid_removed, 'removal_window'] = 'mid_12-48h'
if LIFELINES_AVAILABLE:
    print('\n' + '=' * 70)
    print('SURVIVAL ANALYSIS')
    print('=' * 70)
    survival_df = df.copy()
    survival_df['duration'] = survival_df['hours_to_removal']
    if has_t12_data:
        mask_early = survival_df['duration'].isna() & (survival_df[
            'removal_window'] == 'early_0-12h')
        n_early = mask_early.sum()
        if n_early > 0:
            survival_df.loc[mask_early, 'duration'] = np.random.uniform(0.1,
                12.0, n_early)
        mask_mid = survival_df['duration'].isna() & (survival_df[
            'removal_window'] == 'mid_12-48h')
        n_mid = mask_mid.sum()
        if n_mid > 0:
            survival_df.loc[mask_mid, 'duration'] = np.random.uniform(12.0,
                48.0, n_mid)
    else:
        mask_unknown_removed = survival_df['duration'].isna() & (survival_df
            ['is_removed_inferred'] == 1)
        n_unknown_removed = mask_unknown_removed.sum()
        if n_unknown_removed > 0:
            survival_df.loc[mask_unknown_removed, 'duration'
                ] = np.random.uniform(0.1, 48.0, n_unknown_removed)
    survival_df['duration'] = survival_df['duration'].fillna(48.0)
    survival_df['event'] = survival_df['is_removed_inferred'].fillna(0).astype(
        int)
    survival_df = survival_df[survival_df['duration'] > 0].copy()
    print(f'\n📊 Survival analysis dataset:')
    print(f'   Total posts: {len(survival_df):,}')
    print(f"   Events (removals): {survival_df['event'].sum():,}")
    print(f"   Censored: {(1 - survival_df['event']).sum():,}")
    print('\n📈 Computing Kaplan-Meier survival curves...')
    kmf = KaplanMeierFitter()
    kmf.fit(survival_df['duration'], survival_df['event'], label='All Posts')
    survival_at_12h = kmf.survival_function_at_times(12).values[0]
    survival_at_24h = kmf.survival_function_at_times(24).values[0]
    survival_at_48h = kmf.survival_function_at_times(48).values[0]
    print(f'\n✓ Survival probabilities (not removed by):')
    print(f'   12 hours: {survival_at_12h:.1%}')
    print(f'   24 hours: {survival_at_24h:.1%}')
    print(f'   48 hours: {survival_at_48h:.1%}')
    median_survival = kmf.median_survival_time_
    print(f'   Median survival time: {median_survival:.1f} hours')
    print('\n📊 Computing survival curves by subreddit...')
    top_subreddits = survival_df['subreddit'].value_counts().head(10).index
    subreddit_survival = {}
    for sub in top_subreddits:
        sub_data = survival_df[survival_df['subreddit'] == sub]
        kmf_sub = KaplanMeierFitter()
        kmf_sub.fit(sub_data['duration'], sub_data['event'], label=sub)
        subreddit_survival[sub] = {'kmf': kmf_sub, 'median': kmf_sub.
            median_survival_time_, 'survival_12h': kmf_sub.
            survival_function_at_times(12).values[0]}
    print('\n🧪 Testing for differences between subreddits...')
    top_k = 5
    if len(top_subreddits) >= 2:
        print('   Running multivariate log-rank test (top 5 subreddits)...')
        mask = survival_df['subreddit'].isin(top_subreddits[:top_k])
        sub_surv = survival_df[mask].copy()
        try:
            results = multivariate_logrank_test(event_durations=sub_surv[
                'duration'], groups=sub_surv['subreddit'], event_observed=
                sub_surv['event'])
            print(f'   Chi-square: {results.test_statistic:.2f}')
            print(f'   p-value: {results.p_value:.4f}')
            print(
                f"   Result: Subreddit {'DOES' if results.p_value < 0.05 else 'does NOT'} significantly affect removal timing"
                )
        except Exception as e:
            print(f'   ⚠️  Log-rank test failed: {e}')
            print('   Trying pairwise log-rank tests...')
            try:
                from lifelines.statistics import logrank_test
                sub_list = list(top_subreddits[:top_k])
                significant_pairs = []
                for i in range(len(sub_list)):
                    for j in range(i + 1, len(sub_list)):
                        sub1_data = sub_surv[sub_surv['subreddit'] ==
                            sub_list[i]]
                        sub2_data = sub_surv[sub_surv['subreddit'] ==
                            sub_list[j]]
                        result_pair = logrank_test(sub1_data['duration'],
                            sub2_data['duration'], event_observed_A=
                            sub1_data['event'], event_observed_B=sub2_data[
                            'event'])
                        if result_pair.p_value < 0.05:
                            significant_pairs.append((sub_list[i], sub_list
                                [j], result_pair.p_value))
                if significant_pairs:
                    print(
                        f'   Found {len(significant_pairs)} significantly different subreddit pairs:'
                        )
                    for sub1, sub2, p_val in significant_pairs[:3]:
                        print(f'      {sub1} vs {sub2}: p={p_val:.4f}')
                else:
                    print(
                        '   No significant differences found in pairwise tests'
                        )
            except Exception as e2:
                print(f'   ⚠️  Pairwise tests also failed: {e2}')
    else:
        print('   Not enough subreddits for log-rank test.')
    if has_t12_data and 't12_score' in df.columns:
        print('\n📈 Fitting Cox Proportional Hazards model...')
        print(
            '   ℹ️  Filtering for conditional survival (posts > 12h) to avoid lookahead bias...'
            )
        cox_df = survival_df[survival_df['duration'] > 12].copy()
        cox_df = cox_df[['duration', 'event', 'initial_score', 't12_score',
            'subreddit']]
        cox_df['initial_score'] = pd.to_numeric(cox_df['initial_score'],
            errors='coerce')
        cox_df['t12_score'] = pd.to_numeric(cox_df['t12_score'], errors=
            'coerce')
        cox_df = cox_df.dropna(subset=['duration', 'event', 'initial_score',
            't12_score'])
        if len(cox_df) < 10:
            print('⚠️  Not enough data for Cox model after cleaning. Skipping.'
                )
        else:
            cox_df['initial_score_norm'] = (cox_df['initial_score'] -
                cox_df['initial_score'].mean()) / (cox_df['initial_score'].
                std() + 0.1)
            cox_df['t12_score_norm'] = (cox_df['t12_score'] - cox_df[
                't12_score'].mean()) / (cox_df['t12_score'].std() + 0.1)
            corr = cox_df[['initial_score_norm', 't12_score_norm']].corr(
                ).iloc[0, 1]
            if abs(corr) > 0.98:
                print(
                    f'   ⚠️ High collinearity between initial and t12 scores (r={corr:.3f}).'
                    )
                print('   → Fitting Cox model with initial_score_norm only.')
                covariate_cols = ['duration', 'event', 'initial_score_norm']
            else:
                covariate_cols = ['duration', 'event', 'initial_score_norm',
                    't12_score_norm']
            cph = CoxPHFitter(penalizer=0.1)
            try:
                cph.fit(cox_df[covariate_cols], duration_col='duration',
                    event_col='event')
                print('\n✓ Cox model fitted')
                print('\n   Hazard ratios:')
                if 'initial_score_norm' in cph.params_.index:
                    print(
                        f"   Initial score: {np.exp(cph.params_['initial_score_norm']):.3f}"
                        )
                if 't12_score_norm' in cph.params_.index:
                    print(
                        f"   T12 score: {np.exp(cph.params_['t12_score_norm']):.3f}"
                        )
                print('   (HR > 1 = increased removal hazard)')
            except Exception as e:
                print(f'   ⚠️  Cox model failed: {e}')
else:
    print('\n⚠️  Survival analysis skipped (install lifelines)')
print('\n' + '=' * 70)
print('SUBREDDIT-NORMALIZED METRICS')
print('=' * 70)
print("""
🔧 Computing baseline statistics per subreddit...""")
survivors = df[df['is_removed_inferred'] == 0]
subreddit_stats = survivors.groupby('subreddit').agg({'initial_score': [
    'mean', 'std'], 'final_score': ['mean', 'std'], 'initial_num_comments':
    ['mean', 'std'], 'final_num_comments': ['mean', 'std']}).round(3)
subreddit_stats.columns = ['_'.join(col).strip() for col in subreddit_stats
    .columns.values]
stats_full = df.groupby('subreddit').agg({'is_removed_inferred': 'mean',
    'subreddit': 'size'})
stats_full = stats_full.rename(columns={'is_removed_inferred':
    'removal_rate', 'subreddit': 'post_count'})
subreddit_stats = subreddit_stats.join(stats_full)
for col in subreddit_stats.columns:
    if col.endswith('_std'):
        subreddit_stats[col] = subreddit_stats[col].fillna(0.0)
print(f'✓ Computed baselines for {len(subreddit_stats)} subreddits')
df = df.merge(subreddit_stats, left_on='subreddit', right_index=True, how=
    'left', suffixes=('', '_sub_baseline'))
print("""
📊 Calculating normalized scores...""")
df['initial_score_z'] = (df['initial_score'] - df['initial_score_mean']) / (df
    ['initial_score_std'] + 0.1)
df['final_score_z'] = (df['final_score'] - df['final_score_mean']) / (df[
    'final_score_std'] + 0.1)
df['initial_comments_z'] = (df['initial_num_comments'] - df[
    'initial_num_comments_mean']) / (df['initial_num_comments_std'] + 0.1)
print('✓ Normalized metrics computed (Z-scores)')
print('\n' + '=' * 70)
print('VELOCITY METRICS')
print('=' * 70)
if has_t12_data:
    print(
        f'\n📈 Computing velocity metrics (using {args.velocity_window}h window)...'
        )
    w = args.velocity_window
    second_leg_hours = max(1, 48 - w)
    active_duration_t12 = df['hours_to_removal'].fillna(w).clip(upper=w)
    active_duration_t12 = active_duration_t12.replace(0, 0.1)
    df['score_velocity_t0_t12'] = (df['t12_score'] - df['initial_score']
        ) / active_duration_t12
    df['score_velocity_t12_t48'] = (df['final_score'] - df['t12_score']
        ) / second_leg_hours
    df['comments_velocity_t0_t12'] = (df['t12_num_comments'] - df[
        'initial_num_comments']) / active_duration_t12
    df['comments_velocity_t12_t48'] = (df['final_num_comments'] - df[
        't12_num_comments']) / second_leg_hours
    df['score_acceleration'] = df['score_velocity_t12_t48'] - df[
        'score_velocity_t0_t12']
    df['comments_acceleration'] = df['comments_velocity_t12_t48'] - df[
        'comments_velocity_t0_t12']
    print(f'✓ Velocity metrics computed:')
    print(
        f"   Mean score velocity (T0→T{w}): {df['score_velocity_t0_t12'].mean():.2f} pts/hr"
        )
    print(
        f"   Mean score velocity (T{w}→T48): {df['score_velocity_t12_t48'].mean():.2f} pts/hr"
        )
    print(
        f"   Mean comments velocity (T0→T{w}): {df['comments_velocity_t0_t12'].mean():.2f} comments/hr"
        )
else:
    print('⚠️  No T+12h data - velocity metrics limited')
    df['score_velocity_overall'] = (df['final_score'] - df['initial_score']
        ) / 48
print('\n' + '=' * 70)
print('TEMPORAL PATTERNS')
print('=' * 70)
print("""
🕐 Analyzing time-of-day patterns...""")
df['created_hour'] = df['created_dt'].dt.hour
df['created_day_of_week'] = df['created_dt'].dt.dayofweek
df['created_day_name'] = df['created_dt'].dt.day_name()
if df['removed_dt'].notna().sum() > 100:
    removed_posts = df[df['removed_dt'].notna()].copy()
    removed_posts['removed_hour'] = removed_posts['removed_dt'].dt.hour
    removal_by_hour = removed_posts.groupby('removed_hour').size()
    post_by_hour = df.groupby('created_hour').size()
    print(f'✓ Time-of-day patterns computed')
    print(f'   Peak posting hour: {post_by_hour.idxmax()}:00')
    print(f'   Peak removal hour: {removal_by_hour.idxmax()}:00')
else:
    print('⚠️  Insufficient removal timestamps for time-of-day analysis')
removal_by_dow = df.groupby('created_day_name')['is_removed_inferred'].agg([
    'sum', 'mean'])
removal_by_dow = removal_by_dow.reindex(['Monday', 'Tuesday', 'Wednesday',
    'Thursday', 'Friday', 'Saturday', 'Sunday'])
print(f"""
📅 Day-of-week patterns:""")
print(
    f"   Highest removal rate: {removal_by_dow['mean'].idxmax()} ({removal_by_dow['mean'].max():.1%})"
    )
print(
    f"   Lowest removal rate: {removal_by_dow['mean'].idxmin()} ({removal_by_dow['mean'].min():.1%})"
    )
print('\n' + '=' * 70)
print('GROWING-THEN-REMOVED DETECTION')
print('=' * 70)
if has_t12_data:
    print('\n🔍 Detecting posts that grew significantly then were removed...')
    growing_then_removed = df[((df['t12_score'] - df['initial_score_mean']) /
        (df['initial_score_std'] + 0.1) > 0.5) & (df[
        'score_velocity_t0_t12'] > 0) & (df['t12_is_removed'] == False) & (
        df['is_removed_inferred'] == True)].copy()
    growing_then_removed = growing_then_removed[growing_then_removed[
        't12_score'] > growing_then_removed['initial_score_mean']]
    print(f'\n✓ Posts that grew, then were removed:')
    print(
        f'   Count: {len(growing_then_removed):,} ({len(growing_then_removed) / len(df) * 100:.2f}%)'
        )
    if len(growing_then_removed) > 0:
        print(
            f"   Avg score at T12: {growing_then_removed['t12_score'].mean():.0f}"
            )
        print(
            f"   Avg score velocity: {growing_then_removed['score_velocity_t0_t12'].mean():.2f} pts/hr"
            )
        print(
            f"   Avg comments at T12: {growing_then_removed.get('t12_num_comments', pd.Series([0])).mean():.0f}"
            )
        print(f'\n   Top 5 subreddits with this pattern:')
        top_subs = growing_then_removed['subreddit'].value_counts().head()
        for sub, count in top_subs.items():
            total_sub = len(df[df['subreddit'] == sub])
            print(
                f"      {sub}: {count} ({count / total_sub * 100:.1f}% of sub's posts)"
                )
    df['is_growing_then_removed'] = False
    df.loc[growing_then_removed.index, 'is_growing_then_removed'] = True
else:
    print('⚠️  No T+12h data - cannot detect growing-then-removed pattern')
linguistic_features_available = {'title_toxicity': 'title_toxicity' in df.
    columns, 'selftext_toxicity': 'selftext_toxicity' in df.columns,
    'title_sentiment': 'title_sentiment_polarity' in df.columns,
    'selftext_sentiment': 'selftext_sentiment_polarity' in df.columns,
    'title_readability': 'title_readability_flesch_reading_ease' in df.columns}
linguistic_cols = []
for feat, available in linguistic_features_available.items():
    if available:
        if feat == 'title_toxicity':
            linguistic_cols.append('title_toxicity')
        elif feat == 'selftext_toxicity':
            linguistic_cols.append('selftext_toxicity')
        elif feat == 'title_sentiment':
            linguistic_cols.append('title_sentiment_polarity')
        elif feat == 'selftext_sentiment':
            linguistic_cols.append('selftext_sentiment_polarity')
        elif feat == 'title_readability':
            linguistic_cols.append('title_readability_flesch_reading_ease')
if linguistic_cols:
    print('\n' + '=' * 70)
    print('LINGUISTIC PATTERNS BY REMOVAL TIMING')
    print('=' * 70)
    print(f'\n📊 Analyzing {len(linguistic_cols)} linguistic features...')
    linguistic_by_window_df = pd.DataFrame()
    if has_t12_data:
        linguistic_by_window = []
        for window in ['early_0-12h', 'mid_12-48h', 'not_removed']:
            window_posts = df[df['removal_window'] == window]
            if len(window_posts) == 0:
                continue
            window_stats = {'removal_window': window, 'n_posts': len(
                window_posts)}
            for col in linguistic_cols:
                if col in df.columns:
                    window_stats[f'{col}_mean'] = window_posts[col].mean()
                    window_stats[f'{col}_median'] = window_posts[col].median()
            linguistic_by_window.append(window_stats)
        linguistic_by_window_df = pd.DataFrame(linguistic_by_window)
        print('\n📋 Linguistic features by removal timing:')
        print(linguistic_by_window_df.to_string(index=False))
        print(
            '\n🧪 Testing for differences in linguistic features by removal timing...'
            )
        for col in linguistic_cols:
            if col in df.columns:
                early = df[df['removal_window'] == 'early_0-12h'][col].dropna()
                mid = df[df['removal_window'] == 'mid_12-48h'][col].dropna()
                not_removed = df[df['removal_window'] == 'not_removed'][col
                    ].dropna()
                if len(early) > 30 and len(not_removed) > 30:
                    t_stat, p_val = stats.ttest_ind(early, not_removed,
                        equal_var=False)
                    effect = calculate_effect_size(early, not_removed)
                    print(f'\n   {col}:')
                    print(f'      Early removal mean: {early.mean():.4f}')
                    print(f'      Not removed mean: {not_removed.mean():.4f}')
                    print(
                        f'      t-statistic: {t_stat:.4f}, p-value: {p_val:.4f}'
                        )
                    print(f"      Effect size (Cohen's d): {effect:.4f}")
                    if p_val < 0.05:
                        direction = 'higher' if early.mean(
                            ) > not_removed.mean() else 'lower'
                        print(
                            f'      ✓ Early-removed posts have significantly {direction} {col}'
                            )
    else:
        removed = df[df['is_removed_inferred'] == True]
        not_removed = df[df['is_removed_inferred'] == False]
        print('\n📋 Linguistic features comparison:')
        print(f'   Removed posts: {len(removed):,}')
        print(f'   Not removed posts: {len(not_removed):,}')
        for col in linguistic_cols:
            if col in df.columns:
                removed_mean = removed[col].mean()
                not_removed_mean = not_removed[col].mean()
                print(f'\n   {col}:')
                print(f'      Removed: {removed_mean:.4f}')
                print(f'      Not removed: {not_removed_mean:.4f}')
                print(
                    f'      Difference: {removed_mean - not_removed_mean:+.4f}'
                    )
else:
    print('\n⚠️  No linguistic features available for temporal analysis')
print('\n' + '=' * 70)
print('BATCH REMOVAL DETECTION')
print('=' * 70)
if df['removed_dt'].notna().sum() > 100:
    print('\n🔍 Detecting potential batch removals (Vectorized)...')
    removed_with_time = df[df['removed_dt'].notna()].copy()
    removed_with_time = removed_with_time.sort_values(['subreddit',
        'removed_dt'])
    prev_sub = removed_with_time['subreddit'].shift()
    prev_time = removed_with_time['removed_dt'].shift()
    is_new_batch = (removed_with_time['subreddit'] != prev_sub) | (
        removed_with_time['removed_dt'] - prev_time > pd.Timedelta(minutes=5))
    removed_with_time['batch_id'] = is_new_batch.cumsum()
    batch_counts = removed_with_time['batch_id'].value_counts()
    large_batch_ids = batch_counts[batch_counts >= 3].index
    batch_posts_mask = removed_with_time['batch_id'].isin(large_batch_ids)
    batch_post_indices = removed_with_time[batch_posts_mask].index
    df['is_batch_removal'] = False
    df.loc[batch_post_indices, 'is_batch_removal'] = True
    print(f'✓ Detected {len(large_batch_ids)} batch events')
    print(f'   Total posts in batches: {len(batch_post_indices)}')
else:
    print('⚠️  Insufficient removal timestamps for batch detection')
print('\n' + '=' * 70)
print('STATISTICAL HYPOTHESIS TESTING')
print('=' * 70)
print("""
🧪 Running statistical tests...""")
removed = df[df['is_removed_inferred'] == True]
not_removed = df[df['is_removed_inferred'] == False]
if len(removed) > 30 and len(not_removed) > 30:
    removed_init = removed['initial_score'].dropna()
    not_removed_init = not_removed['initial_score'].dropna()
    removed_final = removed['final_score'].dropna()
    not_removed_final = not_removed['final_score'].dropna()
    if len(removed_init) >= 2 and len(not_removed_init) >= 2:
        t_stat_init, p_val_init = stats.ttest_ind(removed_init,
            not_removed_init)
        effect_size_init = calculate_effect_size(removed_init, not_removed_init
            )
        print(f'\n1. Initial Score Comparison:')
        print(
            f'   Removed: {removed_init.mean():.2f} ± {removed_init.std():.2f}'
            )
        print(
            f'   Not Removed: {not_removed_init.mean():.2f} ± {not_removed_init.std():.2f}'
            )
        print(f'   t-statistic: {t_stat_init:.3f}, p-value: {p_val_init:.4f}')
        print(f"   Effect size (Cohen's d): {effect_size_init:.3f}")
        print(
            f"   Significance: {'***' if p_val_init < 0.001 else '**' if p_val_init < 0.01 else '*' if p_val_init < 0.05 else 'ns'}"
            )
    else:
        print(
            '\n1. Initial Score Comparison: skipped (too few non-missing values).'
            )
    if len(removed_final) >= 2 and len(not_removed_final) >= 2:
        t_stat_final, p_val_final = stats.ttest_ind(removed_final,
            not_removed_final)
        effect_size_final = calculate_effect_size(removed_final,
            not_removed_final)
        print(f'\n2. Final Score Comparison:')
        print(
            f'   Removed: {removed_final.mean():.2f} ± {removed_final.std():.2f}'
            )
        print(
            f'   Not Removed: {not_removed_final.mean():.2f} ± {not_removed_final.std():.2f}'
            )
        print(f'   t-statistic: {t_stat_final:.3f}, p-value: {p_val_final:.4f}'
            )
        print(f"   Effect size (Cohen's d): {effect_size_final:.3f}")
        print(
            f"   Significance: {'***' if p_val_final < 0.001 else '**' if p_val_final < 0.01 else '*' if p_val_final < 0.05 else 'ns'}"
            )
    else:
        print(
            '\n2. Final Score Comparison: skipped (too few non-missing values).'
            )
if len(df['subreddit'].unique()) > 1:
    print(f'\n3. Removal Rate Variance Across Subreddits:')
    contingency_table = pd.crosstab(df['subreddit'], df['is_removed_inferred'])
    chi2, p_val_chi, dof, expected = stats.chi2_contingency(contingency_table)
    print(f'   Chi-square: {chi2:.2f}, df: {dof}, p-value: {p_val_chi:.4f}')
    print(
        f"   Significance: {'***' if p_val_chi < 0.001 else '**' if p_val_chi < 0.01 else '*' if p_val_chi < 0.05 else 'ns'}"
        )
    print(
        f"   Interpretation: Removal rates {'DO' if p_val_chi < 0.05 else 'do NOT'} vary significantly by subreddit"
        )
if has_t12_data and 'score_velocity_t0_t12' in df.columns:
    removed_vel = removed['score_velocity_t0_t12'].dropna()
    not_removed_vel = not_removed['score_velocity_t0_t12'].dropna()
    if len(removed_vel) > 30 and len(not_removed_vel) > 30:
        t_stat_vel, p_val_vel = stats.ttest_ind(removed_vel, not_removed_vel)
        effect_size_vel = calculate_effect_size(removed_vel, not_removed_vel)
        print(f'\n4. Score Velocity Comparison (T0→T12):')
        print(
            f'   Removed: {removed_vel.mean():.3f} ± {removed_vel.std():.3f} pts/hr'
            )
        print(
            f'   Not Removed: {not_removed_vel.mean():.3f} ± {not_removed_vel.std():.3f} pts/hr'
            )
        print(f'   t-statistic: {t_stat_vel:.3f}, p-value: {p_val_vel:.4f}')
        print(f"   Effect size (Cohen's d): {effect_size_vel:.3f}")
        print(
            f"   Significance: {'***' if p_val_vel < 0.001 else '**' if p_val_vel < 0.01 else '*' if p_val_vel < 0.05 else 'ns'}"
            )
    else:
        print(
            '\n4. Score Velocity Comparison (T0→T12): skipped (too few non-missing values).'
            )
p_values = []
test_names = []
if 'p_val_init' in locals():
    p_values.append(p_val_init)
    test_names.append('Initial Score')
if 'p_val_final' in locals():
    p_values.append(p_val_final)
    test_names.append('Final Score')
if 'p_val_chi' in locals():
    p_values.append(p_val_chi)
    test_names.append('Chi-square')
if 'p_val_vel' in locals():
    p_values.append(p_val_vel)
    test_names.append('Velocity')
if STATSMODELS_AVAILABLE and len(p_values) >= 2:
    rejected, corrected_p, alpha_sidak, alpha_bonf = multipletests(p_values,
        alpha=0.05, method='bonferroni')
    print(f'\n5. Multiple Comparison Correction (Bonferroni):')
    for name, orig_p, corr_p, is_sig in zip(test_names, p_values,
        corrected_p, rejected):
        print(
            f"   {name}: p={orig_p:.4f} → corrected p={corr_p:.4f} {'✓ significant' if is_sig else '✗ not significant'}"
            )
elif not STATSMODELS_AVAILABLE:
    print(
        '\n5. Multiple Comparison Correction (Bonferroni): skipped (statsmodels not available)'
        )
else:
    print(
        '\n5. Multiple Comparison Correction (Bonferroni): skipped (not enough tests)'
        )
print('\n' + '=' * 70)
print('CREATING VISUALIZATIONS')
print('=' * 70)
viz_dir = f'{args.output_dir}/visualizations'
os.makedirs(viz_dir, exist_ok=True)
if LIFELINES_AVAILABLE:
    print('\n📊 Creating survival curve plots...')
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax1 = axes[0]
    kmf.plot(ax=ax1, ci_show=True)
    ax1.set_xlabel('Hours Since Creation', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Survival Probability (Not Removed)', fontsize=12,
        fontweight='bold')
    ax1.set_title('Overall Survival Curve', fontsize=14, fontweight='bold')
    ax1.grid(alpha=0.3)
    ax1.axhline(0.5, color='red', linestyle='--', alpha=0.5, label=
        '50% survival')
    ax1.legend()
    ax2 = axes[1]
    for i, (sub, data) in enumerate(list(subreddit_survival.items())[:5]):
        data['kmf'].plot(ax=ax2, ci_show=False, label=sub)
    ax2.set_xlabel('Hours Since Creation', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Survival Probability', fontsize=12, fontweight='bold')
    ax2.set_title('Survival Curves by Subreddit (Top 5)', fontsize=14,
        fontweight='bold')
    ax2.grid(alpha=0.3)
    ax2.legend()
    plt.tight_layout()
    plt.savefig(f'{viz_dir}/survival_curves.png', dpi=300, bbox_inches='tight')
    print('   ✓ Saved: survival_curves.png')
    plt.close()
if has_t12_data:
    print('\n📈 Creating aggregate score trajectory plots...')
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    def plot_aggregate_trajectory(ax, data, label, color):
        if len(data) == 0:
            return
        times = [0, 12, 48]
        means = [data['initial_score'].mean(), data['t12_score'].mean(),
            data['final_score'].mean()]
        ci_lower = [calculate_confidence_interval(data['initial_score'])[0],
            calculate_confidence_interval(data['t12_score'])[0],
            calculate_confidence_interval(data['final_score'])[0]]
        ci_upper = [calculate_confidence_interval(data['initial_score'])[1],
            calculate_confidence_interval(data['t12_score'])[1],
            calculate_confidence_interval(data['final_score'])[1]]
        ax.plot(times, means, color=color, linewidth=3, label=label, marker
            ='o', markersize=8)
        ax.fill_between(times, ci_lower, ci_upper, color=color, alpha=0.2)
    ax1 = axes[0]
    removed_clean = removed.dropna(subset=['initial_score', 't12_score',
        'final_score'])
    plot_aggregate_trajectory(ax1, removed_clean, 'Removed', 'red')
    ax1.set_xlabel('Hours After Creation', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Score (Mean ± 95% CI)', fontsize=12, fontweight='bold')
    ax1.set_title('Score Trajectory: Removed Posts', fontsize=14,
        fontweight='bold')
    ax1.grid(alpha=0.3)
    ax1.legend()
    ax2 = axes[1]
    not_removed_clean = not_removed.dropna(subset=['initial_score',
        't12_score', 'final_score'])
    plot_aggregate_trajectory(ax2, not_removed_clean, 'Not Removed', 'green')
    ax2.set_xlabel('Hours After Creation', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Score (Mean ± 95% CI)', fontsize=12, fontweight='bold')
    ax2.set_title('Score Trajectory: Not Removed Posts', fontsize=14,
        fontweight='bold')
    ax2.grid(alpha=0.3)
    ax2.legend()
    plt.tight_layout()
    plt.savefig(f'{viz_dir}/score_trajectories_aggregate.png', dpi=300,
        bbox_inches='tight')
    print('   ✓ Saved: score_trajectories_aggregate.png')
    plt.close()
print("""
🔥 Creating time-of-day heatmap...""")
removal_by_hour_dow = df.groupby(['created_hour', 'created_day_of_week'])[
    'is_removed_inferred'].mean() * 100
pivot_hour_dow = removal_by_hour_dow.unstack(fill_value=0)
fig, ax = plt.subplots(figsize=(14, 8))
sns.heatmap(pivot_hour_dow.T, annot=True, fmt='.1f', cmap='YlOrRd',
    linewidths=0.5, cbar_kws={'label': 'Removal Rate (%)'}, ax=ax)
ax.set_xlabel('Hour of Day (UTC)', fontsize=12, fontweight='bold')
ax.set_ylabel('Day of Week', fontsize=12, fontweight='bold')
ax.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
    rotation=0)
ax.set_title('Removal Rate by Time of Day and Day of Week', fontsize=14,
    fontweight='bold')
plt.tight_layout()
plt.savefig(f'{viz_dir}/removal_heatmap_time.png', dpi=300, bbox_inches='tight'
    )
print('   ✓ Saved: removal_heatmap_time.png')
plt.close()
if has_t12_data and 'score_velocity_t0_t12' in df.columns:
    print('\n📊 Creating velocity distribution plots...')
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    ax1 = axes[0, 0]
    removed_vel = removed['score_velocity_t0_t12'].dropna()
    not_removed_vel = not_removed['score_velocity_t0_t12'].dropna()
    ax1.hist(removed_vel, bins=50, alpha=0.5, color='red', label='Removed',
        density=True)
    ax1.hist(not_removed_vel, bins=50, alpha=0.5, color='green', label=
        'Not Removed', density=True)
    ax1.set_xlabel('Score Velocity (pts/hr)', fontsize=10, fontweight='bold')
    ax1.set_ylabel('Density', fontsize=10, fontweight='bold')
    ax1.set_title('Score Velocity Distribution (T0→T12)', fontsize=12,
        fontweight='bold')
    ax1.legend()
    ax1.grid(alpha=0.3)
    ax1.set_xlim(-5, 20)
    ax2 = axes[0, 1]
    removed_cvel = removed['comments_velocity_t0_t12'].dropna()
    not_removed_cvel = not_removed['comments_velocity_t0_t12'].dropna()
    ax2.hist(removed_cvel, bins=50, alpha=0.5, color='red', label='Removed',
        density=True)
    ax2.hist(not_removed_cvel, bins=50, alpha=0.5, color='green', label=
        'Not Removed', density=True)
    ax2.set_xlabel('Comments Velocity (comments/hr)', fontsize=10,
        fontweight='bold')
    ax2.set_ylabel('Density', fontsize=10, fontweight='bold')
    ax2.set_title('Comments Velocity Distribution (T0→T12)', fontsize=12,
        fontweight='bold')
    ax2.legend()
    ax2.grid(alpha=0.3)
    ax2.set_xlim(0, 2)
    ax3 = axes[1, 0]
    removed_acc = removed['score_acceleration'].dropna()
    not_removed_acc = not_removed['score_acceleration'].dropna()
    ax3.hist(removed_acc, bins=50, alpha=0.5, color='red', label='Removed',
        density=True)
    ax3.hist(not_removed_acc, bins=50, alpha=0.5, color='green', label=
        'Not Removed', density=True)
    ax3.set_xlabel('Score Acceleration (Δ velocity)', fontsize=10,
        fontweight='bold')
    ax3.set_ylabel('Density', fontsize=10, fontweight='bold')
    ax3.set_title('Score Acceleration Distribution', fontsize=12,
        fontweight='bold')
    ax3.legend()
    ax3.grid(alpha=0.3)
    ax3.set_xlim(-2, 2)
    ax4 = axes[1, 1]
    sample_removed = removed.sample(min(1000, len(removed)), random_state=42)
    sample_not_removed = not_removed.sample(min(1000, len(not_removed)),
        random_state=42)
    ax4.scatter(sample_not_removed['score_velocity_t0_t12'],
        sample_not_removed['comments_velocity_t0_t12'], alpha=0.3, color=
        'green', s=10, label='Not Removed')
    ax4.scatter(sample_removed['score_velocity_t0_t12'], sample_removed[
        'comments_velocity_t0_t12'], alpha=0.3, color='red', s=10, label=
        'Removed')
    ax4.set_xlabel('Score Velocity (pts/hr)', fontsize=10, fontweight='bold')
    ax4.set_ylabel('Comments Velocity (comments/hr)', fontsize=10,
        fontweight='bold')
    ax4.set_title('Score vs Comments Velocity', fontsize=12, fontweight='bold')
    ax4.legend()
    ax4.grid(alpha=0.3)
    ax4.set_xlim(-5, 20)
    ax4.set_ylim(0, 2)
    plt.tight_layout()
    plt.savefig(f'{viz_dir}/velocity_distributions.png', dpi=300,
        bbox_inches='tight')
    print('   ✓ Saved: velocity_distributions.png')
    plt.close()
print("""
📊 Creating subreddit comparison plots...""")
top_10_subs = df['subreddit'].value_counts().head(10).index
sub_comparison = df[df['subreddit'].isin(top_10_subs)].groupby('subreddit'
    ).agg({'is_removed_inferred': 'mean', 'initial_score': 'mean',
    'final_score': 'mean'}).sort_values('is_removed_inferred', ascending=False)
fig, axes = plt.subplots(1, 3, figsize=(18, 6))
ax1 = axes[0]
bars = ax1.barh(range(len(sub_comparison)), sub_comparison[
    'is_removed_inferred'] * 100, color='coral', edgecolor='black')
ax1.set_yticks(range(len(sub_comparison)))
ax1.set_yticklabels(sub_comparison.index)
ax1.set_xlabel('Removal Rate (%)', fontsize=12, fontweight='bold')
ax1.set_title('Removal Rate by Subreddit', fontsize=14, fontweight='bold')
ax1.grid(axis='x', alpha=0.3)
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax1.text(width + 0.5, bar.get_y() + bar.get_height() / 2,
        f'{width:.1f}%', ha='left', va='center', fontsize=9)
ax2 = axes[1]
ax2.barh(range(len(sub_comparison)), sub_comparison['initial_score'], color
    ='steelblue', edgecolor='black')
ax2.set_yticks(range(len(sub_comparison)))
ax2.set_yticklabels(sub_comparison.index)
ax2.set_xlabel('Mean Initial Score', fontsize=12, fontweight='bold')
ax2.set_title('Mean Initial Score by Subreddit', fontsize=14, fontweight='bold'
    )
ax2.grid(axis='x', alpha=0.3)
ax3 = axes[2]
ax3.barh(range(len(sub_comparison)), sub_comparison['final_score'], color=
    'forestgreen', edgecolor='black')
ax3.set_yticks(range(len(sub_comparison)))
ax3.set_yticklabels(sub_comparison.index)
ax3.set_xlabel('Mean Final Score', fontsize=12, fontweight='bold')
ax3.set_title('Mean Final Score by Subreddit', fontsize=14, fontweight='bold')
ax3.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(f'{viz_dir}/subreddit_comparison.png', dpi=300, bbox_inches='tight'
    )
print('   ✓ Saved: subreddit_comparison.png')
plt.close()
if has_t12_data and 'is_growing_then_removed' in df.columns and df[
    'is_growing_then_removed'].sum() > 0:
    print('\n📊 Creating growing-then-removed visualization...')
    gtr = df[df['is_growing_then_removed'] == True]
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax1 = axes[1]
    plot_aggregate_trajectory(ax1, gtr, 'Growing Then Removed', 'orange')
    ax1.set_xlabel('Hours After Creation', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Score (Mean ± 95% CI)', fontsize=12, fontweight='bold')
    ax1.set_title('Score Trajectory: Growing-Then-Removed Posts', fontsize=
        14, fontweight='bold')
    ax1.grid(alpha=0.3)
    ax1.legend()
    ax2 = axes[0]
    gtr_by_sub = gtr['subreddit'].value_counts().head(10)
    ax2.barh(range(len(gtr_by_sub)), gtr_by_sub.values, color='orange',
        edgecolor='black')
    ax2.set_yticks(range(len(gtr_by_sub)))
    ax2.set_yticklabels(gtr_by_sub.index)
    ax2.set_xlabel('Count', fontsize=12, fontweight='bold')
    ax2.set_title('Growing-Then-Removed by Subreddit', fontsize=14,
        fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{viz_dir}/growing_then_removed.png', dpi=300, bbox_inches
        ='tight')
    print('   ✓ Saved: growing_then_removed.png')
    plt.close()
print(f"""
✓ All visualizations saved to: {viz_dir}/""")
if linguistic_cols and has_t12_data:
    print('\n📊 Creating linguistic temporal visualizations...')
    viz_dir = f'{args.output_dir}/visualizations'
    if len(linguistic_by_window_df) > 0:
        fig, axes = plt.subplots(1, len(linguistic_cols[:3]), figsize=(18, 5))
        if len(linguistic_cols[:3]) == 1:
            axes = [axes]
        for idx, col in enumerate(linguistic_cols[:3]):
            mean_col = f'{col}_mean'
            if mean_col in linguistic_by_window_df.columns:
                ax = axes[idx]
                windows = linguistic_by_window_df['removal_window']
                values = linguistic_by_window_df[mean_col]
                colors = [('red' if 'early' in w else 'orange' if 'mid' in
                    w else 'green') for w in windows]
                ax.bar(range(len(windows)), values, color=colors, alpha=0.7,
                    edgecolor='black')
                ax.set_xticks(range(len(windows)))
                ax.set_xticklabels(windows, rotation=45, ha='right')
                ax.set_ylabel('Mean Value')
                ax.set_title(
                    f"{col.replace('_', ' ').title()}\nby Removal Timing")
                ax.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/linguistic_by_removal_window.png', dpi=300,
            bbox_inches='tight')
        print('   ✓ Saved: linguistic_by_removal_window.png')
        plt.close()
    if 'title_toxicity' in df.columns and df['removal_time_known'].sum() > 100:
        fig, ax = plt.subplots(figsize=(10, 6))
        exact_removals = df[df['removal_time_known']]
        exact_removals['toxicity_quartile'] = pd.qcut(exact_removals[
            'title_toxicity'], q=4, labels=['Low', 'Medium-Low',
            'Medium-High', 'High'])
        quartile_times = exact_removals.groupby('toxicity_quartile')[
            'hours_to_removal'].median()
        ax.bar(range(len(quartile_times)), quartile_times.values, color=[
            'green', 'yellow', 'orange', 'red'], alpha=0.7, edgecolor='black')
        ax.set_xticks(range(len(quartile_times)))
        ax.set_xticklabels(quartile_times.index)
        ax.set_xlabel('Toxicity Quartile')
        ax.set_ylabel('Median Time to Removal (hours)')
        ax.set_title('Removal Speed by Content Toxicity')
        ax.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/toxicity_vs_removal_speed.png', dpi=300,
            bbox_inches='tight')
        print('   ✓ Saved: toxicity_vs_removal_speed.png')
        plt.close()
print('\n' + '=' * 70)
print('SAVING RESULTS')
print('=' * 70)
output_file = os.path.join(args.data_dir,
    'analyzed_temporal_patterns_enhanced.csv')
if os.path.exists(output_file) and not args.force:
    print(f'✓ {output_file} already exists. Use --force to overwrite.')
else:
    try:
        df.to_csv(output_file, index=False)
        print(f'📄 Enriched temporal dataset saved to: {output_file}')
    except Exception as e:
        print(f'❌ Failed to write enriched CSV: {e}')
summary_stats = {'analysis_timestamp': datetime.now().isoformat(),
    'total_posts': int(len(df)), 'total_subreddits': int(df['subreddit'].
    nunique()), 'removal_rate': float(df['is_removed_inferred'].mean()),
    'has_t12_data': bool(has_t12_data), 'has_exact_removal_times': int(df[
    'removal_time_known'].sum())}
if has_t12_data:
    summary_stats['early_removal_rate'] = float((df['removal_window'] ==
        'early_0-12h').mean())
    summary_stats['mid_removal_rate'] = float((df['removal_window'] ==
        'mid_12-48h').mean())
    summary_stats['growing_then_removed_count'] = int(df[
        'is_growing_then_removed'].sum())
    summary_stats['mean_score_velocity'] = float(df['score_velocity_t0_t12'
        ].mean())
if LIFELINES_AVAILABLE:
    summary_stats['median_survival_time_hours'] = float(median_survival)
    summary_stats['survival_at_12h'] = float(survival_at_12h)
    summary_stats['survival_at_24h'] = float(survival_at_24h)
    summary_stats['survival_at_48h'] = float(survival_at_48h)
with open(f'{args.output_dir}/temporal_analysis_summary.json', 'w') as f:
    json.dump(summary_stats, f, indent=2, default=str)
print(f'✓ Saved summary: {args.output_dir}/temporal_analysis_summary.json')
test_results = {'hypothesis_tests': []}
if 't_stat_init' in locals():
    test_results['hypothesis_tests'].append({'test':
        'Initial Score (Removed vs Not Removed)', 't_statistic': float(
        t_stat_init), 'p_value': float(p_val_init), 'effect_size_cohens_d':
        float(effect_size_init), 'significant': bool(p_val_init < 0.05)})
if 't_stat_final' in locals():
    test_results['hypothesis_tests'].append({'test':
        'Final Score (Removed vs Not Removed)', 't_statistic': float(
        t_stat_final), 'p_value': float(p_val_final),
        'effect_size_cohens_d': float(effect_size_final), 'significant':
        bool(p_val_final < 0.05)})
if 'chi2' in locals():
    test_results['hypothesis_tests'].append({'test':
        'Removal Rate Variance Across Subreddits (Chi-square)',
        'chi_square': float(chi2), 'degrees_of_freedom': int(dof),
        'p_value': float(p_val_chi), 'significant': bool(p_val_chi < 0.05)})
with open(f'{args.output_dir}/statistical_tests.json', 'w') as f:
    json.dump(test_results, f, indent=2)
print(f'✓ Saved test results: {args.output_dir}/statistical_tests.json')
subreddit_stats.to_csv(f'{args.output_dir}/subreddit_statistics.csv')
print(f'✓ Saved subreddit stats: {args.output_dir}/subreddit_statistics.csv')
if LIFELINES_AVAILABLE:
    survival_summary = pd.DataFrame({'subreddit': list(subreddit_survival.
        keys()), 'median_survival_hours': [data['median'] for data in
        subreddit_survival.values()], 'survival_at_12h': [data[
        'survival_12h'] for data in subreddit_survival.values()]})
    survival_summary.to_csv(f'{args.output_dir}/survival_analysis.csv',
        index=False)
    print(f'✓ Saved survival analysis: {args.output_dir}/survival_analysis.csv'
        )
if linguistic_cols and has_t12_data and len(linguistic_by_window_df) > 0:
    linguistic_by_window_df.to_csv(
        f'{args.output_dir}/linguistic_by_removal_window.csv', index=False)
    print(
        f'✓ Saved linguistic analysis: {args.output_dir}/linguistic_by_removal_window.csv'
        )
print('\n' + '=' * 70)
print('✅ ENHANCED TEMPORAL ANALYSIS COMPLETE')
print('=' * 70)
print(f"""
📊 Analysis Summary:""")
print(f'   Total posts: {len(df):,}')
print(f"   Subreddits: {df['subreddit'].nunique()}")
print(f"   Overall removal rate: {df['is_removed_inferred'].mean():.1%}")
if has_t12_data:
    print(f'\n⏱️  Removal Timing:')
    print(
        f"   Early (0-12h): {(df['removal_window'] == 'early_0-12h').sum():,} ({(df['removal_window'] == 'early_0-12h').mean():.1%})"
        )
    print(
        f"   Mid (12-48h): {(df['removal_window'] == 'mid_12-48h').sum():,} ({(df['removal_window'] == 'mid_12-48h').mean():.1%})"
        )
    print(
        f"   Growing-then-removed: {df['is_growing_then_removed'].sum():,} ({df['is_growing_then_removed'].mean():.1%})"
        )
if linguistic_cols and has_t12_data:
    print(f'   • linguistic_by_removal_window.png')
    if df['removal_time_known'].sum() > 100:
        print(f'   • toxicity_vs_removal_speed.png')
if LIFELINES_AVAILABLE:
    print(f'\n📈 Survival Analysis:')
    print(f'   Median survival time: {median_survival:.1f} hours')
    print(f'   Survival probability at 12h: {survival_at_12h:.1%}')
    print(f'   Survival probability at 48h: {survival_at_48h:.1%}')
print(f'\n📂 Output Files:')
print(f'   {args.data_dir}/analyzed_temporal_patterns_enhanced.csv')
print(f'   {args.output_dir}/temporal_analysis_summary.json')
print(f'   {args.output_dir}/statistical_tests.json')
print(f'   {args.output_dir}/subreddit_statistics.csv')
if LIFELINES_AVAILABLE:
    print(f'   {args.output_dir}/survival_analysis.csv')
    print(f'   • survival_curves.png')
if linguistic_cols and has_t12_data:
    print(f'   {args.output_dir}/linguistic_by_removal_window.csv')
print(f'   {viz_dir}/ (visualizations)')
print(f"""
📊 Visualizations Created:""")
if has_t12_data:
    print(f'   • score_trajectories_aggregate.png')
    print(f'   • velocity_distributions.png')
    if df['is_growing_then_removed'].sum() > 0:
        print(f'   • growing_then_removed.png')
print(f'   • removal_heatmap_time.png')
print(f'   • subreddit_comparison.png')
if linguistic_cols and has_t12_data:
    print(f'   • linguistic_by_removal_window.png')
    if df['removal_time_known'].sum() > 100:
        print(f'   • toxicity_vs_removal_speed.png')
print(f'\n💡 Key Findings:')
if 't_stat_init' in locals():
    print(
        f"   • Initial scores {'DO' if p_val_init < 0.05 else 'do NOT'} differ significantly (p={p_val_init:.4f})"
        )
    print(
        f"   • Effect size: {abs(effect_size_init):.2f} ({'large' if abs(effect_size_init) > 0.8 else 'medium' if abs(effect_size_init) > 0.5 else 'small'})"
        )
print('\n' + '=' * 70)
