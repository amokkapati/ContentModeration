import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from scipy.stats.contingency import association
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from statsmodels.stats.proportion import proportions_ztest
import warnings
import statsmodels.api as sm
import os
import psutil
import glob
warnings.filterwarnings('ignore')
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = 12, 8
ALPHA = 0.05
PSM_CALIPER = 0.2
RDD_CUTOFF = 7
RDD_BANDWIDTH = 7


def fdr_correction(p_values, alpha=0.05):
    p_values = np.array(p_values)
    n = len(p_values)
    sorted_indices = np.argsort(p_values)
    sorted_p = p_values[sorted_indices]
    critical_values = np.arange(1, n + 1) / n * alpha
    comparisons = sorted_p <= critical_values
    if comparisons.any():
        max_idx = np.where(comparisons)[0].max()
        return sorted_p[max_idx]
    return 0.0


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
print('🔍 Looking for data files...')
data_files = glob.glob('../data/master_pass2_combined.csv')
if not data_files:
    print("❌ Error: No combined data files found in 'data/' folder")
    print('   Please run collect_pass2.py first')
    exit(1)
latest_file = max(data_files, key=os.path.getctime)
print(f'📁 Found: {latest_file}')
os.makedirs('../results', exist_ok=True)
os.makedirs('../results/figures', exist_ok=True)
os.makedirs('../results/stats', exist_ok=True)
os.makedirs('../results/causal', exist_ok=True)
print("""
📊 Loading data...""")
try:
    df = pd.read_csv(latest_file)
    if 'post_id' in df.columns:
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['post_id'], keep='first').copy()
        removed_count = initial_rows - len(df)
        print(
            f"""
✓ Removed {removed_count:,} cross-post duplicates (Duplicates found: {removed_count / initial_rows * 100:.2f}%)"""
            )
    else:
        print("\n⚠️ Cannot check for duplicates: 'post_id' column missing.")
        print(f'✓ Loaded {len(df)} posts')
        print(f'✓ Columns: {len(df.columns)}')
except Exception as e:
    print(f'❌ Error loading data: {e}')
    exit(1)
print('\n' + '=' * 70)
print('DATA CLEANING & PREPARATION')
print('=' * 70)
print(f"""
Initial dataset: {len(df)} posts""")
if 'is_removed_inferred' in df.columns:
    df['is_removed'] = df['is_removed_inferred'].fillna(False).astype(bool)
elif 'is_removed_official' in df.columns:
    df['is_removed'] = df['is_removed_official'].fillna(False).astype(bool)
else:
    print('❌ Error: No removal indicator column found!')
    exit(1)
print("✓ Created unified 'is_removed' indicator")
df['score'] = df.get('final_score', df.get('initial_score', 0)).fillna(0)
df['num_comments'] = df.get('final_num_comments', df.get(
    'initial_num_comments', 0)).fillna(0)
if 'thread_deletion_ratio' in df.columns:
    df['thread_deletion_ratio'] = df['thread_deletion_ratio'].fillna(0.0)
else:
    df['thread_deletion_ratio'] = 0.0
initial_count = len(df)
if 'author_unavailable' in df.columns:
    df = df[df['author_unavailable'] != True].copy()
df = df[df['author_username'].notna()].copy()
df = df[df['author_username'] != '[deleted]'].copy()
df = df[df['author_username'] != 'AutoModerator'].copy()
removed_count = initial_count - len(df)
print(
    f"""
✓ Removed {removed_count} deleted/unavailable/AutoMod posts ({removed_count / initial_count * 100:.1f}%)"""
    )
df['author_account_age_days'] = df['author_account_age_days'].fillna(df[
    'author_account_age_days'].median())
df['author_total_karma'] = df['author_total_karma'].fillna(0)
df['score'] = df['score'].fillna(0)
df['num_comments'] = df['num_comments'].fillna(0)
df['is_removed'] = df['is_removed'].astype(bool)
if 'is_locked' not in df.columns:
    df['is_locked'] = False
df['is_locked'] = df['is_locked'].fillna(False).astype(bool)
print("""
✓ Creating demographic categories...""")
df['account_age_category'] = pd.cut(df['author_account_age_days'], bins=[0,
    30, 180, 365, 1825, np.inf], labels=['Very New (0-30d)',
    'New (30-180d)', 'Established (6m-1y)', 'Veteran (1-5y)', 'Ancient (5y+)'])
df['karma_category'] = pd.cut(df['author_total_karma'], bins=[0, 100, 1000,
    10000, 50000, np.inf], labels=['Very Low (<100)', 'Low (100-1k)',
    'Medium (1k-10k)', 'High (10k-50k)', 'Very High (50k+)'])
df['karma_per_day'] = df['author_total_karma'] / (df[
    'author_account_age_days'] + 1)
df = df.dropna(subset=['account_age_category', 'karma_category'])
print(f"""
✓ Clean dataset: {len(df)} posts""")
print(f"✓ Subreddits: {df['subreddit'].nunique()}")
print(f"✓ Unique authors: {df['author_username'].nunique()}")
print('\n' + '=' * 70)
print('DESCRIPTIVE STATISTICS')
print('=' * 70)
print(f"""
📌 Overall Moderation Rates:""")
total_removed = df['is_removed'].sum()
removal_rate = df['is_removed'].mean() * 100
total_locked = df['is_locked'].sum()
lock_rate = df['is_locked'].mean() * 100
print(f'  Removed: {total_removed:,} posts ({removal_rate:.2f}%)')
print(f'  Locked: {total_locked:,} posts ({lock_rate:.2f}%)')
print(
    f'  Not Moderated: {len(df) - total_removed:,} posts ({100 - removal_rate:.2f}%)'
    )
print(f"""
📌 User Demographics:""")
print(f'  Account Age:')
print(
    f"    Mean: {df['author_account_age_days'].mean():.0f} days ({df['author_account_age_days'].mean() / 365:.1f} years)"
    )
print(
    f"    Median: {df['author_account_age_days'].median():.0f} days ({df['author_account_age_days'].median() / 365:.1f} years)"
    )
print(f'\n  Karma:')
print(f"    Mean: {df['author_total_karma'].mean():.0f}")
print(f"    Median: {df['author_total_karma'].median():.0f}")
print(f'\n📌 By Subreddit:')
subreddit_stats = df.groupby('subreddit').agg({'is_removed': ['count',
    'sum', 'mean'], 'is_locked': 'mean'}).round(4)
subreddit_stats.columns = ['Total Posts', 'Removed Count', 'Removal Rate',
    'Lock Rate']
subreddit_stats['Removal Rate'] = (subreddit_stats['Removal Rate'] * 100
    ).round(2)
subreddit_stats['Lock Rate'] = (subreddit_stats['Lock Rate'] * 100).round(2)
print(subreddit_stats.sort_values('Removal Rate', ascending=False).to_string())


def interpret_cramers_v(v):
    if v < 0.1:
        return 'negligible'
    elif v < 0.3:
        return 'small'
    elif v < 0.5:
        return 'medium'
    else:
        return 'large'


def interpret_odds_ratio(or_value):
    if or_value > 1:
        return f'{or_value:.2f}x more likely'
    else:
        return f'{1 / or_value:.2f}x less likely'


def calculate_cramers_v_ci(contingency, confidence=0.95):
    n = contingency.sum().sum()
    r, c = contingency.shape
    chi2, _, _, _ = stats.chi2_contingency(contingency)
    v = np.sqrt(chi2 / (n * min(r - 1, c - 1)))
    se = np.sqrt((1 - v ** 2) / n)
    z = stats.norm.ppf((1 + confidence) / 2)
    ci_lower = max(0, v - z * se)
    ci_upper = min(1, v + z * se)
    return ci_lower, ci_upper


print('\n' + '=' * 70)
print('ENHANCED HYPOTHESIS TESTING')
print('=' * 70)
test_results = []
print('\n' + '=' * 50)
print('H1: Does Account Age Affect Removal Rate?')
print('=' * 50)
age_removal = df.groupby('account_age_category')['is_removed'].agg(['count',
    'sum', 'mean'])
age_removal.columns = ['Total Posts', 'Removed', 'Removal Rate']
age_removal['Removal Rate'] = (age_removal['Removal Rate'] * 100).round(2)
print(age_removal.to_string())
contingency_age = pd.crosstab(df['account_age_category'], df['is_removed'])
chi2_age, p_age, dof_age, expected_age = stats.chi2_contingency(contingency_age
    )
cramers_v_age = association(contingency_age, method='cramer')
v_ci_lower, v_ci_upper = calculate_cramers_v_ci(contingency_age)
print(f"""
📊 Statistical Test Results:""")
print(f'  Chi-square: χ² = {chi2_age:.2f}, df = {dof_age}, p = {p_age:.6f}')
print(
    f"  Cramér's V: {cramers_v_age:.3f} (95% CI: [{v_ci_lower:.3f}, {v_ci_upper:.3f}])"
    )
print(f'  Effect Size: {interpret_cramers_v(cramers_v_age).upper()}')
print(
    f"  Significance: {'✓ SIGNIFICANT' if p_age < ALPHA else '✗ NOT SIGNIFICANT'} (α={ALPHA})"
    )
test_results.append({'Hypothesis': 'H1: Account Age', 'Chi-Square':
    chi2_age, 'p-value': p_age, 'Cramers_V': cramers_v_age, 'Effect_Size':
    interpret_cramers_v(cramers_v_age), 'Significant': p_age < ALPHA})
print('\n' + '=' * 50)
print('H2: Does Karma Level Affect Removal Rate?')
print('=' * 50)
karma_removal = df.groupby('karma_category')['is_removed'].agg(['count',
    'sum', 'mean'])
karma_removal.columns = ['Total Posts', 'Removed', 'Removal Rate']
karma_removal['Removal Rate'] = (karma_removal['Removal Rate'] * 100).round(2)
print(karma_removal.to_string())
contingency_karma = pd.crosstab(df['karma_category'], df['is_removed'])
chi2_karma, p_karma, dof_karma, expected_karma = stats.chi2_contingency(
    contingency_karma)
cramers_v_karma = association(contingency_karma, method='cramer')
v_ci_lower_k, v_ci_upper_k = calculate_cramers_v_ci(contingency_karma)
print(f"""
📊 Statistical Test Results:""")
print(
    f'  Chi-square: χ² = {chi2_karma:.2f}, df = {dof_karma}, p = {p_karma:.6f}'
    )
print(
    f"  Cramér's V: {cramers_v_karma:.3f} (95% CI: [{v_ci_lower_k:.3f}, {v_ci_upper_k:.3f}])"
    )
print(f'  Effect Size: {interpret_cramers_v(cramers_v_karma).upper()}')
print(
    f"  Significance: {'✓ SIGNIFICANT' if p_karma < ALPHA else '✗ NOT SIGNIFICANT'} (α={ALPHA})"
    )
test_results.append({'Hypothesis': 'H2: Karma Level', 'Chi-Square':
    chi2_karma, 'p-value': p_karma, 'Cramers_V': cramers_v_karma,
    'Effect_Size': interpret_cramers_v(cramers_v_karma), 'Significant': 
    p_karma < ALPHA})
print('\n' + '=' * 50)
print('H3: Age × Karma Interaction Effect')
print('=' * 50)
df['age_x_karma'] = df['author_account_age_days'] * df['author_total_karma']
scaler = StandardScaler()
X_interaction = scaler.fit_transform(df[['author_account_age_days',
    'author_total_karma', 'age_x_karma']])
y_interaction = df['is_removed']
lr_interaction = LogisticRegression(max_iter=1000)
lr_interaction.fit(X_interaction, y_interaction)
print('  Coefficients (per 1 SD change):')
print(
    f'    Age (per 1 SD):       {lr_interaction.coef_[0][0]:.6f} (OR per 1 SD ≈ {np.exp(lr_interaction.coef_[0][0]):.3f})'
    )
print(
    f'    Karma (per 1 SD):     {lr_interaction.coef_[0][1]:.6f} (OR per 1 SD ≈ {np.exp(lr_interaction.coef_[0][1]):.3f})'
    )
print(
    f'    Age×Karma (per 1 SD): {lr_interaction.coef_[0][2]:.6f} (OR per 1 SD ≈ {np.exp(lr_interaction.coef_[0][2]):.3f})'
    )
X_sm_inter = sm.add_constant(df[['author_account_age_days',
    'author_total_karma', 'age_x_karma']])
y_sm_inter = df['is_removed'].astype(int)
model_label = None
p_int = None
try:
    inter_model = sm.Logit(y_sm_inter, X_sm_inter).fit(disp=False)
    model_label = 'statsmodels Logit'
except Exception as e:
    print(
        f"""
⚠️  Logit interaction model failed ({e}); falling back to OLS (linear probability model)."""
        )
    inter_model = sm.OLS(y_sm_inter, X_sm_inter).fit()
    model_label = 'statsmodels OLS (LPM fallback)'
try:
    p_int = float(inter_model.pvalues['age_x_karma'])
    print(f'\n  {model_label} interaction p-value: {p_int:.6f}')
    print(
        f"  Interaction: {'✓ SIGNIFICANT' if p_int < ALPHA else '✗ NOT SIGNIFICANT'} (α={ALPHA})"
        )
except Exception as e:
    print(f'\n⚠️  Interaction significance extraction failed: {e}')
print('\n' + '=' * 50)
print('H4: Does Account Age Affect Lock Rate?')
print('=' * 50)
age_locked = df.groupby('account_age_category')['is_locked'].agg(['count',
    'sum', 'mean'])
age_locked.columns = ['Total Posts', 'Locked', 'Lock Rate']
age_locked['Lock Rate'] = (age_locked['Lock Rate'] * 100).round(2)
print(age_locked.to_string())
contingency_lock = pd.crosstab(df['account_age_category'], df['is_locked'])
chi2_lock, p_lock, dof_lock, expected_lock = stats.chi2_contingency(
    contingency_lock)
print(f"""
📊 Statistical Test Results (Locked):""")
print(f'  Chi-square: {chi2_lock:.2f}, p = {p_lock:.6f}')
print(
    f"  Significance: {'✓ SIGNIFICANT' if p_lock < ALPHA else '✗ NOT SIGNIFICANT'}"
    )
print('\n' + '=' * 70)
print('CAUSAL INFERENCE: Propensity Score Matching')
print('=' * 70)
psm_success = False
treated_matched = None
treated = None
control_matched = None
covariates = ['author_account_age_days', 'author_total_karma']
if 'title_length' in df.columns:
    covariates.append('title_length')
if 'body_length' in df.columns:
    covariates.append('body_length')
if 'initial_score' in df.columns:
    covariates.append('initial_score')
df_psm = df[covariates + ['is_removed']].dropna()
if df_psm['is_removed'].nunique() < 2:
    print('\n⚠️  Skipping PSM: Not enough variation in removal status.')
else:
    try:
        scaler = StandardScaler()
        X_psm = df_psm[covariates]
        X_psm_scaled = scaler.fit_transform(X_psm)
        y_psm = df_psm['is_removed']
        lr_psm = LogisticRegression(max_iter=1000)
        lr_psm.fit(X_psm_scaled, y_psm)
        df_psm['propensity_score'] = lr_psm.predict_proba(X_psm_scaled)[:, 1]
        treated = df_psm[df_psm['is_removed'] == True].copy()
        control = df_psm[df_psm['is_removed'] == False].copy()
        if len(treated) == 0 or len(control) == 0:
            print('\n⚠️  Skipping PSM: No treated or no control group.')
            psm_success = False
        else:
            treated_scores = treated['propensity_score'].values.reshape(-1, 1)
            control_scores = control['propensity_score'].values.reshape(-1, 1)
            nn = NearestNeighbors(n_neighbors=1)
            nn.fit(control_scores)
            distances, indices = nn.kneighbors(treated_scores)
            valid_matches = distances[:, 0] <= PSM_CALIPER
            if not valid_matches.any():
                print(
                    f'⚠️  PSM produced 0 matches within caliper={PSM_CALIPER}.'
                    )
                psm_success = False
            else:
                treated_matched = treated.iloc[valid_matches].copy()
                control_matched = control.iloc[indices[valid_matches, 0]].copy(
                    )
                psm_success = True
                print(
                    f'✓ PSM completed: {len(treated_matched)} matched pairs (match rate={len(treated_matched) / len(treated) * 100:.1f}%)'
                    )
    except Exception as e:
        print(f'\n⚠️  PSM failed: {e}')
        psm_success = False
print(f"""
After matching (caliper={PSM_CALIPER}):""")
if psm_success and treated_matched is not None and treated is not None and len(
    treated) > 0:
    print(f'  Treated (removed): {len(treated_matched):,}')
    print(f'  Control (not removed): {len(control_matched):,}')
    print(f'  Match rate: {len(treated_matched) / len(treated) * 100:.1f}%')
else:
    print('  ⚠️  PSM was skipped or failed — no matched dataset available.')
if psm_success and treated_matched is not None and control_matched is not None:
    print(f'\nCovariate Balance:')
    print(
        f"{'Variable':<30} {'Before (std diff)':<20} {'After (std diff)':<20}")
    print('-' * 70)
    for cov in ['author_account_age_days', 'author_total_karma']:
        sd_pooled_before = np.sqrt((treated[cov].var(ddof=1) + control[cov]
            .var(ddof=1)) / 2)
        before_diff = (treated[cov].mean() - control[cov].mean()
            ) / sd_pooled_before
        sd_pooled_after = np.sqrt((treated_matched[cov].var(ddof=1) +
            control_matched[cov].var(ddof=1)) / 2)
        after_diff = (treated_matched[cov].mean() - control_matched[cov].mean()
            ) / sd_pooled_after
        print(f'{cov:<30} {before_diff:>15.3f}     {after_diff:>15.3f}')
else:
    print('\nSkipping balance/saving: no matched data.')
if psm_success and treated_matched is not None and control_matched is not None:
    treated_matched.to_csv('../results/causal/psm_treated.csv', index=False)
    control_matched.to_csv('../results/causal/psm_control.csv', index=False)
    print('✓ Saved matched samples to results/causal/')
else:
    print('Skipping save: no matched data.')
print('\n' + '=' * 70)
print('CAUSAL INFERENCE: Regression Discontinuity Design')
print('=' * 70)
rdd_success = False
effect = None
p_value_rdd = None
df_rdd = df[(df['author_account_age_days'] >= RDD_CUTOFF - RDD_BANDWIDTH) &
    (df['author_account_age_days'] <= RDD_CUTOFF + RDD_BANDWIDTH)].copy()
if len(df_rdd) < 20:
    print('\n⚠️  Skipping RDD: Not enough data near cutoff.')
else:
    df_rdd['treated'] = (df_rdd['author_account_age_days'] >= RDD_CUTOFF
        ).astype(int)
    below = df_rdd[df_rdd['treated'] == 0]
    above = df_rdd[df_rdd['treated'] == 1]
    if len(below) == 0 or len(above) == 0:
        print('\n⚠️  Skipping RDD: No data on one side of cutoff.')
    else:
        if abs(len(above) - len(below)) / ((len(above) + len(below)) / 2
            ) > 0.2:
            print('⚠️  WARNING: Significant density discontinuity detected!')
            print(
                '   This suggests manipulation (users waiting for account age).'
                )
        try:
            removal_rate_below = below['is_removed'].mean()
            removal_rate_above = above['is_removed'].mean()
            effect = removal_rate_above - removal_rate_below
            successes = np.array([below['is_removed'].sum(), above[
                'is_removed'].sum()])
            ns = np.array([len(below), len(above)])
            z_stat, p_value_rdd = proportions_ztest(successes, ns)
            rdd_success = True
            print(
                f'✓ RDD completed (effect={effect:.4f}, z={z_stat:.2f}, p={p_value_rdd:.4f})'
                )
        except Exception as e:
            print(f'\n⚠️  RDD failed: {e}')
if rdd_success:
    print(f'\nRemoval rate BELOW cutoff: {removal_rate_below * 100:.1f}%')
    print(f'Removal rate ABOVE cutoff: {removal_rate_above * 100:.1f}%')
    print(f'Treatment effect: {effect * 100:.1f} percentage points')
    print(f'\nProportion z-test: z = {z_stat:.2f}, p = {p_value_rdd:.6f}')
    print(
        f"{'✓ SIGNIFICANT' if p_value_rdd < ALPHA else '✗ NOT SIGNIFICANT'} (α={ALPHA})"
        )
else:
    print('\nℹ️ RDD results unavailable (skipped or failed).')
if rdd_success and p_value_rdd is not None:
    test_results.append({'Hypothesis': 'H5: RDD (Age Cutoff)', 'Chi-Square':
        np.nan, 'p-value': p_value_rdd, 'Cramers_V': np.nan, 'Effect_Size':
        'N/A', 'Significant': p_value_rdd < ALPHA})
print('\n' + '=' * 70)
print('CAUSAL INFERENCE: Mediation Analysis (OLS Linear Probability Model)')
print('=' * 70)
print(
    """
Testing: Does author_account_age_days → is_removed work through author_total_karma?"""
    )
print(
    'Note: OLS mediation on the probability scale (Y model: is_removed ~ age + karma, linear probability model).'
    )
mediation_success = False
proportion_mediated = None
df_med = df[['author_account_age_days', 'author_total_karma', 'is_removed']
    ].dropna().copy()
df_med['author_total_karma_k'] = df_med['author_total_karma'] / 1000.0
if df_med['is_removed'].nunique() < 2:
    print('\n⚠️  Skipping mediation: Only one removal class.')
else:
    try:
        X_m = sm.add_constant(df_med['author_account_age_days'])
        y_m = df_med['author_total_karma_k']
        model_m = sm.OLS(y_m, X_m).fit()
        a_path = model_m.params['author_account_age_days']
        X_y_full = sm.add_constant(df_med[['author_account_age_days',
            'author_total_karma_k']])
        y_y = df_med['is_removed'].astype(float)
        model_y_full = sm.OLS(y_y, X_y_full).fit()
        direct_effect = model_y_full.params['author_account_age_days']
        b_path = model_y_full.params['author_total_karma_k']
        X_y_reduced = sm.add_constant(df_med[['author_account_age_days']])
        model_y_reduced = sm.OLS(y_y, X_y_reduced).fit()
        total_effect = model_y_reduced.params['author_account_age_days']
        indirect_effect = a_path * b_path
        print(
            f'Point estimate (indirect, probability scale): {indirect_effect:.6f}'
            )
        print('🔧 Bootstrapping (1000 iters, OLS mediation)...')
        boot_effects = []
        n_boot = 1000
        for _ in range(n_boot):
            sample = df_med.sample(frac=1, replace=True)
            X_m_b = sm.add_constant(sample['author_account_age_days'])
            y_m_b = sample['author_total_karma_k']
            try:
                model_m_b = sm.OLS(y_m_b, X_m_b).fit()
                a_b = model_m_b.params['author_account_age_days']
            except Exception:
                continue
            X_y_full_b = sm.add_constant(sample[['author_account_age_days',
                'author_total_karma_k']])
            y_y_b = sample['is_removed'].astype(float)
            try:
                model_y_full_b = sm.OLS(y_y_b, X_y_full_b).fit()
                b_b = model_y_full_b.params['author_total_karma_k']
            except Exception:
                continue
            boot_effects.append(a_b * b_b)
        if len(boot_effects) == 0:
            print('⚠️  All bootstrap iterations failed; no CI available.')
            ci_lower, ci_upper = np.nan, np.nan
        else:
            ci_lower = np.percentile(boot_effects, 2.5)
            ci_upper = np.percentile(boot_effects, 97.5)
        print(
            f'✓ 95% CI for indirect effect (probability scale): [{ci_lower:.6f}, {ci_upper:.6f}]'
            )
        mediation_success = True
        if np.isclose(total_effect, 0):
            proportion_mediated = np.nan
        else:
            proportion_mediated = indirect_effect / total_effect
    except Exception as e:
        print(f'\n⚠️  Mediation failed: {e}')
        total_effect = 0.0
        direct_effect = 0.0
        indirect_effect = 0.0
        proportion_mediated = 0.0
print("""
Results (OLS mediation, probability scale):""")
if mediation_success:
    print(f'  Total effect c (probability):       {total_effect:.6f}')
    print(f"  Direct effect c' (probability):     {direct_effect:.6f}")
    print(f'  Indirect effect a*b (probability):  {indirect_effect:.6f}')
    if np.isnan(proportion_mediated):
        print('  Proportion mediated:            undefined (total effect ≈ 0)')
    else:
        print(
            f'  Proportion mediated:            {proportion_mediated * 100:.1f}%'
            )
    if not np.isnan(proportion_mediated):
        if abs(proportion_mediated) > 0.3:
            print(
                '\n✓ SUBSTANTIAL MEDIATION: Karma mediates a large share of age → removal'
                )
        elif abs(proportion_mediated) > 0.1:
            print(
                '\n⚠️  PARTIAL MEDIATION: Karma partially mediates age → removal'
                )
        else:
            print(
                '\n✗ WEAK/NO MEDIATION: Karma does not meaningfully mediate age → removal'
                )
    mediation_results = pd.DataFrame([{'total_effect_prob': total_effect,
        'direct_effect_prob': direct_effect, 'indirect_effect_prob':
        indirect_effect, 'proportion_mediated': proportion_mediated,
        'ci_lower_prob': ci_lower, 'ci_upper_prob': ci_upper}])
    mediation_results.to_csv('../results/causal/mediation_results.csv',
        index=False)
    print('\n✓ Saved mediation results to results/causal/mediation_results.csv'
        )
else:
    print('  ⚠️  Mediation skipped or failed — no results available.')
all_p_values = [p_age, p_karma]
if rdd_success and p_value_rdd is not None:
    all_p_values.append(p_value_rdd)
fdr_threshold = fdr_correction(all_p_values, ALPHA)
print(f"""
🧪 FDR Threshold: {fdr_threshold:.6f}""")
significance_cutoff = fdr_threshold if fdr_threshold > 0 else ALPHA
for tr in test_results:
    p = tr.get('p-value', None)
    if p is not None:
        tr['Significant'] = p <= significance_cutoff
print(
    f'Using significance cutoff = {significance_cutoff:.3f} (FDR-adjusted) for test_results.'
    )
print('\n' + '=' * 70)
print('SAVING STATISTICAL TEST RESULTS')
print('=' * 70)
test_results_df = pd.DataFrame(test_results)
test_results_df.to_csv('../results/stats/hypothesis_tests.csv', index=False)
print('✓ Saved: results/stats/hypothesis_tests.csv')
summary = {'Total Posts': len(df), 'Subreddits': df['subreddit'].nunique(),
    'Overall Removal Rate': f"{df['is_removed'].mean() * 100:.2f}%"}
if psm_success:
    summary['PSM Match Rate'
        ] = f'{len(treated_matched) / len(treated) * 100:.1f}%'
else:
    summary['PSM Match Rate'] = 'N/A'
if rdd_success:
    summary['RDD Treatment Effect'] = f'{effect * 100:.1f} pp'
    summary['RDD p-value'] = f'{p_value_rdd:.6f}'
else:
    summary['RDD Treatment Effect'] = 'N/A'
    summary['RDD p-value'] = 'N/A'
if mediation_success:
    summary['Mediation Proportion (probability)'
        ] = f'{proportion_mediated * 100:.1f}%'
else:
    summary['Mediation Proportion (probability)'] = 'N/A'
pd.DataFrame([summary]).to_csv('../results/summary_statistics_enhanced.csv',
    index=False)
print("""
=== FINAL SUMMARY ===""")
for k, v in summary.items():
    print(f'{k}: {v}')
print('\n' + '=' * 70)
print('CREATING VISUALIZATIONS')
print('=' * 70)
fig1, axes1 = plt.subplots(2, 2, figsize=(16, 12))
fig1.suptitle('Reddit Moderation Analysis: Enhanced Statistical Tests',
    fontsize=16, fontweight='bold')
ax = axes1[0, 0]
age_data = df.groupby('account_age_category')['is_removed'].mean() * 100
age_data.plot(kind='bar', ax=ax, color='steelblue', edgecolor='black')
ax.set_title(
    f"""Removal Rate by Account Age
(χ²={chi2_age:.1f}, p={p_age:.3f}, V={cramers_v_age:.3f})"""
    , fontweight='bold')
ax.set_ylabel('Removal Rate (%)')
ax.set_xlabel('Account Age')
ax.tick_params(axis='x', rotation=45)
ax.grid(axis='y', alpha=0.3)
for container in ax.containers:
    ax.bar_label(container, fmt='%.1f%%')
ax = axes1[0, 1]
karma_data = df.groupby('karma_category')['is_removed'].mean() * 100
karma_data.plot(kind='bar', ax=ax, color='coral', edgecolor='black')
ax.set_title(
    f"""Removal Rate by Karma
(χ²={chi2_karma:.1f}, p={p_karma:.3f}, V={cramers_v_karma:.3f})"""
    , fontweight='bold')
ax.set_ylabel('Removal Rate (%)')
ax.set_xlabel('Karma Level')
ax.tick_params(axis='x', rotation=45)
ax.grid(axis='y', alpha=0.3)
for container in ax.containers:
    ax.bar_label(container, fmt='%.1f%%')
ax = axes1[1, 0]
if psm_success and treated_matched is not None and control_matched is not None:
    before_means = [treated['author_account_age_days'].mean() / 100, 
        treated['author_total_karma'].mean() / 1000]
    after_means = [(treated_matched[cov].mean() / (100 if cov ==
        'author_account_age_days' else 1000)) for cov in [
        'author_account_age_days', 'author_total_karma']]
    control_means = [(control_matched[cov].mean() / (100 if cov ==
        'author_account_age_days' else 1000)) for cov in [
        'author_account_age_days', 'author_total_karma']]
    x = np.arange(2)
    width = 0.25
    ax.bar(x - width, before_means, width, label='Treated (before)', color=
        'red', alpha=0.7)
    ax.bar(x, after_means, width, label='Treated (after)', color='darkred')
    ax.bar(x + width, control_means, width, label='Control (after)', color=
        'green')
    ax.set_ylabel('Mean Value (scaled)')
    ax.set_title('Propensity Score Matching: Covariate Balance', fontweight
        ='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(['Age (days/100)', 'Karma (k)'])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
else:
    ax.text(0.5, 0.5, 'PSM not available', ha='center', va='center')
    ax.set_axis_off()
ax = axes1[1, 1]
if rdd_success and effect is not None:
    df_rdd['distance_from_cutoff'] = df_rdd['author_account_age_days'
        ] - RDD_CUTOFF
    bin_width = 2
    min_val = np.floor(df_rdd['distance_from_cutoff'].min())
    max_val = np.ceil(df_rdd['distance_from_cutoff'].max())
    bins = np.arange(min_val, max_val + bin_width, bin_width)
    df_rdd['bin'] = pd.cut(df_rdd['distance_from_cutoff'], bins=bins, right
        =False)
    binned = df_rdd.groupby('bin')['is_removed'].mean() * 100
    bin_centers = [b.mid for b in binned.index]
    ax.scatter(bin_centers, binned.values, s=100, alpha=0.7, color='steelblue')
    ax.axvline(x=0, color='red', linestyle='--', linewidth=2, label=
        f'Cutoff ({RDD_CUTOFF} days)')
    ax.set_xlabel('Days from Cutoff', fontsize=12, fontweight='bold')
    ax.set_ylabel('Removal Rate (%)', fontsize=12, fontweight='bold')
    ax.set_title(
        f"""Regression Discontinuity Design
(Effect={effect * 100:.1f}pp, p={p_value_rdd:.3f})"""
        , fontweight='bold')
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig('../results/figures/enhanced_analysis.png', dpi=300,
        bbox_inches='tight')
    print('✓ Saved: results/figures/enhanced_analysis.png')
else:
    ax.text(0.5, 0.5, 'RDD not available', ha='center', va='center')
    ax.set_axis_off()
print('\n' + '=' * 70)
print('✅ ENHANCED ANALYSIS COMPLETE!')
print('=' * 70)
print('\n💡 Key Findings:')
significant_tests = [t for t in test_results if t['Significant']]
print(
    f'  • {len(significant_tests)}/{len(test_results)} hypotheses were statistically significant'
    )
if significant_tests:
    print('  • Significant after FDR:', ', '.join(t['Hypothesis'] for t in
        significant_tests))
else:
    print('  • Significant after FDR: none')
if psm_success and treated_matched is not None and treated is not None and len(
    treated) > 0:
    print(
        f'  • PSM match rate: {len(treated_matched) / len(treated) * 100:.1f}%'
        )
else:
    print(f'  • PSM match rate: N/A (PSM skipped or failed)')
if rdd_success and effect is not None:
    print(f'  • RDD treatment effect: {effect * 100:.1f} percentage points')
else:
    print(f'  • RDD treatment effect: N/A (RDD skipped or failed)')
if mediation_success and proportion_mediated is not None:
    print(
        f'  • Mediation: {proportion_mediated * 100:.1f}% of age effect (probability scale) works through karma'
        )
else:
    print(f'  • Mediation: N/A (mediation skipped or failed)')
print('\n' + '=' * 70)
