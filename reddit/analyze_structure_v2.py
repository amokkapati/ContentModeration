import argparse
import pandas as pd
import numpy as np
import os
import glob
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.metrics import silhouette_score, silhouette_samples, calinski_harabasz_score
from sklearn.metrics.pairwise import cosine_similarity
import umap
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from scipy.spatial.distance import pdist, squareform
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from scipy.stats import zscore
import psutil
from tqdm import tqdm
import warnings
import hdbscan
import json
from datetime import datetime
warnings.filterwarnings('ignore')
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = 14, 10
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(BASE_DIR, '..', 'data')
data_dir = os.path.abspath(data_dir)
parser = argparse.ArgumentParser(description=
    'Enhanced UMAP subreddit similarity analysis')
parser.add_argument('--n-neighbors', type=int, default=5, help=
    'UMAP n_neighbors parameter (default: 5)')
parser.add_argument('--min-dist', type=float, default=0.1, help=
    'UMAP min_dist parameter (default: 0.1)')
parser.add_argument('--n-clusters', default=5, help=
    'Number of K-means clusters or "auto" for automatic detection (default: 5)'
    )
parser.add_argument('--min-posts', type=int, default=50, help=
    'Minimum posts per subreddit to include (default: 50)')
parser.add_argument('--scaler', choices=['standard', 'robust'], default=
    'robust', help='Feature scaling method (default: robust)')
parser.add_argument('--data-dir', type=str, default=data_dir, help=
    f'Data directory (default: {data_dir})')
parser.add_argument('--outlier-method', choices=['zscore', 'iforest'],
    default='iforest', help=
    'Outlier detection method: simple z-score or IsolationForest (default: iforest)'
    )
parser.add_argument('--outlier-fraction', type=float, default=None, help=
    'Approximate fraction of subreddits to treat as outliers (e.g., 0.05 for 5%%). If not set, a sensible default is chosen based on the number of subreddits.'
    )
args = parser.parse_args()
RANDOM_STATE = 42
if args.n_clusters == 'auto':
    AUTO_CLUSTERS = True
    N_CLUSTERS = None
else:
    AUTO_CLUSTERS = False
    N_CLUSTERS = int(args.n_clusters)


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


def detect_outliers(data: np.ndarray, method: str='iforest', contamination:
    (float | None)=None, z_thresh: float=3.0, random_state: int=42):
    n_subs = data.shape[0]
    if method == 'zscore':
        z_scores = np.abs(zscore(data, axis=0, nan_policy='omit'))
        max_z = np.nanmax(z_scores, axis=1)
        outlier_mask = max_z > z_thresh
        return outlier_mask, max_z
    if contamination is None:
        base_fraction = 50 / max(n_subs, 1)
        contamination = float(np.clip(base_fraction, 0.02, 0.08))
    iforest = IsolationForest(n_estimators=200, contamination=contamination,
        random_state=random_state)
    iforest.fit(data)
    preds = iforest.predict(data)
    outlier_mask = preds == -1
    scores = -iforest.score_samples(data)
    return outlier_mask, scores


def find_optimal_clusters(data, max_k=10):
    n = len(data)
    if n < 3:
        raise ValueError(
            'Not enough subreddits for cluster search (need at least 3).')
    max_k = min(max_k, n - 1)
    if max_k < 2:
        raise ValueError(
            f'Cannot run cluster search with max_k={max_k} and n={n}. Need at least 2 clusters.'
            )
    K_range = range(2, max_k + 1)
    inertias = []
    silhouettes = []
    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = kmeans.fit_predict(data)
        inertias.append(kmeans.inertia_)
        silhouettes.append(silhouette_score(data, labels))
    return K_range, inertias, silhouettes


if not check_memory(min_gb=2):
    exit(0)
print('=' * 70)
print('🗺️  ENHANCED UMAP SUBREDDIT SIMILARITY ANALYSIS')
print('=' * 70)
print("""
📂 Looking for data files...""")
master_file = f'{args.data_dir}/linguistics_boolean_fixed.csv'
if os.path.exists(master_file):
    print(f'✓ Found Master Dataset: {master_file}')
    df = pd.read_csv(master_file)
else:
    print('⚠️  Master file not found. Falling back to raw data.')
    data_files = glob.glob(f'{args.data_dir}/combined_complete_*.csv')
    if not data_files:
        data_files = glob.glob('combined_complete_*.csv')
    if not data_files:
        print('❌ No data found.')
        exit(1)
    latest_file = max(data_files, key=os.path.getctime)
    df = pd.read_csv(latest_file)
print(f"✓ Loaded {len(df):,} posts from {df['subreddit'].nunique()} subreddits"
    )
subreddit_counts = df['subreddit'].value_counts()
valid_subreddits = subreddit_counts[subreddit_counts >= args.min_posts].index
df = df[df['subreddit'].isin(valid_subreddits)]
print(
    f"✓ After filtering (min {args.min_posts} posts): {df['subreddit'].nunique()} subreddits"
    )
output_dir = 'results/structure'
os.makedirs(output_dir, exist_ok=True)
print('\n' + '=' * 70)
print('BUILDING SUBREDDIT FEATURE MATRIX')
print('=' * 70)
print("""
🔧 Extracting features per subreddit...""")
subreddit_features = {}
feature_missingness = {}
for subreddit in tqdm(df['subreddit'].unique(), desc='Processing subreddits'):
    sub_df = df[df['subreddit'] == subreddit]
    features = {}
    if 'is_removed_inferred' in sub_df.columns:
        removal_col = 'is_removed_inferred'
    elif 'is_removed' in sub_df.columns:
        removal_col = 'is_removed'
    else:
        removal_col = None
    if removal_col is not None:
        features['removal_rate'] = sub_df[removal_col].mean() * 100
    else:
        features['removal_rate'] = np.nan
    features['lock_rate'] = sub_df['is_locked'].mean(
        ) * 100 if 'is_locked' in sub_df.columns else np.nan
    features['total_posts'] = len(sub_df)
    if 'author_account_age_days' in sub_df.columns:
        features['median_account_age'] = sub_df['author_account_age_days'
            ].median()
        features['mean_account_age'] = sub_df['author_account_age_days'].mean()
        features['account_age_std'] = sub_df['author_account_age_days'].std()
    else:
        features['median_account_age'] = np.nan
        features['mean_account_age'] = np.nan
        features['account_age_std'] = np.nan
    if 'author_total_karma' in sub_df.columns:
        features['median_karma'] = sub_df['author_total_karma'].median()
        features['mean_karma'] = sub_df['author_total_karma'].mean()
        features['karma_std'] = sub_df['author_total_karma'].std()
    else:
        features['median_karma'] = np.nan
        features['mean_karma'] = np.nan
        features['karma_std'] = np.nan
    if 'thread_deletion_ratio' in sub_df.columns:
        features['mean_toxicity'] = sub_df['thread_deletion_ratio'].mean()
        features['median_toxicity'] = sub_df['thread_deletion_ratio'].median()
        features['toxicity_std'] = sub_df['thread_deletion_ratio'].std()
    else:
        features['mean_toxicity'] = np.nan
        features['median_toxicity'] = np.nan
        features['toxicity_std'] = np.nan
    if 'mod_comment_text' in sub_df.columns:
        features['mod_comment_rate'] = sub_df['mod_comment_text'].notna().mean(
            ) * 100
    else:
        features['mod_comment_rate'] = np.nan
    if 'post_type' in sub_df.columns:
        post_types = sub_df['post_type'].value_counts(normalize=True) * 100
        for ptype in ['text', 'link', 'image', 'video']:
            features[f'pct_{ptype}'] = post_types.get(ptype, 0)
    else:
        for ptype in ['text', 'link', 'image', 'video']:
            features[f'pct_{ptype}'] = np.nan
    if 'final_score' in sub_df.columns:
        features['median_score'] = sub_df['final_score'].median()
        features['mean_score'] = sub_df['final_score'].mean()
    else:
        features['median_score'] = np.nan
        features['mean_score'] = np.nan
    if 'final_num_comments' in sub_df.columns:
        features['median_comments'] = sub_df['final_num_comments'].median()
        features['mean_comments'] = sub_df['final_num_comments'].mean()
    else:
        features['median_comments'] = np.nan
        features['mean_comments'] = np.nan
    if 'mod_comment_text' in sub_df.columns:
        removed_with_comments = sub_df[sub_df['mod_comment_text'].notna()]
        if len(removed_with_comments) > 5:
            comments_list = removed_with_comments['mod_comment_text'].astype(
                str).tolist()
            total_comments = len(comments_list)
            spam_count = sum(1 for c in comments_list if 'spam' in c.lower())
            rule_count = sum(1 for c in comments_list if 'rule' in c.lower())
            civility_words = ['uncivil', 'rude', 'attack', 'harassment',
                'toxic']
            civility_count = sum(1 for c in comments_list if any(w in c.
                lower() for w in civility_words))
            features['pct_spam_mentions'] = spam_count / total_comments * 100
            features['pct_rule_mentions'] = rule_count / total_comments * 100
            features['pct_civility_mentions'
                ] = civility_count / total_comments * 100
        else:
            features['pct_spam_mentions'] = np.nan
            features['pct_rule_mentions'] = np.nan
            features['pct_civility_mentions'] = np.nan
    else:
        features['pct_spam_mentions'] = np.nan
        features['pct_rule_mentions'] = np.nan
        features['pct_civility_mentions'] = np.nan
    if 'title_toxicity' in sub_df.columns:
        features['mean_title_toxicity'] = sub_df['title_toxicity'].mean()
        features['median_title_toxicity'] = sub_df['title_toxicity'].median()
    else:
        features['mean_title_toxicity'] = np.nan
        features['median_title_toxicity'] = np.nan
    if 'selftext_toxicity' in sub_df.columns:
        features['mean_selftext_toxicity'] = sub_df['selftext_toxicity'].mean()
        features['median_selftext_toxicity'] = sub_df['selftext_toxicity'
            ].median()
    else:
        features['mean_selftext_toxicity'] = np.nan
        features['median_selftext_toxicity'] = np.nan
    if 'title_sentiment_polarity' in sub_df.columns:
        features['mean_title_sentiment'] = sub_df['title_sentiment_polarity'
            ].mean()
        features['median_title_sentiment'] = sub_df['title_sentiment_polarity'
            ].median()
    else:
        features['mean_title_sentiment'] = np.nan
        features['median_title_sentiment'] = np.nan
    if 'selftext_sentiment_polarity' in sub_df.columns:
        features['mean_selftext_sentiment'] = sub_df[
            'selftext_sentiment_polarity'].mean()
        features['median_selftext_sentiment'] = sub_df[
            'selftext_sentiment_polarity'].median()
    else:
        features['mean_selftext_sentiment'] = np.nan
        features['median_selftext_sentiment'] = np.nan
    if 'title_readability_flesch_reading_ease' in sub_df.columns:
        features['mean_title_readability'] = sub_df[
            'title_readability_flesch_reading_ease'].mean()
        features['median_title_readability'] = sub_df[
            'title_readability_flesch_reading_ease'].median()
    else:
        features['mean_title_readability'] = np.nan
        features['median_title_readability'] = np.nan
    if 'selftext_readability_flesch_reading_ease' in sub_df.columns:
        features['mean_selftext_readability'] = sub_df[
            'selftext_readability_flesch_reading_ease'].mean()
        features['median_selftext_readability'] = sub_df[
            'selftext_readability_flesch_reading_ease'].median()
    else:
        features['mean_selftext_readability'] = np.nan
        features['median_selftext_readability'] = np.nan
    boolean_features = ['title_has_political', 'title_has_profanity',
        'title_has_controversial', 'selftext_has_political',
        'selftext_has_profanity', 'selftext_has_controversial',
        'title_has_emotion_intense', 'selftext_has_emotion_intense',
        'title_has_questioning_authority', 'selftext_has_questioning_authority'
        ]
    for bool_feat in boolean_features:
        if bool_feat in sub_df.columns:
            features[f'pct_{bool_feat}'] = sub_df[bool_feat].mean() * 100
        else:
            features[f'pct_{bool_feat}'] = np.nan
    subreddit_features[subreddit] = features
feature_df = pd.DataFrame(subreddit_features).T
feature_df.index.name = 'subreddit'
print(
    f"""
✓ Built feature matrix: {len(feature_df)} subreddits × {len(feature_df.columns)} features"""
    )
print(f"""
Features included:""")
for col in feature_df.columns:
    missing_pct = feature_df[col].isna().sum() / len(feature_df) * 100
    print(f'   - {col:<30} (missing: {missing_pct:>5.1f}%)')
    feature_missingness[col] = missing_pct
print(f"""
📊 Feature Missingness Summary:""")
HIGH_MISSING_THRESHOLD = 20.0
high_missing = {k: v for k, v in feature_missingness.items() if v >
    HIGH_MISSING_THRESHOLD}
if high_missing:
    print(f'   ⚠️  High missingness (>{HIGH_MISSING_THRESHOLD:.1f}%):')
    for feat, pct in high_missing.items():
        print(f'      - {feat}: {pct:.1f}%')
else:
    print(
        f'   ✓ All features have <{HIGH_MISSING_THRESHOLD:.1f}% missing values'
        )
features_to_drop = [k for k, v in feature_missingness.items() if v >
    HIGH_MISSING_THRESHOLD]
if features_to_drop:
    feature_df = feature_df.drop(columns=features_to_drop)
    print(
        f'   🗑️  Dropped {len(features_to_drop)} features with >{HIGH_MISSING_THRESHOLD:.1f}% missingness.'
        )
print("""
🔧 Applying logic-aware imputation and feature selection...""")
rate_cols = [c for c in feature_df.columns if c.startswith('pct_') or 
    'rate' in c]
feature_df[rate_cols] = feature_df[rate_cols].fillna(0.0)
demo_cols = [c for c in feature_df.columns if c not in rate_cols]
feature_df[demo_cols] = feature_df[demo_cols].fillna(feature_df[demo_cols].
    median())
cols_to_drop = [c for c in feature_df.columns if c.startswith('mean_') or c
    .endswith('_std')]
if cols_to_drop:
    print(
        f'   ✂️  Dropping {len(cols_to_drop)} redundant features (means/stds) to prevent weighting bias.'
        )
    feature_df = feature_df.drop(columns=cols_to_drop)
feature_df = feature_df.fillna(0)
feature_df = feature_df.loc[:, feature_df.std() > 0]
print(
    f"""
✓ After removing constant features: {len(feature_df.columns)} features"""
    )
n_subs = len(feature_df)
if n_subs < 3:
    print(
        f'❌ Not enough subreddits for meaningful clustering (got {n_subs}, need at least 3).'
        )
    exit(1)
if feature_df.shape[1] == 0:
    print(
        '❌ All features are constant after cleaning; no usable features remain.'
        )
    print('   Check your input data or missingness thresholds.')
    exit(1)
print('\n' + '=' * 70)
print('FEATURE ANALYSIS')
print('=' * 70)
print("""
📊 Analyzing feature variance...""")
if args.scaler == 'robust':
    temp_scaler = RobustScaler()
else:
    temp_scaler = StandardScaler()
scaled_features = temp_scaler.fit_transform(feature_df)
feature_variance = np.var(scaled_features, axis=0)
variance_df = pd.DataFrame({'feature': feature_df.columns, 'variance':
    feature_variance}).sort_values('variance', ascending=False)
print(f"""
Top 10 features by variance:""")
print(variance_df.head(10).to_string(index=False))
print("""
🔍 Running PCA for feature importance...""")
pca = PCA()
pca.fit(scaled_features)
explained_variance_ratio = pca.explained_variance_ratio_
n_show = min(5, len(explained_variance_ratio))
cumulative_variance = np.cumsum(explained_variance_ratio)
print(f"""
📊 Explained Variance (Top {n_show} PCs):""")
for i in range(n_show):
    print(
        f'   PC {i + 1}: {explained_variance_ratio[i]:.4f} (Cumulative: {cumulative_variance[i]:.4f})'
        )
if len(explained_variance_ratio) >= 3:
    print(
        f'   Total variance explained by first 3 PCs: {cumulative_variance[2]:.4f}'
        )
n_components_to_show = min(3, len(feature_df.columns))
feature_importance = np.abs(pca.components_[:n_components_to_show]).sum(axis=0)
feature_importance = feature_importance / feature_importance.sum()
importance_df = pd.DataFrame({'feature': feature_df.columns, 'importance':
    feature_importance}).sort_values('importance', ascending=False)
print(f"""
Top 10 most important features (by PCA contribution):""")
print(importance_df.head(10).to_string(index=False))
print('\n' + '=' * 70)
print('STANDARDIZING FEATURES')
print('=' * 70)
print(f"""
⚙️  Scaling features using {args.scaler} scaler...""")
if args.scaler == 'robust':
    scaler = RobustScaler()
else:
    scaler = StandardScaler()
feature_matrix = scaler.fit_transform(feature_df)
print('✓ Features standardized')
if not AUTO_CLUSTERS:
    if N_CLUSTERS < 2:
        raise ValueError(
            'n_clusters must be at least 2 for silhouette analysis.')
    if N_CLUSTERS > n_subs:
        raise ValueError(
            f'n_clusters ({N_CLUSTERS}) cannot exceed number of subreddits ({n_subs}).'
            )
print('\n' + '=' * 70)
print('OUTLIER DETECTION')
print('=' * 70)
print("""
🔍 Detecting outliers...""")
outlier_mask, outlier_scores = detect_outliers(feature_matrix, method=args.
    outlier_method, contamination=args.outlier_fraction, random_state=
    RANDOM_STATE)
outlier_subreddits = feature_df.index[outlier_mask].tolist()
print(f"""
✓ Found {sum(outlier_mask)} outlier subreddits:""")
if len(outlier_subreddits) > 0:
    for sub in outlier_subreddits:
        score = outlier_scores[feature_df.index.get_loc(sub)]
        print(f'   - {sub} (anomaly score: {score:.3f})')
else:
    print('   (none detected)')
print('\n' + '=' * 70)
print('CLUSTER VALIDATION')
print('=' * 70)
if AUTO_CLUSTERS:
    print('\n🎯 Finding optimal number of clusters...')
    K_range, inertias, silhouettes = find_optimal_clusters(feature_matrix,
        max_k=min(10, len(feature_df) // 3))
    best_k_idx = np.argmax(silhouettes)
    N_CLUSTERS = K_range[best_k_idx]
    print(f'\n✓ Optimal clusters detected: {N_CLUSTERS}')
    print(f'   Silhouette score: {silhouettes[best_k_idx]:.3f}')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    ax1.plot(K_range, inertias, 'bo-', linewidth=2, markersize=8)
    ax1.axvline(N_CLUSTERS, color='red', linestyle='--', alpha=0.7, label=
        f'Selected k={N_CLUSTERS}')
    ax1.set_xlabel('Number of Clusters (k)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Inertia (Within-cluster Sum of Squares)', fontsize=12,
        fontweight='bold')
    ax1.set_title('Elbow Method', fontsize=14, fontweight='bold')
    ax1.grid(alpha=0.3)
    ax1.legend()
    ax2.plot(K_range, silhouettes, 'go-', linewidth=2, markersize=8)
    ax2.axvline(N_CLUSTERS, color='red', linestyle='--', alpha=0.7, label=
        f'Selected k={N_CLUSTERS}')
    ax2.set_xlabel('Number of Clusters (k)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Silhouette Score', fontsize=12, fontweight='bold')
    ax2.set_title('Silhouette Analysis', fontsize=14, fontweight='bold')
    ax2.grid(alpha=0.3)
    ax2.legend()
    plt.tight_layout()
    plt.savefig(f'{output_dir}/cluster_validation.png', dpi=300,
        bbox_inches='tight')
    print(f'✓ Saved: cluster_validation.png')
    plt.close()
else:
    print(f'\n🎯 Using specified number of clusters: {N_CLUSTERS}')
print('\n' + '=' * 70)
print('COMPUTING UMAP EMBEDDINGS')
print('=' * 70)
print(f"""
🧮 UMAP parameters:""")
print(f'   n_neighbors: {args.n_neighbors}')
print(f'   min_dist: {args.min_dist}')
print(f'   random_state: {RANDOM_STATE}')
print("""
📐 Computing 2D embeddings (Optimized for visualization)...""")
umap_2d = umap.UMAP(n_components=2, n_neighbors=args.n_neighbors, min_dist=
    args.min_dist, random_state=RANDOM_STATE, metric='euclidean')
embeddings_2d = umap_2d.fit_transform(feature_matrix)
print('✓ 2D embeddings computed')
print("""
📐 Computing 3D embeddings...""")
umap_3d = umap.UMAP(n_components=3, n_neighbors=args.n_neighbors, min_dist=
    args.min_dist, random_state=RANDOM_STATE, metric='euclidean')
embeddings_3d = umap_3d.fit_transform(feature_matrix)
print('✓ 3D embeddings computed')
print('\n' + '=' * 70)
print('CLUSTER ANALYSIS (HDBSCAN)')
print('=' * 70)
import hdbscan
print(f"""
🎯 Running HDBSCAN clustering...""")
print(
    '   (Parameters tuned for higher sensitivity to find smaller sub-communities)'
    )
clusterer = hdbscan.HDBSCAN(min_cluster_size=5, min_samples=2,
    cluster_selection_epsilon=0.1, gen_min_span_tree=True)
clusters = clusterer.fit_predict(embeddings_2d)
clusters = clusters + 1
N_CLUSTERS = len(set(clusters))
print(f'   ✓ HDBSCAN found {N_CLUSTERS} groups total')
print(f'   ✓ {sum(clusters == 0)} subreddits classified as Noise (Cluster 0)')
print("""
📊 Computing Cluster Quality...""")
silhouette_vals = np.zeros(len(feature_matrix))
if N_CLUSTERS > 2:
    real_data_mask = clusters > 0
    real_silhouettes = silhouette_samples(embeddings_2d[real_data_mask],
        clusters[real_data_mask])
    silhouette_vals[real_data_mask] = real_silhouettes
    silhouette_avg = real_silhouettes.mean()
    calinski_score = calinski_harabasz_score(embeddings_2d[real_data_mask],
        clusters[real_data_mask])
    print(f'   Silhouette Score (Real Clusters Only): {silhouette_avg:.3f}')
    print(f'   Calinski-Harabasz: {calinski_score:.1f}')
else:
    print(
        '⚠️  Not enough distinct clusters found to calculate Silhouette score.'
        )
    silhouette_avg = 0
    calinski_score = 0
print("""
📊 Cluster distribution:""")
for cluster in sorted(list(set(clusters))):
    count = (clusters == cluster).sum()
    pct = count / len(clusters) * 100
    label = 'Noise' if cluster == 0 else f'Cluster {cluster}'
    print(f'   {label:<10}: {count:>3} subreddits ({pct:>4.1f}%)')
print('\n' + '=' * 70)
print('COMPUTING SIMILARITY METRICS')
print('=' * 70)
print("""
🔢 Computing cosine similarity matrix...""")
cosine_sim = cosine_similarity(feature_matrix)
print('🔢 Computing euclidean distance matrix...')
euclidean_dist = squareform(pdist(feature_matrix, metric='euclidean'))
euclidean_sim = 1 / (1 + euclidean_dist)
print('✓ Similarity matrices computed')
print('\n' + '=' * 70)
print('CREATING VISUALIZATIONS')
print('=' * 70)
plot_df = pd.DataFrame({'subreddit': feature_df.index, 'x': embeddings_2d[:,
    0], 'y': embeddings_2d[:, 1], 'cluster': clusters, 'cluster_label': 
    'Cluster ' + pd.Series(clusters).astype(str), 'removal_rate':
    feature_df['removal_rate'].values, 'total_posts': feature_df[
    'total_posts'].values, 'silhouette': silhouette_vals, 'is_outlier':
    outlier_mask})
print("""
📊 Creating static 2D visualization...""")
fig, ax = plt.subplots(figsize=(14, 10))
colors = plt.cm.Set3(np.linspace(0, 1, N_CLUSTERS))
for cluster in range(N_CLUSTERS):
    cluster_data = plot_df[plot_df['cluster'] == cluster]
    ax.scatter(cluster_data['x'], cluster_data['y'], c=[colors[cluster]],
        label=f'Cluster {cluster} (n={len(cluster_data)})', s=100, alpha=
        0.7, edgecolors='black', linewidth=0.5)
if sum(outlier_mask) > 0:
    outlier_data = plot_df[plot_df['is_outlier']]
    ax.scatter(outlier_data['x'], outlier_data['y'], c='red', marker='x', s
        =200, linewidths=3, label='Outliers', zorder=10)
for idx, row in plot_df.iterrows():
    if row['is_outlier'] or row['total_posts'] > plot_df['total_posts'
        ].quantile(0.9):
        ax.annotate(row['subreddit'], (row['x'], row['y']), fontsize=9, ha=
            'center', va='bottom', alpha=0.8, fontweight='bold' if row[
            'is_outlier'] else 'normal')
ax.set_xlabel('UMAP Dimension 1', fontsize=12, fontweight='bold')
ax.set_ylabel('UMAP Dimension 2', fontsize=12, fontweight='bold')
ax.set_title(f'Subreddit Similarity Map (2D UMAP, k={N_CLUSTERS})',
    fontsize=14, fontweight='bold')
ax.legend(loc='best', frameon=True, fontsize=10)
ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(f'{output_dir}/subreddit_embeddings_2d.png', dpi=300,
    bbox_inches='tight')
print('✓ Saved: subreddit_embeddings_2d.png')
plt.close()
print("""
🌐 Creating interactive 2D visualization...""")
fig = px.scatter(plot_df, x='x', y='y', color='removal_rate', size=
    'total_posts', hover_data=['subreddit', 'removal_rate', 'cluster_label',
    'silhouette'], text='subreddit', color_continuous_scale='Reds', title=
    f'Interactive Subreddit Map (sized by post count, colored by removal rate)'
    , labels={'x': 'UMAP Dimension 1', 'y': 'UMAP Dimension 2',
    'removal_rate': 'Removal Rate (%)', 'silhouette': 'Silhouette Score'})
fig.update_traces(textposition='top center', textfont_size=10)
fig.update_layout(height=800, width=1200)
fig.write_html(f'{output_dir}/subreddit_embeddings_2d_interactive.html')
print('✓ Saved: subreddit_embeddings_2d_interactive.html')
print("""
🌐 Creating interactive 3D visualization...""")
plot_df_3d = pd.DataFrame({'subreddit': feature_df.index, 'x':
    embeddings_3d[:, 0], 'y': embeddings_3d[:, 1], 'z': embeddings_3d[:, 2],
    'cluster': clusters, 'cluster_label': 'Cluster ' + pd.Series(clusters).
    astype(str), 'removal_rate': feature_df['removal_rate'].values,
    'total_posts': feature_df['total_posts'].values, 'silhouette':
    silhouette_vals})
fig_3d = px.scatter_3d(plot_df_3d, x='x', y='y', z='z', color=
    'cluster_label', hover_data=['subreddit', 'removal_rate', 'total_posts',
    'silhouette'], text='subreddit', title=
    f'3D Subreddit Similarity Map (k={N_CLUSTERS})', labels={'x':
    'Dimension 1', 'y': 'Dimension 2', 'z': 'Dimension 3'})
fig_3d.update_traces(textposition='top center', textfont_size=8)
fig_3d.update_layout(height=800, width=1200)
fig_3d.write_html(f'{output_dir}/subreddit_embeddings_3d.html')
print('✓ Saved: subreddit_embeddings_3d.html')
print("""
🌳 Creating hierarchical clustering dendrogram...""")
fig, ax = plt.subplots(figsize=(14, 8))
linkage_matrix = linkage(feature_matrix, method='ward')
dendrogram(linkage_matrix, labels=feature_df.index.tolist(), ax=ax,
    leaf_font_size=10, color_threshold=0.7 * max(linkage_matrix[:, 2]))
ax.set_xlabel('Subreddit', fontsize=12, fontweight='bold')
ax.set_ylabel('Ward Distance', fontsize=12, fontweight='bold')
ax.set_title('Hierarchical Clustering of Subreddits', fontsize=14,
    fontweight='bold')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(f'{output_dir}/hierarchical_clustering.png', dpi=300,
    bbox_inches='tight')
print('✓ Saved: hierarchical_clustering.png')
plt.close()
print("""
🔥 Creating similarity matrix (cosine)...""")
fig, ax = plt.subplots(figsize=(12, 10))
sns.heatmap(cosine_sim, xticklabels=feature_df.index, yticklabels=
    feature_df.index, cmap='YlOrRd', linewidths=0.5, cbar_kws={'label':
    'Cosine Similarity'}, ax=ax, vmin=0, vmax=1)
ax.set_title('Subreddit Similarity Matrix (Cosine)', fontsize=14,
    fontweight='bold')
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
plt.savefig(f'{output_dir}/similarity_matrix_cosine.png', dpi=300,
    bbox_inches='tight')
print('✓ Saved: similarity_matrix_cosine.png')
plt.close()
print("""
📊 Creating feature importance visualization...""")
fig, ax = plt.subplots(figsize=(10, 8))
top_features = importance_df.head(15)
ax.barh(range(len(top_features)), top_features['importance'], color=
    'steelblue', edgecolor='black')
ax.set_yticks(range(len(top_features)))
ax.set_yticklabels(top_features['feature'])
ax.set_xlabel('Importance (PCA Contribution)', fontsize=12, fontweight='bold')
ax.set_title('Top 15 Most Important Features Driving Dimensionality',
    fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(f'{output_dir}/feature_importance.png', dpi=300, bbox_inches=
    'tight')
print('✓ Saved: feature_importance.png')
plt.close()
print("""
📊 Creating silhouette analysis plot...""")
fig, ax = plt.subplots(figsize=(10, 8))
y_lower = 10
for i in range(N_CLUSTERS):
    cluster_silhouette_vals = silhouette_vals[clusters == i]
    cluster_silhouette_vals.sort()
    size_cluster_i = cluster_silhouette_vals.shape[0]
    y_upper = y_lower + size_cluster_i
    color = plt.cm.Set3(i / N_CLUSTERS)
    ax.fill_betweenx(np.arange(y_lower, y_upper), 0,
        cluster_silhouette_vals, facecolor=color, edgecolor=color, alpha=0.7)
    ax.text(-0.05, y_lower + 0.5 * size_cluster_i, f'C{i}')
    y_lower = y_upper + 10
ax.set_xlabel('Silhouette Coefficient', fontsize=12, fontweight='bold')
ax.set_ylabel('Cluster', fontsize=12, fontweight='bold')
ax.set_title(f'Silhouette Analysis (avg={silhouette_avg:.3f})', fontsize=14,
    fontweight='bold')
ax.axvline(silhouette_avg, color='red', linestyle='--', linewidth=2, label=
    f'Average ({silhouette_avg:.3f})')
ax.set_yticks([])
ax.legend()
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(f'{output_dir}/silhouette_analysis.png', dpi=300, bbox_inches=
    'tight')
print('✓ Saved: silhouette_analysis.png')
plt.close()
if sum(outlier_mask) > 0:
    print('\n🔍 Creating outlier analysis...')
    fig, ax = plt.subplots(figsize=(12, 6))
    outlier_df = feature_df[outlier_mask]
    normal_df = feature_df[~outlier_mask]
    metrics_to_compare = ['removal_rate', 'total_posts', 'mod_comment_rate']
    metrics_to_compare = [m for m in metrics_to_compare if m in feature_df.
        columns]
    x = np.arange(len(metrics_to_compare))
    width = 0.35
    outlier_means = [outlier_df[m].mean() for m in metrics_to_compare]
    normal_means = [normal_df[m].mean() for m in metrics_to_compare]
    ax.bar(x - width / 2, normal_means, width, label='Normal', color=
        'steelblue', edgecolor='black')
    ax.bar(x + width / 2, outlier_means, width, label='Outliers', color=
        'coral', edgecolor='black')
    ax.set_xlabel('Metric', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average Value', fontsize=12, fontweight='bold')
    ax.set_title('Outlier vs Normal Subreddit Comparison', fontsize=14,
        fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(metrics_to_compare, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/outlier_analysis.png', dpi=300, bbox_inches=
        'tight')
    print('✓ Saved: outlier_analysis.png')
    plt.close()
print('\n' + '=' * 70)
print('CLUSTER CHARACTERISTICS')
print('=' * 70)
cluster_profiles = []
for cluster in range(N_CLUSTERS):
    cluster_mask = clusters == cluster
    cluster_subs = feature_df[cluster_mask]
    cluster_silhouettes = silhouette_vals[cluster_mask]
    profile = {'cluster': cluster, 'n_subreddits': len(cluster_subs),
        'subreddits': ', '.join(cluster_subs.index.tolist()),
        'avg_silhouette': cluster_silhouettes.mean(), 'avg_removal_rate':
        cluster_subs['removal_rate'].mean(), 'std_removal_rate':
        cluster_subs['removal_rate'].std(), 'avg_total_posts': cluster_subs
        ['total_posts'].mean()}
    for col in ['median_account_age', 'median_karma', 'mod_comment_rate',
        'mean_toxicity']:
        if col in cluster_subs.columns:
            profile[f'avg_{col}'] = cluster_subs[col].mean()
            profile[f'std_{col}'] = cluster_subs[col].std()
    cluster_profiles.append(profile)
cluster_profile_df = pd.DataFrame(cluster_profiles)
print("""
📊 Cluster Profiles:""")
for _, row in cluster_profile_df.iterrows():
    print(f"\n{'=' * 60}")
    print(
        f"Cluster {int(row['cluster'])} (Silhouette: {row['avg_silhouette']:.3f})"
        )
    print(f"{'=' * 60}")
    print(f"   Subreddits ({int(row['n_subreddits'])}): {row['subreddits']}")
    print(
        f"   Removal Rate: {row['avg_removal_rate']:.1f}% (±{row['std_removal_rate']:.1f})"
        )
    if 'avg_median_account_age' in row.index:
        print(
            f"   Account Age: {row['avg_median_account_age']:.0f} days (±{row['std_median_account_age']:.0f})"
            )
    if 'avg_mod_comment_rate' in row.index:
        print(
            f"   Mod Comment Rate: {row['avg_mod_comment_rate']:.1f}% (±{row['std_mod_comment_rate']:.1f})"
            )
    if 'avg_mean_toxicity' in row.index:
        print(
            f"   Thread Toxicity: {row['avg_mean_toxicity']:.3f} (±{row['std_mean_toxicity']:.3f})"
            )
print('\n' + '=' * 70)
print('CLUSTER INTERPRETATION')
print('=' * 70)
print("""
🔍 Identifying distinguishing features per cluster...""")
cluster_interpretations = []
for cluster in range(N_CLUSTERS):
    cluster_mask = clusters == cluster
    cluster_features = feature_df[cluster_mask]
    other_features = feature_df[~cluster_mask]
    differences = {}
    for col in feature_df.columns:
        cluster_mean = cluster_features[col].mean()
        other_mean = other_features[col].mean()
        diff = cluster_mean - other_mean
        pooled_std = feature_df[col].std()
        if pooled_std > 0:
            effect_size = diff / pooled_std
            differences[col] = effect_size
    sorted_diffs = sorted(differences.items(), key=lambda x: abs(x[1]),
        reverse=True)
    top_features = sorted_diffs[:5]
    interpretation = {'cluster': cluster, 'distinguishing_features': ', '.
        join([f'{feat} ({eff:+.2f}σ)' for feat, eff in top_features])}
    cluster_interpretations.append(interpretation)
    print(f'\nCluster {cluster} - Distinguishing Features:')
    for feat, eff in top_features:
        direction = 'higher' if eff > 0 else 'lower'
        print(f'   • {feat}: {abs(eff):.2f}σ {direction} than average')
print('\n' + '=' * 70)
print('SAVING RESULTS')
print('=' * 70)
print("""
💾 Saving embeddings...""")
embeddings_df = pd.DataFrame({'subreddit': feature_df.index, 'umap_x':
    embeddings_2d[:, 0], 'umap_y': embeddings_2d[:, 1], 'umap_z':
    embeddings_3d[:, 2], 'cluster': clusters, 'silhouette_score':
    silhouette_vals, 'is_outlier': outlier_mask, 'outlier_score':
    outlier_scores})
embeddings_df.to_csv(f'{output_dir}/embeddings_with_clusters.csv', index=False)
print('✓ Saved: embeddings_with_clusters.csv')
print("""
💾 Saving similarity matrices...""")
cosine_df = pd.DataFrame(cosine_sim, index=feature_df.index, columns=
    feature_df.index)
cosine_df.to_csv(f'{output_dir}/similarity_matrix_cosine.csv')
print('✓ Saved: similarity_matrix_cosine.csv')
euclidean_df = pd.DataFrame(euclidean_sim, index=feature_df.index, columns=
    feature_df.index)
euclidean_df.to_csv(f'{output_dir}/similarity_matrix_euclidean.csv')
print('✓ Saved: similarity_matrix_euclidean.csv')
print("""
💾 Saving cluster profiles...""")
cluster_profile_df.to_csv(f'{output_dir}/cluster_profiles.csv', index=False)
print('✓ Saved: cluster_profiles.csv')
interpretation_df = pd.DataFrame(cluster_interpretations)
interpretation_df.to_csv(f'{output_dir}/cluster_interpretations.csv', index
    =False)
print('✓ Saved: cluster_interpretations.csv')
print("""
💾 Saving feature importance...""")
importance_df.to_csv(f'{output_dir}/feature_importance.csv', index=False)
print('✓ Saved: feature_importance.csv')
variance_df.to_csv(f'{output_dir}/feature_variance.csv', index=False)
print('✓ Saved: feature_variance.csv')
print("""
💾 Saving feature matrix...""")
feature_df.to_csv(f'{output_dir}/subreddit_features.csv')
print('✓ Saved: subreddit_features.csv')
if len(outlier_subreddits) > 0:
    outlier_details = feature_df.loc[outlier_subreddits].copy()
    outlier_details['subreddit'] = outlier_details.index
    outlier_details.to_csv(f'{output_dir}/outlier_subreddits.csv', index=False)
    print('✓ Saved: outlier_subreddits.csv')
metadata = {'analysis_date': datetime.now().isoformat(), 'n_subreddits':
    len(feature_df), 'n_features': len(feature_df.columns), 'n_clusters':
    N_CLUSTERS, 'cluster_method': 'k-means', 'auto_clusters': AUTO_CLUSTERS,
    'silhouette_score': float(silhouette_avg), 'calinski_harabasz_score':
    float(calinski_score), 'n_outliers': int(sum(outlier_mask)),
    'umap_params': {'n_neighbors': args.n_neighbors, 'min_dist': args.
    min_dist, 'random_state': RANDOM_STATE}, 'scaler': args.scaler,
    'min_posts_threshold': args.min_posts}
with open(f'{output_dir}/analysis_metadata.json', 'w') as f:
    json.dump(metadata, f, indent=2)
print('✓ Saved: analysis_metadata.json')
print('\n' + '=' * 70)
print('VALIDATION REPORT')
print('=' * 70)
validation_report = []
validation_report.append("""
📊 Statistical Validation:""")
validation_report.append(f'   • Silhouette Score: {silhouette_avg:.3f}')
validation_report.append(
    f"     - Interpretation: {'Good' if silhouette_avg > 0.5 else 'Fair' if silhouette_avg > 0.25 else 'Poor'}"
    )
validation_report.append(f'   • Calinski-Harabasz Score: {calinski_score:.1f}')
validation_report.append(
    f'     - Higher is better (measures cluster separation)')
validation_report.append(f"""
🎯 Cluster Quality:""")
for cluster in range(N_CLUSTERS):
    cluster_sil = silhouette_vals[clusters == cluster].mean()
    validation_report.append(
        f'   • Cluster {cluster}: {cluster_sil:.3f} silhouette')
validation_report.append(f"""
⚠️  Potential Issues:""")
issues = []
if silhouette_avg < 0.25:
    issues.append(
        'Low overall silhouette score - clusters may not be well-separated')
if any(silhouette_vals[clusters == i].mean() < 0 for i in range(N_CLUSTERS)):
    issues.append(
        'Some clusters have negative silhouette scores - may indicate misclassification'
        )
if sum(outlier_mask) > len(feature_df) * 0.2:
    issues.append(
        f'High outlier rate ({sum(outlier_mask) / len(feature_df) * 100:.1f}%) - consider investigating'
        )
if not issues:
    validation_report.append('   ✓ No major issues detected')
else:
    for issue in issues:
        validation_report.append(f'   • {issue}')
for line in validation_report:
    print(line)
with open(f'{output_dir}/validation_report.txt', 'w') as f:
    f.write('\n'.join(validation_report))
print(f"""
✓ Saved: validation_report.txt""")
print('\n' + '=' * 70)
print('✅ ENHANCED UMAP ANALYSIS COMPLETE!')
print('=' * 70)
print(f'\n📊 Summary:')
print(f'   Subreddits analyzed: {len(feature_df)}')
print(f'   Features used: {len(feature_df.columns)}')
print(
    f"   Clusters identified: {N_CLUSTERS} {'(auto-detected)' if AUTO_CLUSTERS else '(specified)'}"
    )
print(f'   Outliers detected: {sum(outlier_mask)}')
print(f'   Overall quality: {silhouette_avg:.3f} silhouette score')
print(f'\n💡 Key Findings:')
for _, row in cluster_profile_df.iterrows():
    print(f"   • Cluster {int(row['cluster'])}: {row['subreddits']}")
    print(f"     - Avg removal rate: {row['avg_removal_rate']:.1f}%")
    print(f"     - Cluster quality: {row['avg_silhouette']:.3f} silhouette")
if len(outlier_subreddits) > 0:
    print(f'\n🔍 Outlier Subreddits:')
    for sub in outlier_subreddits:
        print(f'   • {sub}')
print('\n' + '=' * 70)
