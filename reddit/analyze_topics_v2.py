import argparse
import pandas as pd
import numpy as np
import os
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
import glob
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
import psutil
import warnings
import re
import json
from datetime import datetime
from difflib import SequenceMatcher
from tqdm import tqdm
import hashlib
warnings.filterwarnings('ignore')
try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = 14, 8


def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(BASE_DIR, '..', 'data')
    data_dir = os.path.abspath(data_dir)
    parser = argparse.ArgumentParser(description=
        'Enhanced BERTopic analysis for moderator comments')
    parser.add_argument('--min-topic-size', type=int, default=15, help=
        'Minimum topic size (default: 15)')
    parser.add_argument('--n-topics-show', type=int, default=15, help=
        'Number of topics to display (default: 15)')
    parser.add_argument('--min-comment-length', type=int, default=50, help=
        'Minimum comment length in characters (default: 50)')
    parser.add_argument('--remove-templates', action='store_true', help=
        'Detect and remove templated comments')
    parser.add_argument('--template-threshold', type=float, default=0.85,
        help='Similarity threshold for template detection (default: 0.85)')
    parser.add_argument('--remove-duplicates', action='store_true', help=
        'Remove near-duplicate comments')
    parser.add_argument('--duplicate-threshold', type=float, default=0.95,
        help='Similarity threshold for duplicate detection (default: 0.95)')
    parser.add_argument('--data-dir', type=str, default=data_dir, help=
        f'Data directory (default: {data_dir})')
    parser.add_argument('--stability-runs', type=int, default=0, help=
        'Number of stability analysis runs (default: 0, disabled)')
    parser.add_argument('--non-interactive', action='store_true', help=
        'Do not prompt for input; proceed with warnings')
    args = parser.parse_args()
    MIN_TOPIC_SIZE = args.min_topic_size
    N_TOPICS_TO_SHOW = args.n_topics_show
    MIN_COMMENT_LENGTH = args.min_comment_length
    REMOVE_TEMPLATES = args.remove_templates
    TEMPLATE_THRESHOLD = args.template_threshold
    REMOVE_DUPLICATES = args.remove_duplicates
    DUPLICATE_THRESHOLD = args.duplicate_threshold
    STABILITY_RUNS = args.stability_runs


    class TextPreprocessor:

        @staticmethod
        def clean_text(text):
            if pd.isna(text) or not text:
                return ''
            text = str(text)
            text = re.sub(
                'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\\\(\\\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
                , '[URL]', text)
            text = re.sub('\\[([^\\]]+)\\]\\([^\\)]+\\)', '\\1', text)
            text = re.sub('/u/\\w+', '[USER]', text)
            text = re.sub('/r/\\w+', '[SUBREDDIT]', text)
            text = re.sub('r/\\w+', '[SUBREDDIT]', text)
            text = re.sub('\\*\\*([^\\*]+)\\*\\*', '\\1', text)
            text = re.sub('\\*([^\\*]+)\\*', '\\1', text)
            text = re.sub('`([^`]+)`', '\\1', text)
            text = re.sub('~~([^~]+)~~', '\\1', text)
            text = re.sub('\\s+', ' ', text)
            text = re.sub('\\n+', ' ', text)
            text = re.sub('([!?.]){2,}', '\\1', text)
            return text.strip()

        @staticmethod
        def extract_template_features(text):
            features = {}
            features['has_rule_mention'] = bool(re.search(
                '\\b(?:rule|r)\\s*\\d+\\b', text.lower()))
            features['has_removal_phrase'] = bool(re.search(
                '\\b(?:removed|deleted|violation|violates|breaks|against)\\b',
                text.lower()))
            features['starts_with_your'] = text.lower().startswith('your')
            features['has_placeholder'] = bool(re.search(
                '\\[.*?\\]|\\{.*?\\}', text))
            features['has_bullets'] = bool(re.search('^\\s*[-*•]\\s', text,
                re.MULTILINE))
            return features


    class AutomatedTemplateDetector:

        @staticmethod
        def is_template(text):
            if pd.isna(text) or not text:
                return True
            text = str(text).strip()
            text_lower = text.lower()
            indicators = {'very_long': len(text) > 800, 'many_urls': text.
                count('http') > 2, 'has_automod': 'automoderator' in
                text_lower or 'bot' in text_lower, 'has_sidebar': 'sidebar' in
                text_lower and 'rules' in text_lower, 'has_wiki': 'wiki' in
                text_lower and ('read' in text_lower or 'check' in
                text_lower), 'has_modmail': 'modmail' in text_lower or 
                'message the moderators' in text_lower, 'has_contact_mod': 
                'contact' in text_lower and 'moderators' in text_lower,
                'has_appeals': 'appeal' in text_lower and ('message' in
                text_lower or 'contact' in text_lower), 'has_placeholder':
                bool(re.search('\\[.*?\\]|\\{.*?\\}', text)),
                'has_multiple_bullets': text.count('* ') > 3 or text.count(
                '- ') > 3, 'very_short': len(text) < 30,
                'boilerplate_start': any(text_lower.startswith(phrase) for
                phrase in ['your post has been removed',
                'your submission has been removed',
                'this post has been removed',
                'your comment has been removed',
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


    class TemplateDetector:

        def __init__(self, threshold=0.85):
            self.threshold = threshold
            self.templates = []

        def detect_templates(self, texts):
            print(f'\n🔍 Detecting templates (threshold: {self.threshold})...')
            vectorizer = TfidfVectorizer(max_features=500, ngram_range=(2, 
                4), min_df=2)
            try:
                tfidf_matrix = vectorizer.fit_transform(texts)
            except ValueError:
                print('   ⚠️  Not enough variation for template detection')
                return np.zeros(len(texts), dtype=bool)
            n_docs = tfidf_matrix.shape[0]
            high_similarity_counts = np.zeros(n_docs, dtype=int)
            chunk_size = 512
            for start in range(0, n_docs, chunk_size):
                end = min(n_docs, start + chunk_size)
                chunk = tfidf_matrix[start:end]
                sim_chunk = cosine_similarity(chunk, tfidf_matrix)
                counts = (sim_chunk > self.threshold).sum(axis=1)
                for i, global_idx in enumerate(range(start, end)):
                    if sim_chunk[i, global_idx] > self.threshold:
                        counts[i] -= 1
                high_similarity_counts[start:end] = counts
            is_template = (high_similarity_counts >= 3) | (
                high_similarity_counts > len(texts) * 0.05)
            n_templates = is_template.sum()
            print(
                f'   ✓ Detected {n_templates} templated comments ({n_templates / len(texts) * 100:.1f}%)'
                )
            return is_template


    class DuplicateDetector:

        def __init__(self, threshold=0.95):
            self.threshold = threshold
            self.model = None
            if HAS_SENTENCE_TRANSFORMERS:
                try:
                    self.model = SentenceTransformer('all-MiniLM-L6-v2')
                    print(
                        '   ✓ Loaded SentenceTransformer for embedding-based deduplication'
                        )
                except Exception as e:
                    print(f'   ⚠️  Could not load SentenceTransformer: {e}')
                    self.model = None

        def find_duplicates(self, texts):
            print(f'\n🔍 Detecting duplicates (threshold: {self.threshold})...')
            if len(texts) == 0:
                return np.array([], dtype=bool)
            hashes = [hashlib.md5(t.encode()).hexdigest() for t in texts]
            hash_counts = Counter(hashes)
            exact_duplicates = [(hash_counts[h] > 1) for h in hashes]
            print(f'   ✓ Found {sum(exact_duplicates)} exact duplicates')
            keep_mask = np.ones(len(texts), dtype=bool)
            unique_indices = [i for i, is_dup in enumerate(exact_duplicates
                ) if not is_dup]
            if self.threshold < 1.0 and len(unique_indices) > 1:
                seen = set()
                for i, idx_i in enumerate(unique_indices):
                    if idx_i in seen:
                        continue
                    for idx_j in unique_indices[i + 1:]:
                        if idx_j in seen:
                            continue
                        sim = SequenceMatcher(None, texts[idx_i], texts[idx_j]
                            ).ratio()
                        if sim > self.threshold:
                            keep_mask[idx_j] = False
                            seen.add(idx_j)
            if self.model is not None and len(unique_indices) > 1:
                long_indices = [idx for idx in unique_indices if len(texts[
                    idx]) > 100 and keep_mask[idx]]
                if len(long_indices) > 1:
                    long_texts = [texts[i] for i in long_indices]
                    try:
                        embeddings = self.model.encode(long_texts,
                            batch_size=64, show_progress_bar=False,
                            convert_to_numpy=True)
                        norm = np.linalg.norm(embeddings, axis=1, keepdims=True
                            ) + 1e-08
                        emb_norm = embeddings / norm
                        chunk_size = 256
                        for start in range(0, len(long_indices), chunk_size):
                            end = min(len(long_indices), start + chunk_size)
                            chunk = emb_norm[start:end]
                            sim_chunk = np.dot(chunk, emb_norm.T)
                            for i, row_idx in enumerate(range(start, end)):
                                idx_i = long_indices[row_idx]
                                if not keep_mask[idx_i]:
                                    continue
                                similar = np.where(sim_chunk[i] > self.
                                    threshold)[0]
                                for j in similar:
                                    idx_j = long_indices[j]
                                    if idx_i == idx_j or not keep_mask[idx_j]:
                                        continue
                                    keep_mask[idx_j] = False
                    except Exception as e:
                        print(
                            f'   ⚠️  Embedding-based deduplication skipped due to error: {e}'
                            )
            for i, h in enumerate(hashes):
                if hash_counts[h] > 1 and i > 0:
                    prev_hash = hashes[:i]
                    if h in prev_hash:
                        keep_mask[i] = False
            n_removed = len(texts) - keep_mask.sum()
            print(
                f'   ✓ Marked {n_removed} total duplicates for removal ({n_removed / len(texts) * 100:.1f}%)'
                )
            return keep_mask


    class TopicQualityMetrics:

        @staticmethod
        def calculate_coherence(topic_model, documents):
            print('\n📊 Calculating topic coherence...')
            try:
                from gensim.models import CoherenceModel
                from gensim.corpora import Dictionary
                tokenized_docs = [doc.lower().split() for doc in documents]
                dictionary = Dictionary(tokenized_docs)
                topics = []
                topic_info = topic_model.get_topic_info()
                for topic_id in topic_info['Topic']:
                    if topic_id != -1:
                        topic_data = topic_model.get_topic(topic_id)
                        if topic_data:
                            topic_words = [str(word) for word, _ in
                                topic_data[:10]]
                            topics.append(topic_words)
                if not topics:
                    print(
                        '   ⚠️  No valid topics found for coherence calculation (only outliers).'
                        )
                    return 0.0, []
                coherence_model = CoherenceModel(topics=topics, texts=
                    tokenized_docs, dictionary=dictionary, coherence='c_v')
                coherence_score = coherence_model.get_coherence()
                print(f'   ✓ Topic coherence (c_v): {coherence_score:.4f}')
                coherence_per_topic = coherence_model.get_coherence_per_topic()
                return coherence_score, coherence_per_topic
            except ImportError:
                print(
                    '   ⚠️  gensim not available, using alternative coherence metric'
                    )
                return TopicQualityMetrics._simple_coherence(topic_model), []
            except Exception as e:
                print(f'   ⚠️  Coherence calculation failed: {e}')
                fallback = TopicQualityMetrics._simple_coherence(topic_model)
                print(
                    f'   → Falling back to simple coherence approximation: {fallback:.4f}'
                    )
                return fallback, []

        @staticmethod
        def _simple_coherence(topic_model):
            coherences = []
            for topic_id in range(len(topic_model.get_topic_info()) - 1):
                if topic_id >= 0:
                    words = [word for word, _ in topic_model.get_topic(
                        topic_id)[:10]]
                    avg_length = np.mean([len(w) for w in words])
                    coherences.append(avg_length)
            return np.mean(coherences) / 10

        @staticmethod
        def calculate_diversity(topic_model):
            print('\n📊 Calculating topic diversity...')
            all_words = set()
            total_words = 0
            for topic_id in range(len(topic_model.get_topic_info()) - 1):
                if topic_id >= 0:
                    words = [word for word, _ in topic_model.get_topic(
                        topic_id)[:10]]
                    all_words.update(words)
                    total_words += len(words)
            diversity = len(all_words) / total_words if total_words > 0 else 0
            print(
                f'   ✓ Topic diversity: {diversity:.4f} ({len(all_words)} unique words / {total_words} total)'
                )
            return diversity

        @staticmethod
        def calculate_outlier_ratio(topics):
            topics_arr = np.array(topics)
            outlier_ratio = (topics_arr == -1).sum() / len(topics_arr)
            return outlier_ratio

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
    if not args.non_interactive and not check_memory(min_gb=2):
        exit(0)
    print('=' * 70)
    print('🔬 ENHANCED BERTOPIC ANALYSIS FOR MODERATOR COMMENTS')
    print('=' * 70)
    print(f'\n⚙️  Configuration:')
    print(f'   Min topic size: {MIN_TOPIC_SIZE}')
    print(f'   Min comment length: {MIN_COMMENT_LENGTH}')
    print(f'   Remove templates: {REMOVE_TEMPLATES}')
    print(f'   Remove duplicates: {REMOVE_DUPLICATES}')
    if STABILITY_RUNS > 0:
        print(f'   Stability runs: {STABILITY_RUNS}')
    print('\n📂 Looking for data files...')
    data_files = glob.glob(f'{args.data_dir}/linguistics_boolean_fixed*.csv')
    if not data_files:
        data_files = glob.glob(os.path.join(args.data_dir,
            'combined_complete_*.csv'))
    if not data_files:
        print('❌ No combined data files found. Run collect_pass2.py first.')
        exit(1)
    latest_file = max(data_files, key=os.path.getctime)
    print(f'✓ Found: {latest_file}')
    print('\n📊 Loading data...')
    df = pd.read_csv(latest_file)
    print(f'✓ Loaded {len(df):,} posts')
    output_dir = 'results/topics'
    os.makedirs(output_dir, exist_ok=True)
    print('\n' + '=' * 70)
    print('DATA PREPARATION')
    print('=' * 70)
    if 'mod_comment_text' not in df.columns:
        print(
            "❌ 'mod_comment_text' column missing. Run Pass 2 with mod comment collection."
            )
        exit(1)
    posts_with_comments = df[df['mod_comment_text'].notna()].copy()
    posts_with_comments['is_template'] = posts_with_comments['mod_comment_text'
        ].apply(AutomatedTemplateDetector.is_template)
    n_template = posts_with_comments['is_template'].sum()
    n_human = (~posts_with_comments['is_template']).sum()
    print(f'\n✓ Found {len(posts_with_comments):,} posts with mod comments')
    print('\n🧹 Preprocessing text...')
    tqdm.pandas()
    preprocessor = TextPreprocessor()
    posts_with_comments['cleaned_text'] = posts_with_comments[
        'mod_comment_text'].progress_apply(preprocessor.clean_text)
    posts_with_comments['text_length'] = posts_with_comments['cleaned_text'
        ].str.len()
    posts_with_comments = posts_with_comments[posts_with_comments[
        'text_length'] >= MIN_COMMENT_LENGTH].copy()
    print(
        f'✓ After cleaning and length filter: {len(posts_with_comments):,} comments'
        )
    if not args.non_interactive and len(posts_with_comments) < 100:
        print(
            '⚠️  Warning: Less than 100 comments available. Results may not be meaningful.'
            )
        print(
            '   Consider lowering MIN_COMMENT_LENGTH or collecting more data.')
        response = input('Continue anyway? (y/n): ')
        if response.lower() != 'y':
            exit(0)
    if REMOVE_TEMPLATES:
        detector = TemplateDetector(threshold=TEMPLATE_THRESHOLD)
        is_template = detector.detect_templates(posts_with_comments[
            'cleaned_text'].tolist())
        posts_with_comments = posts_with_comments[~is_template].copy()
        print(
            f'✓ After template removal: {len(posts_with_comments):,} comments')
    if REMOVE_DUPLICATES:
        dup_detector = DuplicateDetector(threshold=DUPLICATE_THRESHOLD)
        keep_mask = dup_detector.find_duplicates(posts_with_comments[
            'cleaned_text'].tolist())
        posts_with_comments = posts_with_comments[keep_mask].copy()
        print(f'✓ After deduplication: {len(posts_with_comments):,} comments')
    if len(posts_with_comments) < 50:
        print('\n❌ Too few comments remaining after preprocessing.')
        print('   Try: --min-comment-length 30 or disable --remove-templates')
        exit(1)
    posts_with_comments = posts_with_comments.reset_index(drop=True)
    documents = posts_with_comments['cleaned_text'].tolist()
    print(f'\n✓ Prepared {len(documents):,} documents for topic modeling')
    print('\n' + '=' * 70)
    print('Extracting linguistic features for posts with moderator comments')
    print('=' * 70)
    linguistic_features_available = {'title_toxicity': 'title_toxicity' in
        posts_with_comments.columns, 'selftext_toxicity': 
        'selftext_toxicity' in posts_with_comments.columns,
        'title_sentiment': 'title_sentiment_polarity' in
        posts_with_comments.columns, 'selftext_sentiment': 
        'selftext_sentiment_polarity' in posts_with_comments.columns,
        'title_readability': 'title_readability_flesch_reading_ease' in
        posts_with_comments.columns, 'selftext_readability': 
        'selftext_readability_flesch_reading_ease' in posts_with_comments.
        columns}
    print(f'\n📊 Linguistic Features Availability:')
    for feat, available in linguistic_features_available.items():
        print(f"   {feat:<25} {'✓' if available else '✗'}")
    linguistic_cols = []
    if linguistic_features_available.get('title_toxicity', False):
        linguistic_cols.append('title_toxicity')
    if linguistic_features_available.get('selftext_toxicity', False):
        linguistic_cols.append('selftext_toxicity')
    if linguistic_features_available.get('title_sentiment', False):
        linguistic_cols.append('title_sentiment_polarity')
    if linguistic_features_available.get('selftext_sentiment', False):
        linguistic_cols.append('selftext_sentiment_polarity')
    if linguistic_features_available.get('title_readability', False):
        linguistic_cols.append('title_readability_flesch_reading_ease')
    if linguistic_features_available.get('selftext_readability', False):
        linguistic_cols.append('selftext_readability_flesch_reading_ease')
    boolean_linguistic_features = ['title_has_political',
        'title_has_profanity', 'title_has_controversial',
        'selftext_has_political', 'selftext_has_profanity',
        'selftext_has_controversial', 'title_has_emotion_intense',
        'selftext_has_emotion_intense', 'title_has_questioning_authority',
        'selftext_has_questioning_authority']
    for bool_feat in boolean_linguistic_features:
        if bool_feat in posts_with_comments.columns:
            linguistic_cols.append(bool_feat)
    print(f'\n✓ Found {len(linguistic_cols)} linguistic features to analyze')
    print('\n' + '=' * 70)
    print('BUILDING BERTOPIC MODEL')
    print('=' * 70)
    print('\n⚙️  Configuring BERTopic...')
    print(f'   Min topic size: {MIN_TOPIC_SIZE}')
    print(f'   Embedding model: all-MiniLM-L6-v2')
    print(f'   Calculate probabilities: False (faster)')
    vectorizer_model = CountVectorizer(ngram_range=(1, 2), stop_words=
        'english', min_df=3, max_df=0.7)
    topic_model = BERTopic(embedding_model='all-MiniLM-L6-v2',
        min_topic_size=MIN_TOPIC_SIZE, vectorizer_model=vectorizer_model,
        calculate_probabilities=False, verbose=True, nr_topics='auto')
    print('\n🧠 Fitting topic model...')
    print(
        f'   Estimated time: {len(documents) // 100} - {len(documents) // 50} minutes'
        )
    print('   Progress:')
    try:
        topics, _ = topic_model.fit_transform(documents)
        print('\n✅ Model training complete!')
        try:
            print('\n🔧 Reducing topics for higher-level structure...')
            result = topic_model.reduce_topics(documents)
            if isinstance(result, tuple):
                topic_model, topics = result
            else:
                topic_model = result
            n_reduced = len(topic_model.get_topics()) - 1
            print(f'   ✓ Reduced to {n_reduced} topics (excluding outliers)')
        except Exception as e:
            print(f'   ⚠️ Could not reduce topics: {e}')
    except Exception as e:
        print(f'\n❌ Error during model training: {e}')
        print('   Try: --min-topic-size 20 or --min-comment-length 60')
        exit(1)
    print('\n' + '=' * 70)
    print('QUALITY ASSESSMENT')
    print('=' * 70)
    quality_metrics = TopicQualityMetrics()
    coherence_score, coherence_per_topic = quality_metrics.calculate_coherence(
        topic_model, documents)
    diversity_score = quality_metrics.calculate_diversity(topic_model)
    outlier_ratio = quality_metrics.calculate_outlier_ratio(topics)
    print(f'\n📊 Overall Quality Metrics:')
    print(
        f'   Coherence: {coherence_score:.4f} (higher is better, >0.5 is good)'
        )
    print(
        f'   Diversity: {diversity_score:.4f} (higher is better, >0.8 is good)'
        )
    print(
        f'   Outlier Ratio: {outlier_ratio:.2%} (lower is better, <20% is good)'
        )
    quality_assessment = ('Good' if coherence_score > 0.5 and 
        diversity_score > 0.8 and outlier_ratio < 0.2 else 'Fair' if 
        coherence_score > 0.3 and diversity_score > 0.6 and outlier_ratio <
        0.3 else 'Poor')
    print(f'\n🎯 Overall Assessment: {quality_assessment}')
    if quality_assessment == 'Poor':
        print('   ⚠️  Consider:')
        print('      - Increasing --min-topic-size')
        print('      - Enabling --remove-templates')
        print('      - Collecting more data')
    print('\n' + '=' * 70)
    print('TOPIC MODEL RESULTS')
    print('=' * 70)
    topic_info = topic_model.get_topic_info()
    n_topics = len(topic_info) - 1
    print(f'\n✓ Discovered {n_topics} topics')
    print(
        f'✓ Outliers (topic -1): {(np.array(topics) == -1).sum():,} documents ({outlier_ratio:.1%})'
        )
    if len(coherence_per_topic) > 0:
        coherence_dict = {i: coh for i, coh in enumerate(coherence_per_topic)}
        topic_info['coherence'] = topic_info['Topic'].map(lambda x:
            coherence_dict.get(x, 0))
    print(f'\n📊 Top {min(10, n_topics)} Topics:')
    print('-' * 80)
    print(f"{'Topic':<8} {'Count':<8} {'Coherence':<12} {'Top Words'}")
    print('-' * 80)
    for idx, row in topic_info.head(11).iterrows():
        if row['Topic'] == -1:
            continue
        topic_words = topic_model.get_topic(row['Topic'])
        top_words = ', '.join([word for word, _ in topic_words[:5]])
        coherence_str = (f"{row['coherence']:.3f}" if 'coherence' in row else
            'N/A')
        print(
            f"{row['Topic']:<8} {row['Count']:<8} {coherence_str:<12} {top_words}"
            )
    print('\n' + '=' * 70)
    print('TEMPLATE VS HUMAN CLASSIFICATION')
    print('=' * 70)
    classified = df[df['mod_comment_text'].notna() & (df['mod_comment_text'
        ].astype(str).str.strip() != '')].copy()
    classified['is_template'] = classified['mod_comment_text'].apply(
        AutomatedTemplateDetector.is_template)
    n_template = classified['is_template'].sum()
    n_human = (~classified['is_template']).sum()
    pct_template = 100 * n_template / len(classified)
    print(f'📊 Template comments: {n_template:,} ({pct_template:.1f}%)')
    print(f'👤 Human comments: {n_human:,} ({100 - pct_template:.1f}%)')
    print(f'Total posts with mod comments analyzed: {len(classified):,}')
    template_stats = pd.DataFrame({'category': ['Template', 'Human',
        'Total'], 'count': [n_template, n_human, len(df)], 'percentage': [
        pct_template, 100 - pct_template, 100.0]})
    template_stats.to_csv(f'{output_dir}/template_vs_human_stats.csv',
        index=False)
    print(f'✓ Saved: template_vs_human_stats.csv')
    print('\n' + '=' * 70)
    print('TOPIC LABELING')
    print('=' * 70)

    def create_enhanced_label(topic_num, topic_model, documents, topics):
        if topic_num == -1:
            return 'Outliers/Uncategorized'
        words = topic_model.get_topic(topic_num)
        top_words = [word for word, _ in words[:10]]
        themes = {'spam': ['spam', 'promotional', 'advertising',
            'self promotion'], 'rules': ['rule', 'rules', 'violation',
            'violates', 'breaks'], 'quality': ['quality', 'effort',
            'low quality', 'shitpost'], 'formatting': ['format', 'flair',
            'title', 'tag'], 'civility': ['civil', 'rude', 'attack',
            'harassment', 'toxic'], 'repost': ['repost', 'duplicate',
            'already posted'], 'off-topic': ['topic', 'relevant', 'belongs',
            'off topic'], 'account': ['account', 'karma', 'age', 'new user'
            ], 'politics': ['political', 'politics', 'election']}
        matched_themes = []
        for theme, keywords in themes.items():
            if any(kw in ' '.join(top_words) for kw in keywords):
                matched_themes.append(theme)
        if matched_themes:
            return ' + '.join(matched_themes[:2]).title()
        common_mod_words = {'removed', 'post', 'comment', 'please',
            'violation', 'read'}
        meaningful_words = [w for w in top_words if w not in
            common_mod_words and len(w) > 3][:3]
        if meaningful_words:
            return ' + '.join(meaningful_words).title()
        return ' + '.join(top_words[:3]).title()
    print('\n🏷️  Generating enhanced topic labels...')
    topic_labels = {}
    for topic_num in topic_info['Topic'].unique():
        topic_labels[topic_num] = create_enhanced_label(topic_num,
            topic_model, documents, topics)
    all_assigned_topics = sorted(set(topics))
    for t in all_assigned_topics:
        if t not in topic_labels:
            if t == -1:
                topic_labels[t] = 'Outliers/Uncategorized'
            else:
                topic_labels[t] = f'Topic {t}'
    print('\n✓ Generated topic labels:')
    for topic in sorted([t for t in topic_labels.keys() if t != -1])[:10]:
        print(f'   Topic {topic}: {topic_labels[topic]}')
    topic_labels_df = pd.DataFrame([{'Topic': t, 'label': lbl} for t, lbl in
        topic_labels.items()])
    topic_labels_df.to_csv(os.path.join(output_dir,
        'bertopic_topic_labels.csv'), index=False)
    print(
        f"✓ Saved topic labels to {os.path.join(output_dir, 'bertopic_topic_labels.csv')}"
        )
    topic_info_with_labels = topic_info.merge(topic_labels_df, on='Topic',
        how='left')
    topic_info_with_labels.to_csv(os.path.join(output_dir,
        'bertopic_topic_summary_with_labels.csv'), index=False)
    print(
        f'✓ Saved labeled topic summary to bertopic_topic_summary_with_labels.csv'
        )
    print('\n' + '=' * 70)
    print('HIERARCHICAL TOPIC ANALYSIS')
    print('=' * 70)
    topic_sizes = pd.Series(topics).value_counts()
    large_topics = topic_sizes[topic_sizes >= 1000].index.tolist()
    print(f"\n   Topic 0 size: {topic_sizes.get(0, 'not found')}")
    large_topics = [t for t in large_topics if t != -1]
    if len(large_topics) > 0:
        print(f'\n🔍 Found {len(large_topics)} large topics to split:')
        for topic_id in large_topics:
            count = topic_sizes[topic_id]
            print(f'   Topic {topic_id}: {count:,} documents')
        hierarchical_dir = f'{output_dir}/hierarchical_topics'
        os.makedirs(hierarchical_dir, exist_ok=True)
        subtopic_results = []
        for topic_id in large_topics:
            print(f'\n📊 Splitting Topic {topic_id}...')
            topic_mask = np.array(topics) == topic_id
            topic_docs = [doc for doc, mask in zip(documents, topic_mask) if
                mask]
            topic_indices = np.where(topic_mask)[0]
            print(f'   Documents in topic: {len(topic_docs):,}')
            sub_min_size = max(30, len(topic_docs) // 100)
            print(f'   Sub-topic min size: {sub_min_size}')
            try:
                sub_model = BERTopic(language='english', min_topic_size=
                    sub_min_size, nr_topics='auto', verbose=False)
                sub_topics, sub_probs = sub_model.fit_transform(topic_docs)
                sub_topic_info = sub_model.get_topic_info()
                n_subtopics = len([t for t in sub_topic_info['Topic'] if t !=
                    -1])
                print(f'   ✓ Discovered {n_subtopics} sub-topics')
                sub_model.save(f'{hierarchical_dir}/topic_{topic_id}_submodel')
                sub_df = pd.DataFrame({'original_index': topic_indices,
                    'parent_topic': topic_id, 'sub_topic': sub_topics,
                    'sub_topic_prob': sub_probs})
                sub_df.to_csv(
                    f'{hierarchical_dir}/topic_{topic_id}_subtopics.csv',
                    index=False)
                for sub_topic in sorted([t for t in set(sub_topics) if t != -1]
                    ):
                    words = sub_model.get_topic(sub_topic)
                    top_words = ', '.join([word for word, _ in words[:5]])
                    count = (np.array(sub_topics) == sub_topic).sum()
                    subtopic_results.append({'parent_topic': topic_id,
                        'sub_topic': sub_topic, 'count': count, 'top_words':
                        top_words})
                    print(
                        f'      Sub-topic {sub_topic} ({count:,} docs): {top_words}'
                        )
            except Exception as e:
                print(f'   ⚠️  Error splitting topic: {e}')
                continue
        if subtopic_results:
            subtopic_df = pd.DataFrame(subtopic_results)
            subtopic_df.to_csv(f'{hierarchical_dir}/all_subtopics.csv',
                index=False)
            print(f'\n✓ Saved hierarchical analysis to {hierarchical_dir}/')
    else:
        print('\nℹ️  No large topics found (threshold: 1000 documents)')
    print(f'\n🔍 Debug: Topic size analysis')
    print(f'   Total topics in series: {len(topic_sizes)}')
    print(f"   Topic 0 size: {topic_sizes.get(0, 'NOT FOUND')}")
    print(f'   Topics over 1000: {topic_sizes[topic_sizes > 1000]}')
    print(f'   Large topics list: {large_topics}')
    if linguistic_cols:
        print('\n' + '=' * 70)
        print('ANALYZING LINGUISTIC PATTERNS BY TOPIC')
        print('=' * 70)
        posts_with_comments['topic'] = topics
        posts_with_comments['topic_label'] = posts_with_comments['topic'].map(
            lambda t: topic_labels.get(t, 'Outlier'))
        print('\n📊 Computing linguistic statistics by topic...')
        linguistic_by_topic = []
        for topic_num in range(-1, n_topics):
            topic_posts = posts_with_comments[posts_with_comments['topic'] ==
                topic_num]
            if len(topic_posts) == 0:
                continue
            topic_stats = {'topic': topic_num, 'topic_label': topic_labels.
                get(topic_num, 'Outlier'), 'n_posts': len(topic_posts)}
            for col in linguistic_cols:
                if col in posts_with_comments.columns:
                    if posts_with_comments[col
                        ].dtype == bool or col.startswith(('title_has_',
                        'selftext_has_')):
                        topic_stats[f'{col}_pct'] = topic_posts[col].mean(
                            ) * 100
                    else:
                        topic_stats[f'{col}_mean'] = topic_posts[col].mean()
                        topic_stats[f'{col}_median'] = topic_posts[col].median(
                            )
            linguistic_by_topic.append(topic_stats)
        linguistic_by_topic_df = pd.DataFrame(linguistic_by_topic)
        non_outlier_topics = linguistic_by_topic_df[linguistic_by_topic_df[
            'topic'] >= 0]
        print('\n📋 Linguistic patterns by topic (top 10):')
        display_cols = ['topic', 'topic_label', 'n_posts']
        if 'title_toxicity_mean' in linguistic_by_topic_df.columns:
            display_cols.append('title_toxicity_mean')
        if 'title_sentiment_polarity_mean' in linguistic_by_topic_df.columns:
            display_cols.append('title_sentiment_polarity_mean')
        if 'title_has_profanity_pct' in linguistic_by_topic_df.columns:
            display_cols.append('title_has_profanity_pct')
        available_display_cols = [c for c in display_cols if c in
            linguistic_by_topic_df.columns]
        print(linguistic_by_topic_df[available_display_cols].head(10).
            to_string(index=False))
    if 'is_template' in df.columns and not REMOVE_TEMPLATES:
        print('\n' + '=' * 70)
        print('TEMPLATE VS HUMAN TOPIC COMPARISON')
        print('=' * 70)
        df_with_topics = posts_with_comments.copy()
        df_with_topics['topic'] = topics
        df_with_topics['is_template'] = df.loc[posts_with_comments.index,
            'is_template'].values
        template_topics = df_with_topics[df_with_topics['is_template']]['topic'
            ].value_counts()
        human_topics = df_with_topics[~df_with_topics['is_template']]['topic'
            ].value_counts()
        comparison = pd.DataFrame({'template_count': template_topics,
            'human_count': human_topics}).fillna(0)
        comparison['template_pct'] = 100 * comparison['template_count'
            ] / comparison['template_count'].sum()
        comparison['human_pct'] = 100 * comparison['human_count'] / comparison[
            'human_count'].sum()
        comparison['difference'] = comparison['human_pct'] - comparison[
            'template_pct']
        comparison = comparison.sort_values('difference', ascending=False)
        comparison.to_csv(f'{output_dir}/template_vs_human_topics.csv')
        print('\n📊 Topics dominated by HUMAN comments (top 5):')
        for topic, row in comparison.head(5).iterrows():
            if topic != -1:
                label = topic_labels.get(topic, f'Topic {topic}')
                print(
                    f"   {label}: {row['human_pct']:.1f}% human vs {row['template_pct']:.1f}% template"
                    )
        print('\n🤖 Topics dominated by TEMPLATE comments (top 5):')
        for topic, row in comparison.tail(5).iterrows():
            if topic != -1:
                label = topic_labels.get(topic, f'Topic {topic}')
                print(
                    f"   {label}: {row['template_pct']:.1f}% template vs {row['human_pct']:.1f}% human"
                    )
        fig, ax = plt.subplots(figsize=(12, 8))
        plot_data = comparison.head(15).sort_values('difference')
        x = range(len(plot_data))
        ax.barh(x, plot_data['human_pct'], label='Human', alpha=0.7, color=
            'steelblue')
        ax.barh(x, -plot_data['template_pct'], label='Template', alpha=0.7,
            color='coral')
        ax.set_yticks(x)
        ax.set_yticklabels([topic_labels.get(t, f'Topic {t}') for t in
            plot_data.index])
        ax.set_xlabel('Percentage of Comments (%)')
        ax.set_title('Template vs Human Comments by Topic')
        ax.legend()
        ax.axvline(0, color='black', linewidth=0.8)
        ax.grid(axis='x', alpha=0.3)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/template_vs_human_by_topic.png', dpi=300,
            bbox_inches='tight')
        plt.close()
        print(f'✓ Saved: template_vs_human_by_topic.png')
    print('\n' + '=' * 70)
    print('FINDING REPRESENTATIVE DOCUMENTS')
    print('=' * 70)

    def get_representative_docs(topic_model, documents, topics, topic_num, n=5
        ):
        if topic_num == -1:
            return []
        topic_docs_idx = [i for i, t in enumerate(topics) if t == topic_num]
        if len(topic_docs_idx) == 0:
            return []
        topic_docs = [documents[i] for i in topic_docs_idx]
        topic_words = dict(topic_model.get_topic(topic_num))
        scores = []
        for doc in topic_docs:
            doc_lower = doc.lower()
            score = sum(topic_words.get(word, 0) for word in doc_lower.
                split() if word in topic_words)
            scores.append(score)
        top_indices = np.argsort(scores)[-n:][::-1]
        return [topic_docs[i] for i in top_indices]
    print('\n📄 Extracting representative documents per topic...')
    representative_docs = {}
    for topic_num in range(n_topics):
        if topic_num >= 0:
            rep_docs = get_representative_docs(topic_model, documents,
                topics, topic_num, n=5)
            representative_docs[topic_num] = rep_docs
    print(
        f'✓ Extracted {sum(len(docs) for docs in representative_docs.values())} representative documents'
        )
    print('\n' + '=' * 70)
    print('COMPARISON WITH REGEX-BASED CATEGORIZATION')
    print('=' * 70)
    REMOVAL_PATTERNS = {'spam':
        '\\b(spam\\w*|self.?promot\\w*|advertis\\w*|market\\w*|solicit\\w*)\\b'
        , 'rule_violation':
        '\\b(rule\\s*\\d+|violat\\w*|broken\\srule\\w*|read\\sthe\\srules?)\\b'
        , 'low_quality':
        '\\b(low.?quality\\w*|low.?effort\\w*|shitpost\\w*|not\\senough\\scontext|too\\sshort)\\b'
        , 'repost':
        '\\b(repost\\w*|duplicate\\w*|already.?posted|search.?bar)\\b',
        'off_topic':
        '\\b(off.?topic\\w*|not.?relevant\\w*|doesn.?t.?belong|wrong.?sub)\\b',
        'formatting':
        '\\b(format\\w*|flair\\w*|tag\\w*|link.?description|direct.?link|screenshot\\w*|title\\w*|headline\\w*)\\b'
        , 'civility':
        '\\b(uncivil\\w*|rude\\w*|personal.?attack\\w*|harass\\w*|toxic\\w*|insult\\w*|hate.?speech|be.?nice)\\b'
        , 'misinformation':
        '\\b(misinformation\\w*|misleading\\w*|false\\w*|unverified\\w*|source\\w*|citation\\w*)\\b'
        , 'crowd_control':
        '\\b(account\\w*|new.?user\\w*|karma\\w*|age.?requirement\\w*|verified\\w*|participation\\w*)\\b'
        , 'megathread':
        '\\b(megathread\\w*|sticky\\w*|daily.?thread\\w*|discussion.?thread\\w*)\\b'
        , 'politics': '\\b(politic\\w*|election\\w*|current.?event\\w*)\\b'}

    def categorize_regex(text):
        if pd.isna(text):
            return []
        categories = []
        text_lower = str(text).lower()
        for category, pattern in REMOVAL_PATTERNS.items():
            if re.search(pattern, text_lower):
                categories.append(category)
        return categories if categories else ['other']
    print('\n🔍 Applying regex categorization to compare with topics...')
    posts_with_comments['regex_categories'] = posts_with_comments[
        'mod_comment_text'].apply(categorize_regex)
    topic_to_regex = defaultdict(Counter)
    for idx, row in posts_with_comments.iterrows():
        topic = topics[posts_with_comments.index.get_loc(idx)]
        if topic >= 0:
            for cat in row['regex_categories']:
                topic_to_regex[topic][cat] += 1
    topic_regex_mapping = {}
    for topic_num in range(n_topics):
        if topic_num >= 0 and topic_num in topic_to_regex:
            dominant_regex = topic_to_regex[topic_num].most_common(1)[0][0]
            topic_regex_mapping[topic_num] = dominant_regex
    print('\n📊 Topic-to-Regex Category Mapping:')
    print('-' * 60)
    print(f"{'BERTopic ID':<15} {'BERTopic Label':<30} {'Regex Category'}")
    print('-' * 60)
    for topic in sorted(topic_regex_mapping.keys())[:10]:
        print(
            f"{topic:<15} {topic_labels[topic]:<30} {topic_regex_mapping.get(topic, 'N/A')}"
            )
    if STABILITY_RUNS > 0:
        print('\n' + '=' * 70)
        print(f'STABILITY ANALYSIS ({STABILITY_RUNS} runs)')
        print('=' * 70)
        print(
            f'\n🔄 Running BERTopic {STABILITY_RUNS} times to assess stability...'
            )
        stability_results = []
        for run in range(STABILITY_RUNS):
            print(f'\n   Run {run + 1}/{STABILITY_RUNS}...')
            temp_model = BERTopic(embedding_model='all-MiniLM-L6-v2',
                min_topic_size=MIN_TOPIC_SIZE, vectorizer_model=
                CountVectorizer(ngram_range=(1, 2), stop_words='english',
                min_df=3, max_df=0.7), calculate_probabilities=False,
                verbose=False)
            temp_topics, _ = temp_model.fit_transform(documents)
            temp_n_topics = len(set(temp_topics)) - 1
            stability_results.append({'run': run + 1, 'n_topics':
                temp_n_topics, 'outlier_ratio': (np.array(temp_topics) == -
                1).sum() / len(temp_topics)})
        stability_df = pd.DataFrame(stability_results)
        print(f'\n📊 Stability Results:')
        print(
            f"   Number of topics: {stability_df['n_topics'].mean():.1f} ± {stability_df['n_topics'].std():.1f}"
            )
        print(
            f"   Outlier ratio: {stability_df['outlier_ratio'].mean():.2%} ± {stability_df['outlier_ratio'].std():.2%}"
            )
        topics_std = stability_df['n_topics'].std()
        if topics_std < 2:
            stability_label = 'Good (low variability in number of topics)'
        elif topics_std < 4:
            stability_label = 'Fair (moderate variability)'
        else:
            stability_label = 'Poor (high variability)'
        print(f'   Stability: {stability_label}')
    print('\n' + '=' * 70)
    print('TOPICS BY SUBREDDIT')
    print('=' * 70)
    posts_with_comments['topic'] = topics
    posts_with_comments['topic_label'] = posts_with_comments['topic'].map(
        topic_labels).fillna('Outliers/Uncategorized')
    subreddit_topics = posts_with_comments.groupby(['subreddit', 'topic_label']
        ).size().reset_index(name='count')
    top_topics_per_sub = subreddit_topics.sort_values('count', ascending=False
        ).groupby('subreddit').first().reset_index()
    print('\n📊 Dominant Topic per Subreddit:')
    print('-' * 70)
    print(f"{'Subreddit':<25} {'Top Topic':<35} {'Count'}")
    print('-' * 70)
    for _, row in top_topics_per_sub.head(15).iterrows():
        print(f"{row['subreddit']:<25} {row['topic_label']:<35} {row['count']}"
            )
    print('\n' + '=' * 70)
    print('GENERATING VISUALIZATIONS')
    print('=' * 70)
    print('\n📊 Creating topic distribution chart...')
    fig, ax = plt.subplots(figsize=(14, 8))
    topic_counts = topic_info[topic_info['Topic'] != -1].head(N_TOPICS_TO_SHOW)
    topic_counts['Label'] = topic_counts['Topic'].map(topic_labels)
    if 'coherence' in topic_counts.columns:
        colors = plt.cm.RdYlGn(topic_counts['coherence'] / topic_counts[
            'coherence'].max())
    else:
        colors = 'steelblue'
    bars = ax.barh(range(len(topic_counts)), topic_counts['Count'].values,
        color=colors, edgecolor='black')
    ax.set_yticks(range(len(topic_counts)))
    ax.set_yticklabels(topic_counts['Label'].values)
    ax.set_xlabel('Number of Comments', fontsize=12, fontweight='bold')
    ax.set_title(f'Top {N_TOPICS_TO_SHOW} Topics in Moderator Comments',
        fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width + 5, bar.get_y() + bar.get_height() / 2,
            f'{int(width)}', ha='left', va='center', fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/topic_distribution.png', dpi=300,
        bbox_inches='tight')
    print('✓ Saved: topic_distribution.png')
    plt.close()
    if len(coherence_per_topic) > 0:
        print('\n📊 Creating coherence visualization...')
        fig, ax = plt.subplots(figsize=(12, 6))
        coherence_data = [(i, coh) for i, coh in enumerate(coherence_per_topic)
            ]
        coherence_data = sorted(coherence_data, key=lambda x: x[1], reverse
            =True)[:15]
        topics_sorted = [topic_labels.get(t, f'Topic {t}') for t, _ in
            coherence_data]
        coherences_sorted = [c for _, c in coherence_data]
        bars = ax.barh(range(len(topics_sorted)), coherences_sorted, color=
            'forestgreen', edgecolor='black')
        ax.set_yticks(range(len(topics_sorted)))
        ax.set_yticklabels(topics_sorted)
        ax.set_xlabel('Coherence Score', fontsize=12, fontweight='bold')
        ax.set_title('Topic Coherence Scores', fontsize=14, fontweight='bold')
        ax.axvline(0.5, color='red', linestyle='--', alpha=0.5, label=
            'Good threshold')
        ax.legend()
        ax.grid(axis='x', alpha=0.3)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/topic_coherence.png', dpi=300,
            bbox_inches='tight')
        print('✓ Saved: topic_coherence.png')
        plt.close()
    print('\n🌐 Creating interactive visualizations...')
    try:
        fig = topic_model.visualize_topics()
        fig.write_html(f'{output_dir}/topic_visualization_intertopic.html')
        print('✓ Saved: topic_visualization_intertopic.html')
    except Exception as e:
        print(f'   ⚠️  Could not create intertopic viz: {e}')
    try:
        fig = topic_model.visualize_hierarchy()
        fig.write_html(f'{output_dir}/topic_hierarchy.html')
        print('✓ Saved: topic_hierarchy.html')
    except Exception as e:
        print(f'   ⚠️  Could not create hierarchy viz: {e}')
    try:
        fig = topic_model.visualize_barchart(top_n_topics=min(15, n_topics))
        fig.write_html(f'{output_dir}/topic_barchart_interactive.html')
        print('✓ Saved: topic_barchart_interactive.html')
    except Exception as e:
        print(f'   ⚠️  Could not create barchart viz: {e}')
    print('\n🔥 Creating subreddit-topic heatmap...')
    pivot = subreddit_topics.pivot(index='subreddit', columns='topic_label',
        values='count').fillna(0)
    pivot_normalized = pivot.div(pivot.sum(axis=1), axis=0) * 100
    top_topics_list = topic_info[topic_info['Topic'] != -1].head(10)['Topic'
        ].map(topic_labels).values
    top_subs = posts_with_comments.groupby('subreddit').size().nlargest(20
        ).index
    pivot_filtered = pivot_normalized.loc[top_subs, [col for col in
        top_topics_list if col in pivot_normalized.columns]]
    if len(pivot_filtered) > 0 and len(pivot_filtered.columns) > 0:
        fig, ax = plt.subplots(figsize=(14, 10))
        sns.heatmap(pivot_filtered, annot=True, fmt='.1f', cmap='YlOrRd',
            linewidths=0.5, cbar_kws={'label': '% of Comments'}, ax=ax)
        ax.set_title('Topic Distribution by Subreddit (%) - Top 20 Subs',
            fontsize=14, fontweight='bold')
        ax.set_xlabel('Topic', fontsize=12)
        ax.set_ylabel('Subreddit', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/topics_by_subreddit_heatmap.png', dpi=
            300, bbox_inches='tight')
        print('✓ Saved: topics_by_subreddit_heatmap.png')
        plt.close()
    print('\n📊 Creating regex-topic comparison chart...')
    comparison_data = []
    for topic in range(n_topics):
        if topic >= 0 and topic in topic_to_regex:
            for regex_cat, count in topic_to_regex[topic].most_common(3):
                comparison_data.append({'bertopic': topic_labels.get(topic,
                    f'Topic {topic}'), 'regex_category': regex_cat,
                    'overlap_count': count})
    if comparison_data:
        comp_df = pd.DataFrame(comparison_data)
        pivot_comp = comp_df.pivot_table(index='bertopic', columns=
            'regex_category', values='overlap_count', aggfunc='sum').fillna(0)
        fig, ax = plt.subplots(figsize=(12, 8))
        sns.heatmap(pivot_comp, annot=True, fmt='.0f', cmap='Blues',
            linewidths=0.5, ax=ax)
        ax.set_title('BERTopic vs Regex Category Overlap', fontsize=14,
            fontweight='bold')
        ax.set_xlabel('Regex Category', fontsize=12)
        ax.set_ylabel('BERTopic', fontsize=12)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/bertopic_vs_regex.png', dpi=300,
            bbox_inches='tight')
        print('✓ Saved: bertopic_vs_regex.png')
        plt.close()
    if linguistic_cols and len(linguistic_by_topic_df) > 0:
        print('\n' + '=' * 70)
        print('CREATING LINGUISTIC VISUALIZATIONS')
        print('=' * 70)
        non_outlier_topics = linguistic_by_topic_df[linguistic_by_topic_df[
            'topic'] >= 0]
        if len(non_outlier_topics) > 0:
            if 'title_toxicity_mean' in non_outlier_topics.columns:
                plt.figure(figsize=(12, 6))
                top_topics = non_outlier_topics.nlargest(10, 'n_posts')
                sns.barplot(data=top_topics, x='topic', y=
                    'title_toxicity_mean', palette='Reds')
                plt.xlabel('Topic')
                plt.ylabel('Mean Title Toxicity')
                plt.title('Title Toxicity by Topic (Top 10 Topics by Size)')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(f'{output_dir}/linguistic_toxicity_by_topic.png',
                    dpi=300, bbox_inches='tight')
                print('✓ Saved: linguistic_toxicity_by_topic.png')
                plt.close()
            if 'title_sentiment_polarity_mean' in non_outlier_topics.columns:
                plt.figure(figsize=(12, 6))
                top_topics = non_outlier_topics.nlargest(10, 'n_posts')
                colors = [('red' if x < 0 else 'green') for x in top_topics
                    ['title_sentiment_polarity_mean']]
                plt.bar(range(len(top_topics)), top_topics[
                    'title_sentiment_polarity_mean'], color=colors, alpha=0.7)
                plt.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
                plt.xlabel('Topic')
                plt.ylabel('Mean Sentiment Polarity')
                plt.title('Sentiment Polarity by Topic (Top 10 Topics by Size)'
                    )
                plt.xticks(range(len(top_topics)), top_topics['topic'].
                    values, rotation=45)
                plt.tight_layout()
                plt.savefig(f'{output_dir}/linguistic_sentiment_by_topic.png',
                    dpi=300, bbox_inches='tight')
                print('✓ Saved: linguistic_sentiment_by_topic.png')
                plt.close()
            boolean_cols_in_df = [c for c in linguistic_by_topic_df.columns if
                c.endswith('_pct')]
            if len(boolean_cols_in_df) >= 3:
                plt.figure(figsize=(14, 8))
                top_topics = non_outlier_topics.nlargest(15, 'n_posts')
                heatmap_data = top_topics[['topic'] + boolean_cols_in_df
                    ].set_index('topic')
                heatmap_data.columns = [c.replace('title_has_', 't_').
                    replace('selftext_has_', 's_').replace('_pct', '') for
                    c in heatmap_data.columns]
                sns.heatmap(heatmap_data.T, cmap='YlOrRd', annot=True, fmt=
                    '.1f', cbar_kws={'label': 'Percentage (%)'}, linewidths=0.5
                    )
                plt.xlabel('Topic')
                plt.ylabel('Linguistic Feature')
                plt.title('Linguistic Feature Prevalence by Topic (%)')
                plt.tight_layout()
                plt.savefig(f'{output_dir}/linguistic_features_heatmap.png',
                    dpi=300, bbox_inches='tight')
                print('✓ Saved: linguistic_features_heatmap.png')
                plt.close()
            if 'title_toxicity_mean' in non_outlier_topics.columns and len(
                non_outlier_topics) >= 5:
                fig, axes = plt.subplots(1, 2, figsize=(14, 6))
                most_toxic = non_outlier_topics.nlargest(5,
                    'title_toxicity_mean')
                axes[0].barh(range(len(most_toxic)), most_toxic[
                    'title_toxicity_mean'], color='red', alpha=0.7)
                axes[0].set_yticks(range(len(most_toxic)))
                axes[0].set_yticklabels([f'T{t}: {l[:30]}' for t, l in zip(
                    most_toxic['topic'], most_toxic['topic_label'])])
                axes[0].set_xlabel('Mean Toxicity')
                axes[0].set_title('Most Toxic Topics')
                axes[0].invert_yaxis()
                least_toxic = non_outlier_topics.nsmallest(5,
                    'title_toxicity_mean')
                axes[1].barh(range(len(least_toxic)), least_toxic[
                    'title_toxicity_mean'], color='green', alpha=0.7)
                axes[1].set_yticks(range(len(least_toxic)))
                axes[1].set_yticklabels([f'T{t}: {l[:30]}' for t, l in zip(
                    least_toxic['topic'], least_toxic['topic_label'])])
                axes[1].set_xlabel('Mean Toxicity')
                axes[1].set_title('Least Toxic Topics')
                axes[1].invert_yaxis()
                plt.tight_layout()
                plt.savefig(f'{output_dir}/linguistic_toxicity_comparison.png',
                    dpi=300, bbox_inches='tight')
                print('✓ Saved: linguistic_toxicity_comparison.png')
                plt.close()
    print('\n' + '=' * 70)
    print('SAVING RESULTS')
    print('=' * 70)
    print('\n💾 Saving BERTopic model...')
    model_path = f'{output_dir}/topic_model'
    os.makedirs(model_path, exist_ok=True)
    topic_model.save(model_path, serialization='pytorch', save_ctfidf=True)
    print('✓ Model saved (can be loaded later)')
    print('\n💾 Saving quality metrics...')
    quality_metrics_dict = {'analysis_date': datetime.now().isoformat(),
        'n_documents': len(documents), 'n_topics': n_topics,
        'coherence_score': float(coherence_score), 'diversity_score': float
        (diversity_score), 'outlier_ratio': float(outlier_ratio),
        'quality_assessment': quality_assessment, 'preprocessing': {
        'min_comment_length': MIN_COMMENT_LENGTH, 'templates_removed':
        REMOVE_TEMPLATES, 'duplicates_removed': REMOVE_DUPLICATES},
        'model_params': {'min_topic_size': MIN_TOPIC_SIZE,
        'embedding_model': 'all-MiniLM-L6-v2'}}
    if STABILITY_RUNS > 0:
        quality_metrics_dict['stability'] = {'runs': STABILITY_RUNS,
            'avg_topics': float(stability_df['n_topics'].mean()),
            'std_topics': float(stability_df['n_topics'].std())}
    with open(f'{output_dir}/quality_metrics.json', 'w') as f:
        json.dump(quality_metrics_dict, f, indent=2)
    print('✓ Saved: quality_metrics.json')
    print('\n💾 Saving enhanced topic summary...')
    topic_summary = topic_info.copy()
    topic_summary['Label'] = topic_summary['Topic'].map(topic_labels)
    topic_summary['Regex_Category'] = topic_summary['Topic'].map(lambda x:
        topic_regex_mapping.get(x, 'N/A'))
    if len(coherence_per_topic) > 0:
        topic_summary['Coherence'] = topic_summary['Topic'].map(lambda x: 
            coherence_per_topic[x] if 0 <= x < len(coherence_per_topic) else 0)
    topic_summary.to_csv(f'{output_dir}/topic_summary_enhanced.csv', index=
        False)
    print('✓ Saved: topic_summary_enhanced.csv')
    print('\n💾 Saving subreddit-topic breakdown...')
    subreddit_topics.to_csv(f'{output_dir}/topics_by_subreddit.csv', index=
        False)
    print('✓ Saved: topics_by_subreddit.csv')
    print('\n💾 Saving full data with topic assignments...')
    cols = ['subreddit', 'mod_comment_text', 'topic', 'topic_label']
    if 'post_fullname' in posts_with_comments.columns:
        cols = ['post_fullname'] + cols
    cols.append('regex_categories')
    export_df = posts_with_comments[cols].copy()
    export_df['mod_comment_text'] = export_df['mod_comment_text'].str[:500]
    export_df['regex_categories'] = export_df['regex_categories'].apply(lambda
        x: ', '.join(x))
    export_df.to_csv(f'{output_dir}/comments_with_topics_enhanced.csv',
        index=False)
    print('✓ Saved: comments_with_topics_enhanced.csv')
    print('\n💾 Saving representative documents per topic...')
    for topic_num in range(n_topics):
        if topic_num >= 0 and topic_num in representative_docs:
            rep_docs_data = []
            for doc in representative_docs[topic_num]:
                idx = documents.index(doc)
                original_idx = posts_with_comments.index[idx]
                subreddit = posts_with_comments.loc[original_idx, 'subreddit']
                rep_docs_data.append({'subreddit': subreddit,
                    'representative_comment': doc[:500]})
            if rep_docs_data:
                rep_df = pd.DataFrame(rep_docs_data)
                label = topic_labels[topic_num].replace(' ', '_').replace('+',
                    'and')
                filename = (
                    f'{output_dir}/representative_topic_{topic_num}_{label[:30]}.csv'
                    )
                rep_df.to_csv(filename, index=False)
    print(f'✓ Saved representative documents for {n_topics} topics')
    if STABILITY_RUNS > 0:
        stability_df.to_csv(f'{output_dir}/stability_analysis.csv', index=False
            )
        print('✓ Saved: stability_analysis.csv')
    if topic_regex_mapping:
        mapping_df = pd.DataFrame([{'topic_id': t, 'topic_label':
            topic_labels[t], 'dominant_regex': r} for t, r in
            topic_regex_mapping.items()])
        mapping_df.to_csv(f'{output_dir}/topic_regex_mapping.csv', index=False)
        print('✓ Saved: topic_regex_mapping.csv')
    if linguistic_cols and len(linguistic_by_topic_df) > 0:
        linguistic_by_topic_df.to_csv(
            f'{output_dir}/linguistic_features_by_topic.csv', index=False)
        print('✓ Saved: linguistic_features_by_topic.csv')
        if len(non_outlier_topics) > 0:
            print('\n📊 Most distinctive linguistic patterns:')
            if 'title_toxicity_mean' in non_outlier_topics.columns:
                most_toxic = non_outlier_topics.nlargest(3,
                    'title_toxicity_mean')
                print('\n   Most toxic topics:')
                for _, row in most_toxic.iterrows():
                    print(
                        f"      Topic {row['topic']}: {row['topic_label'][:50]} (toxicity: {row['title_toxicity_mean']:.3f})"
                        )
            if 'title_sentiment_polarity_mean' in non_outlier_topics.columns:
                most_negative = non_outlier_topics.nsmallest(3,
                    'title_sentiment_polarity_mean')
                print('\n   Most negative sentiment topics:')
                for _, row in most_negative.iterrows():
                    print(
                        f"      Topic {row['topic']}: {row['topic_label'][:50]} (sentiment: {row['title_sentiment_polarity_mean']:.3f})"
                        )
            if 'title_has_profanity_pct' in non_outlier_topics.columns:
                most_profane = non_outlier_topics.nlargest(3,
                    'title_has_profanity_pct')
                print('\n   Topics with most profanity:')
                for _, row in most_profane.iterrows():
                    print(
                        f"      Topic {row['topic']}: {row['topic_label'][:50]} ({row['title_has_profanity_pct']:.1f}% with profanity)"
                        )
    print('\n' + '=' * 70)
    print('VALIDATION REPORT')
    print('=' * 70)
    validation_report = []
    validation_report.append('\n📊 BERTOPIC QUALITY VALIDATION')
    validation_report.append('=' * 60)
    validation_report.append(f'\n1. Topic Quality Metrics:')
    validation_report.append(f'   • Coherence: {coherence_score:.4f}')
    if coherence_score > 0.5:
        coherence_interp = 'Excellent (>0.5)'
    elif coherence_score > 0.3:
        coherence_interp = 'Fair (>0.3)'
    else:
        coherence_interp = 'Poor (<0.3)'
    validation_report.append(f'     - Interpretation: {coherence_interp}')
    validation_report.append(f'   • Diversity: {diversity_score:.4f}')
    if diversity_score > 0.8:
        diversity_interp = 'Good (>0.8)'
    elif diversity_score > 0.6:
        diversity_interp = 'Fair (>0.6)'
    else:
        diversity_interp = 'Poor (<0.6)'
    validation_report.append(f'     - Interpretation: {diversity_interp}')
    validation_report.append(f'   • Outlier Ratio: {outlier_ratio:.2%}')
    if outlier_ratio < 0.2:
        outlier_interp = 'Good (<20%)'
    elif outlier_ratio < 0.3:
        outlier_interp = 'Fair (<30%)'
    else:
        outlier_interp = 'High (>30%)'
    validation_report.append(f'     - Interpretation: {outlier_interp}')
    validation_report.append(f'\n2. Model Configuration:')
    validation_report.append(f'   • Documents analyzed: {len(documents):,}')
    validation_report.append(f'   • Topics discovered: {n_topics}')
    validation_report.append(f'   • Min topic size: {MIN_TOPIC_SIZE}')
    validation_report.append(f'   • Templates removed: {REMOVE_TEMPLATES}')
    validation_report.append(f'   • Duplicates removed: {REMOVE_DUPLICATES}')
    if STABILITY_RUNS > 0:
        validation_report.append('\n4. Stability Analysis:')
        validation_report.append(f'   • Runs: {STABILITY_RUNS}')
        validation_report.append(
            f"   • Avg topics: {stability_df['n_topics'].mean():.1f} ± {stability_df['n_topics'].std():.1f}"
            )
        topics_std = stability_df['n_topics'].std()
        if topics_std < 2:
            stab_interp = 'Good (low variability in topic counts across runs)'
        elif topics_std < 4:
            stab_interp = 'Fair (moderate variability)'
        else:
            stab_interp = 'Poor (high variability)'
        validation_report.append(f'   • Stability: {stab_interp}')
    validation_report.append(f'\n4. Recommendations:')
    if coherence_score < 0.5:
        validation_report.append(
            f'   • Low coherence - consider --remove-templates or increase --min-topic-size'
            )
    if diversity_score < 0.6:
        validation_report.append(
            f'   • Low diversity - topics may be too similar, try reducing number of topics'
            )
    if outlier_ratio > 0.3:
        validation_report.append(
            f'   • High outlier ratio - increase --min-comment-length or collect more data'
            )
    if not REMOVE_TEMPLATES:
        validation_report.append(
            f'   • Consider enabling --remove-templates to reduce template pollution'
            )
    if not REMOVE_DUPLICATES:
        validation_report.append(
            f'   • Consider enabling --remove-duplicates for cleaner topics')
    if len(validation_report) == 4:
        validation_report.append(
            f'   ✓ Model quality is good, no major issues detected')
    validation_report.append(f'\n5. Next Steps:')
    validation_report.append(
        f'   • Review representative documents in representative_topic_*.csv')
    validation_report.append(f'   • Open interactive visualizations in browser'
        )
    validation_report.append(
        f'   • Compare with regex categorization (bertopic_vs_regex.png)')
    validation_report.append(f'   • Use topic assignments in further analysis')
    for line in validation_report:
        print(line)
    with open(f'{output_dir}/validation_report.txt', 'w') as f:
        f.write('\n'.join(validation_report))
    print(f'\n✓ Saved: validation_report.txt')
    print('\n' + '=' * 70)
    print('✅ ENHANCED BERTOPIC ANALYSIS COMPLETE!')
    print('=' * 70)
    print(f'\n📊 Summary:')
    print(f'   Total comments analyzed: {len(documents):,}')
    print(f'   Topics discovered: {n_topics}')
    print(
        f'   Outliers: {(np.array(topics) == -1).sum():,} ({outlier_ratio:.1%})'
        )
    print(f'   Quality: {quality_assessment}')
    print(
        f'   Coherence: {coherence_score:.3f} | Diversity: {diversity_score:.3f}'
        )
    print(f'   └── representative_topic_*.csv (per topic)')
    print(f'\n💡 Key Findings:')
    for topic in sorted([t for t in topic_labels.keys() if t >= 0])[:5]:
        count = topic_info[topic_info['Topic'] == topic]['Count'].values[0]
        regex_cat = topic_regex_mapping.get(topic, 'N/A')
        print(
            f"   • Topic {topic} ({topic_labels[topic]}): {count} comments - maps to '{regex_cat}'"
            )
    if quality_assessment == 'Poor':
        print(f'\n⚠️  Quality Warning:')
        print(f'   Your topics may not be meaningful. Try:')
        print(f'   --remove-templates --remove-duplicates --min-topic-size 20')
    print('\n' + '=' * 70)


if __name__ == '__main__':
    main()
