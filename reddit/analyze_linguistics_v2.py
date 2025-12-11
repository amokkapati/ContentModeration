import pandas as pd
import numpy as np
from textblob import TextBlob
import re
import glob
import os
import torch
from tqdm import tqdm
import psutil
import argparse
import warnings
from collections import Counter
import json
from datetime import datetime
warnings.filterwarnings('ignore')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
DATA_DIR = os.path.abspath(DATA_DIR)
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    HAS_VADER = True
except ImportError:
    HAS_VADER = False
    SentimentIntensityAnalyzer = None
    print('⚠️  vaderSentiment not found. Install: pip install vaderSentiment')


class Config:

    def __init__(self):
        self.min_memory_gb = 2
        self.toxicity_batch_size = 16
        self.max_text_length = 2000
        self.max_title_length = 256
        self.max_selftext_length = 2000
        self.data_dir = DATA_DIR
        self.output_dir = os.path.join(BASE_DIR, '..', 'results', 'linguistics'
            )
        self.enable_toxicity = True
        self.enable_advanced_features = True


config = Config()


def check_memory(min_gb=2):
    available = psutil.virtual_memory().available / 1024 ** 3
    if available < min_gb:
        print(
            f'⚠️  WARNING: Low memory: {available:.1f}GB available (recommended: {min_gb}GB)'
            )
        print('   Processing will proceed, but may be slow.')
        return True
    return True


def check_dependencies():
    available = {'textstat': False, 'transformers': False, 'torch': False,
        'vader': False}
    try:
        import textstat
        available['textstat'] = True
    except ImportError:
        print('⚠️  textstat not found. Install: pip install textstat')
    try:
        import transformers
        available['transformers'] = True
    except ImportError:
        print('⚠️  transformers not found. Install: pip install transformers')
    try:
        import torch
        available['torch'] = True
    except ImportError:
        print('⚠️  torch not found. Install: pip install torch')
    if HAS_VADER:
        available['vader'] = True
    return available


URL_RE = re.compile('https?://\\S+|www\\.\\S+')
MARKDOWN_LINK_RE = re.compile('\\[([^\\]]+)\\]\\(([^)]+)\\)')
MULTI_WS_RE = re.compile('\\s+')


def clean_text(text):
    if pd.isna(text) or not text:
        return ''
    text = str(text)
    text = MARKDOWN_LINK_RE.sub('\\1', text)
    text = URL_RE.sub(' ', text)
    text = re.sub('([!?.]){2,}', '\\1', text)
    text = MULTI_WS_RE.sub(' ', text)
    return text.strip()


def safe_divide(numerator, denominator, default=0.0):
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except:
        return default


class TextPreprocessor:

    @staticmethod
    def extract_features(text):
        if not text or pd.isna(text):
            return {'length': 0, 'word_count': 0, 'has_url': 0,
                'has_question': 0, 'has_exclamation': 0, 'all_caps_ratio': 
                0.0, 'punctuation_density': 0.0, 'avg_word_length': 0.0}
        text = str(text)
        words = text.split()
        letters = re.findall('[a-zA-Z]', text)
        caps = re.findall('[A-Z]', text)
        return {'length': len(text), 'word_count': len(words), 'has_url':
            int(bool(re.search('http[s]?://|www\\.', text))),
            'has_question': int('?' in text), 'has_exclamation': int('!' in
            text), 'all_caps_ratio': safe_divide(len(caps), len(letters)),
            'punctuation_density': safe_divide(len(re.findall('[!?,.]',
            text)), len(words)), 'avg_word_length': safe_divide(sum(len(w) for
            w in words), len(words))}


class SentimentAnalyzer:

    def __init__(self):
        if HAS_VADER:
            self.vader = SentimentIntensityAnalyzer()
        else:
            self.vader = None

    def analyze(self, text):
        if not text or pd.isna(text):
            return {'polarity': 0.0, 'subjectivity': 0.0, 'pos': 0.0, 'neg':
                0.0}
        text_str = str(text)
        try:
            blob = TextBlob(text_str)
            subjectivity = blob.sentiment.subjectivity
        except:
            subjectivity = 0.0
        if self.vader:
            try:
                scores = self.vader.polarity_scores(text_str)
                return {'polarity': scores['compound'], 'subjectivity':
                    subjectivity, 'pos': scores['pos'], 'neg': scores['neg']}
            except:
                return {'polarity': 0.0, 'subjectivity': subjectivity,
                    'pos': 0.0, 'neg': 0.0}
        else:
            return {'polarity': blob.sentiment.polarity, 'subjectivity':
                subjectivity, 'pos': 0.0, 'neg': 0.0}


class ReadabilityAnalyzer:

    def __init__(self, use_textstat=True):
        self.use_textstat = use_textstat
        if use_textstat:
            try:
                import textstat
                self.textstat = textstat
            except ImportError:
                self.use_textstat = False

    def analyze(self, text):
        if not text or pd.isna(text) or len(str(text).split()) < 3:
            return {'flesch_reading_ease': np.nan, 'flesch_kincaid_grade':
                np.nan}
        text = str(text)
        if self.use_textstat:
            try:
                return {'flesch_reading_ease': self.textstat.
                    flesch_reading_ease(text), 'flesch_kincaid_grade': self
                    .textstat.flesch_kincaid_grade(text)}
            except:
                pass
        return self._robust_fallback(text)

    def _robust_fallback(self, text):
        words = text.split()
        num_words = len(words)
        if num_words == 0:
            return {'flesch_reading_ease': np.nan, 'flesch_kincaid_grade':
                np.nan}
        num_sentences = max(1, sum(text.count(p) for p in ['.', '!', '?', ';'])
            )
        num_chars = sum(len(w) for w in words)
        est_syllables = max(num_words, num_chars * 0.3)
        ASL = num_words / num_sentences
        ASW = est_syllables / num_words
        fre = 206.835 - 1.015 * ASL - 84.6 * ASW
        fkgl = 0.39 * ASL + 11.8 * ASW - 15.59
        return {'flesch_reading_ease': max(0.0, min(100.0, fre)),
            'flesch_kincaid_grade': max(0.0, fkgl)}


def compute_richness(text: str, window_size: int=50) ->dict:
    if not isinstance(text, str) or not text.strip():
        return {'unique_word_count': 0, 'type_token_ratio': 0.0, 'mattr': 0.0}
    tokens = re.findall('\\b\\w+\\b', text.lower())
    n_tokens = len(tokens)
    if n_tokens == 0:
        return {'unique_word_count': 0, 'type_token_ratio': 0.0, 'mattr': 0.0}
    unique_set = set(tokens)
    ttr = len(unique_set) / n_tokens
    if n_tokens <= window_size:
        mattr = ttr
    else:
        current_counts = Counter(tokens[:window_size])
        unique_counts = [len(current_counts)]
        for i in range(n_tokens - window_size):
            out_token = tokens[i]
            current_counts[out_token] -= 1
            if current_counts[out_token] == 0:
                del current_counts[out_token]
            in_token = tokens[i + window_size]
            current_counts[in_token] += 1
            unique_counts.append(len(current_counts))
        mattr = sum(unique_counts) / len(unique_counts) / window_size
    return {'unique_word_count': len(unique_set), 'type_token_ratio': ttr,
        'mattr': mattr}


class ToxicityDetector:

    def __init__(self, enabled=True, batch_size=16):
        self.enabled = enabled
        self.batch_size = batch_size
        self.model = None
        self.device = -1
        if enabled:
            self._initialize_model()

    def _initialize_model(self):
        try:
            from transformers import pipeline
            model_kwargs = {}
            if torch.cuda.is_available():
                self.device = 0
                model_kwargs['torch_dtype'] = torch.float16
                print('   🚀 Toxicity model using NVIDIA GPU (CUDA) + FP16')
            elif hasattr(torch.backends, 'mps'
                ) and torch.backends.mps.is_available():
                self.device = 'mps'
                model_kwargs['torch_dtype'] = torch.float16
                print(
                    '   🚀 Toxicity model using Apple Silicon GPU (MPS) + FP16')
            else:
                self.device = -1
                print('   🐢 Toxicity model using CPU (Slow)')
            self.model = pipeline('text-classification', model=
                'unitary/toxic-bert', device=self.device, top_k=None,
                batch_size=self.batch_size, model_kwargs=model_kwargs)
            print('   ✓ Toxicity model loaded')
        except Exception as e:
            print(f'   ⚠️  Could not load toxicity model: {e}')
            self.enabled = False

    def analyze_batch(self, texts, truncation_len=None):
        if not self.enabled or self.model is None:
            return [0.0] * len(texts)
        limit = (truncation_len if truncation_len is not None else config.
            max_text_length)
        try:
            processed = [(t if t and len(t.strip()) > 0 else 'neutral') for
                t in texts]
            processed = [t[:limit] for t in processed]
            results = self.model(processed, padding=True, truncation=True)
            scores = []
            for res in results:
                toxic_score = next((item['score'] for item in res if item[
                    'label'] == 'toxic'), 0.0)
                scores.append(toxic_score)
            return scores
        except Exception as e:
            print(f'   ⚠️  Toxicity batch failed: {e}')
            return [0.0] * len(texts)


class KeywordDetector:

    def __init__(self):
        self.keyword_categories = {'political': ['politic', 'election',
            'vote', 'democrat', 'republican', 'liberal', 'conservative',
            'trump', 'biden', 'campaign', 'senate', 'congress', 'brexit',
            'minister', 'government'], 'gender': ['gender', 'female',
            'male', 'woman', 'man', 'girl', 'boy', 'trans', 'nonbinary',
            'feminist', 'patriarchy', 'misogyny', 'misandry'], 'profanity':
            ['fuck', 'shit', 'bitch', 'asshole', 'bastard', 'cunt', 'dick',
            'slut', 'motherfucker', 'fucked', 'fucking'], 'controversial':
            ['racist', 'racism', 'sexist', 'sexism', 'homophobic',
            'homophobia', 'xenophobia', 'nazi', 'fascist', 'bigot',
            'bigotry', 'hate crime'], 'finance': ['money', 'salary', 'wage',
            'income', 'tax', 'loan', 'debt', 'mortgage', 'investment',
            'stock', 'crypto', 'bitcoin', 'budget', 'rent', 'bills'],
            'health': ['doctor', 'hospital', 'medic', 'vaccine', 'illness',
            'disease', 'cancer', 'infection', 'surgery', 'treatment',
            'diabetes', 'covid', 'virus'], 'technology': ['computer',
            'software', 'hardware', 'ai', 'artificial intelligence',
            'machine learning', 'algorithm', 'app', 'phone', 'smartphone',
            'laptop', 'programming', 'code', 'coding'], 'relationship': [
            'girlfriend', 'boyfriend', 'wife', 'husband', 'partner',
            'relationship', 'dating', 'marriage', 'divorce', 'breakup',
            'cheat', 'affair'], 'mental_health': ['depression', 'anxiety',
            'panic attack', 'suicidal', 'therapy', 'therapist',
            'mental health', 'ptsd', 'trauma'], 'addiction': ['addict',
            'addiction', 'alcoholic', 'drugs', 'overdose', 'relapse',
            'rehab', 'withdrawal', 'substance abuse'], 'conflict': ['fight',
            'argue', 'argument', 'conflict', 'war', 'violence', 'violent',
            'attack', 'assault', 'threat', 'threaten'], 'emotion_intense':
            ['hate', 'love', 'angry', 'furious', 'rage', 'sad',
            'devastated', 'heartbroken', 'terrified', 'afraid'],
            'questioning_authority': ['corrupt', 'corruption', 'police',
            'cop', 'judge', 'court', 'unfair', 'unjust', 'authority',
            'power abuse', 'abuse of power'], 'identity': ['race',
            'ethnicity', 'religion', 'religious', 'christian', 'muslim',
            'jewish', 'lgbt', 'gay', 'lesbian', 'bisexual', 'transgender',
            'queer']}
        self.compiled_patterns = {}
        for category, words in self.keyword_categories.items():
            escaped = [(re.escape(w) + '\\w*') for w in words]
            pattern = '\\b(?:%s)\\b' % '|'.join(escaped)
            self.compiled_patterns[category] = re.compile(pattern, re.
                IGNORECASE)

    def detect(self, text: str) ->dict:
        if not isinstance(text, str) or not text.strip():
            return {f'has_{cat}': (0) for cat in self.compiled_patterns.keys()}
        flags = {}
        for category, pattern in self.compiled_patterns.items():
            flags[f'has_{category}'] = 1 if pattern.search(text) else 0
        return flags

    def extract_all_matches(self, text: str) ->dict:
        matches_per_cat = {}
        if not isinstance(text, str) or not text.strip():
            return matches_per_cat
        for category, pattern in self.compiled_patterns.items():
            raw_matches = pattern.findall(text)
            if raw_matches:
                cleaned = {m.lower() for m in raw_matches}
                matches_per_cat[category] = sorted(cleaned)
        return matches_per_cat

    def extract_matched_keywords(self, text: str, top_n: int=None) ->dict:
        matches = self.extract_all_matches(text)
        if top_n is None:
            return matches
        limited = {}
        for cat, words in matches.items():
            limited[cat] = words[:top_n]
        return limited


def main():
    print('=' * 70)
    print('ENHANCED LINGUISTIC ANALYSIS FOR REDDIT MODERATION RESEARCH')
    print('=' * 70)
    parser = argparse.ArgumentParser(description='Enhanced linguistic analysis'
        )
    parser.add_argument('--no-toxicity', action='store_true', help=
        'Skip toxicity detection (faster)')
    parser.add_argument('--no-selftext-toxicity', action='store_true', help
        ='Skip selftext toxicity detection (faster)')
    parser.add_argument('--batch-size', type=int, default=16, help=
        'Batch size for toxicity model')
    parser.add_argument('--data-dir', type=str, default='data', help=
        'Data directory')
    parser.add_argument('--sample', type=int, default=None, help=
        'Process only N samples (for testing)')
    parser.add_argument('--n-workers', type=int, default=None, help=
        'Number of parallel workers (default: auto)')
    parser.add_argument('--force', action='store_true', help=
        'Force re-analysis even if output exists')
    args = parser.parse_args()
    if args.data_dir:
        if os.path.isabs(args.data_dir):
            config.data_dir = args.data_dir
        else:
            config.data_dir = os.path.abspath(os.path.join(BASE_DIR, '..',
                args.data_dir))
    else:
        config.data_dir = DATA_DIR
    config.enable_toxicity = not args.no_toxicity
    config.toxicity_batch_size = args.batch_size
    os.makedirs(config.data_dir, exist_ok=True)
    os.makedirs(config.output_dir, exist_ok=True)
    try:
        from pandarallel import pandarallel
        if args.n_workers:
            nb_workers = args.n_workers
        else:
            cpu_count = psutil.cpu_count(logical=True) or 1
            nb_workers = max(1, cpu_count - 1)
        pandarallel.initialize(progress_bar=True, nb_workers=nb_workers)
        print(f'✓ Parallel processing enabled with {nb_workers} workers')
    except Exception as e:
        print(f'⚠️  Pandarallel initialization failed: {e}')
        print('   Falling back to standard pandas apply()')
        pd.Series.parallel_apply = pd.Series.apply
        pd.DataFrame.parallel_apply = pd.DataFrame.apply
    if not check_memory(config.min_memory_gb):
        return
    print('\n📦 Checking dependencies...')
    deps = check_dependencies()
    print(f'\n🔍 Looking for data files in {config.data_dir}/...')
    data_files = glob.glob(f'{config.data_dir}/master_pass2_combined*.csv')
    if not data_files:
        data_files = glob.glob('master_pass2_combined*.csv')
        if not data_files:
            print(f'❌ No data files found in {config.data_dir}/')
            return
    latest_file = max(data_files, key=os.path.getctime)
    print(f'📂 Loading: {latest_file}')
    df = pd.read_csv(latest_file)
    if args.sample:
        print(f'⚠️  Sampling {args.sample} rows for testing')
        df = df.sample(n=min(args.sample, len(df)), random_state=42)
    print(f'✓ Loaded {len(df):,} posts')
    output_file = f'{config.data_dir}/combined_linguistics_enhanced.csv'
    if os.path.exists(output_file) and not args.force:
        print(f'✓ Linguistics file already exists: {output_file}')
        print('  Use --force to re-analyze')
        return
    required_cols = ['title', 'subreddit']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f'❌ Missing required columns: {missing}')
        return
    if 'selftext' not in df.columns:
        print(
            "⚠️  'selftext' column not found. Creating empty selftext column.")
        df['selftext'] = ''
    if ('is_removed_inferred' not in df.columns and 'is_removed' not in df.
        columns):
        print(
            '⚠️  No removal indicator found. Correlation analysis will be skipped.'
            )
    df['title'] = df['title'].fillna('').infer_objects(copy=False)
    df['selftext'] = df['selftext'].fillna('').infer_objects(copy=False)
    print('\n🔧 Initializing analyzers...')
    preprocessor = TextPreprocessor()
    sentiment_analyzer = SentimentAnalyzer()
    readability_analyzer = ReadabilityAnalyzer(use_textstat=deps['textstat'])
    keyword_detector = KeywordDetector()
    if config.enable_toxicity and deps['transformers'] and deps['torch']:
        toxicity_detector = ToxicityDetector(enabled=True, batch_size=
            config.toxicity_batch_size)
    else:
        toxicity_detector = ToxicityDetector(enabled=False)
        if config.enable_toxicity:
            print('   ⚠️  Toxicity detection disabled (missing dependencies)')
    print('\n' + '=' * 70)
    print('FEATURE EXTRACTION')
    print('=' * 70)
    feature_dfs = {}
    print('\n0️⃣  Cleaning text for linguistic analysis...')
    df['title_clean'] = df['title'].parallel_apply(clean_text)
    df['selftext_clean'] = df['selftext'].parallel_apply(clean_text)
    print('   ✓ Text cleaning complete')
    print('\n1️⃣  Extracting structural features (using raw text)...')
    title_features = df['title'].parallel_apply(preprocessor.extract_features)
    title_features_df = pd.DataFrame(title_features.tolist())
    title_features_df.columns = [('title_' + col) for col in
        title_features_df.columns]
    selftext_features = df['selftext'].parallel_apply(preprocessor.
        extract_features)
    selftext_features_df = pd.DataFrame(selftext_features.tolist())
    selftext_features_df.columns = [('selftext_' + col) for col in
        selftext_features_df.columns]
    feature_dfs['title_structural'] = title_features_df
    feature_dfs['selftext_structural'] = selftext_features_df
    print('   ✓ Extracted structural features')
    df.to_csv(f'{config.data_dir}/.linguistics_checkpoint_1_structural.csv',
        index=False)
    print('   💾 Checkpoint saved')
    print('\n2️⃣  Running sentiment analysis (using clean text)...')
    unique_title_texts = df['title'].unique()
    title_sentiment_map = {text: sentiment_analyzer.analyze(text) for text in
        unique_title_texts}
    title_sentiment_df = df['title_clean'].map(title_sentiment_map).apply(pd
        .Series)
    title_sentiment_df.columns = ['title_sentiment_polarity',
        'title_sentiment_subjectivity', 'title_sentiment_pos',
        'title_sentiment_neg']
    unique_selftext_texts = df['selftext_clean'].unique()
    selftext_sentiment_map = {text: sentiment_analyzer.analyze(text) for
        text in unique_selftext_texts}
    selftext_sentiment_df = df['selftext_clean'].map(selftext_sentiment_map
        ).apply(pd.Series)
    selftext_sentiment_df.columns = ['selftext_sentiment_polarity',
        'selftext_sentiment_subjectivity', 'selftext_sentiment_pos',
        'selftext_sentiment_neg']
    feature_dfs['title_sentiment'] = title_sentiment_df
    feature_dfs['selftext_sentiment'] = selftext_sentiment_df
    print('   ✓ Sentiment analysis complete')
    df.to_csv(f'{config.data_dir}/.linguistics_checkpoint_2_sentiment.csv',
        index=False)
    print('   💾 Checkpoint saved')
    print('\n3️⃣  Calculating readability metrics (using clean text)...')
    unique_title_texts_rb = df['title_clean'].unique()
    title_readability_map = {text: readability_analyzer.analyze(text) for
        text in unique_title_texts_rb}
    title_readability_df = df['title_clean'].map(title_readability_map).apply(
        pd.Series)
    title_readability_df.columns = [f'title_readability_{col}' for col in
        title_readability_df.columns]
    unique_selftext_texts_rb = df['selftext_clean'].unique()
    selftext_readability_map = {text: readability_analyzer.analyze(text) for
        text in unique_selftext_texts_rb}
    selftext_readability_df = df['selftext_clean'].map(selftext_readability_map
        ).apply(pd.Series)
    selftext_readability_df.columns = [f'selftext_readability_{col}' for
        col in selftext_readability_df.columns]
    feature_dfs['title_readability'] = title_readability_df
    feature_dfs['selftext_readability'] = selftext_readability_df
    print('   ✓ Readability metrics complete')
    df.to_csv(f'{config.data_dir}/.linguistics_checkpoint_3_readability.csv',
        index=False)
    print('   💾 Checkpoint saved')
    print('\n3️⃣b  Computing lexical richness features (type-token ratio)...')
    title_richness = df['title_clean'].parallel_apply(compute_richness)
    title_richness_df = pd.DataFrame(title_richness.tolist())
    title_richness_df.columns = [('title_' + col) for col in
        title_richness_df.columns]
    selftext_richness = df['selftext_clean'].parallel_apply(compute_richness)
    selftext_richness_df = pd.DataFrame(selftext_richness.tolist())
    selftext_richness_df.columns = [('selftext_' + col) for col in
        selftext_richness_df.columns]
    feature_dfs['title_richness'] = title_richness_df
    feature_dfs['selftext_richness'] = selftext_richness_df
    print('   ✓ Lexical richness metrics complete')
    df.to_csv(f'{config.data_dir}/.linguistics_checkpoint_3b_richness.csv',
        index=False)
    print('   💾 Checkpoint saved')
    print('\n4️⃣  Detecting keywords (using raw text)...')
    title_kw_results = df['title_clean'].parallel_apply(lambda txt: (
        keyword_detector.detect(txt), keyword_detector.
        extract_matched_keywords(txt, top_n=20)))
    title_flags = title_kw_results.apply(lambda x: x[0])
    title_matches = title_kw_results.apply(lambda x: x[1])
    title_keywords_df = pd.DataFrame(title_flags.tolist())
    title_keywords_df.columns = [('title_' + col) for col in
        title_keywords_df.columns]
    df['title_keyword_matches'] = title_matches.apply(lambda matches: json.
        dumps(matches, ensure_ascii=False))
    selftext_kw_results = df['selftext'].parallel_apply(lambda txt: (
        keyword_detector.detect(txt), keyword_detector.
        extract_matched_keywords(txt, top_n=20)))
    selftext_flags = selftext_kw_results.apply(lambda x: x[0])
    selftext_matches = selftext_kw_results.apply(lambda x: x[1])
    selftext_keywords_df = pd.DataFrame(selftext_flags.tolist())
    selftext_keywords_df.columns = [('selftext_' + col) for col in
        selftext_keywords_df.columns]
    df['selftext_keyword_matches'] = selftext_matches.apply(lambda matches:
        json.dumps(matches, ensure_ascii=False))
    feature_dfs['title_keywords'] = title_keywords_df
    feature_dfs['selftext_keywords'] = selftext_keywords_df
    print('   ✓ Keyword detection complete')
    df.to_csv(f'{config.data_dir}/.linguistics_checkpoint_4_keywords.csv',
        index=False)
    print('   💾 Checkpoint saved')
    if toxicity_detector.enabled:
        print('\n5️⃣  Running toxicity detection (Optimized)...')
        print('   > Processing Titles...')
        unique_titles = df['title_clean'].unique()
        dup_pct = (1 - len(unique_titles) / len(df)) * 100
        print(
            f'Reduced {len(df):,} titles to {len(unique_titles):,} unique texts ({dup_pct:.1f}% duplicates)'
            )
        unique_scores = []
        original_batch = toxicity_detector.batch_size
        toxicity_detector.batch_size = 64
        for i in tqdm(range(0, len(unique_titles), 64), desc=
            '     Inferencing Unique Titles'):
            batch = unique_titles[i:i + 64]
            batch_scores = toxicity_detector.analyze_batch(batch,
                truncation_len=config.max_title_length)
            unique_scores.extend(batch_scores)
        title_score_map = dict(zip(unique_titles, unique_scores))
        df['title_toxicity'] = df['title_clean'].map(title_score_map)
        if not args.no_selftext_toxicity:
            print('   > Processing Selftext...')
            unique_bodies = df['selftext_clean'].unique()
            dup_pct_body = (1 - len(unique_bodies) / len(df)) * 100
            print(
                f'     Reduced {len(df):,} bodies to {len(unique_bodies):,} unique texts ({dup_pct_body:.1f}% duplicates)'
                )
            print('     Sorting data for smart batching...')
            sorted_indices = np.argsort([len(s) for s in unique_bodies])[::-1]
            unique_bodies_sorted = unique_bodies[sorted_indices]
            unique_body_scores = []
            toxicity_detector.batch_size = 32
            for i in tqdm(range(0, len(unique_bodies_sorted), 32), desc=
                '     Inferencing Bodies (Smart Batched)'):
                batch = unique_bodies_sorted[i:i + 32]
                batch_scores = toxicity_detector.analyze_batch(batch,
                    truncation_len=config.max_selftext_length)
                unique_body_scores.extend(batch_scores)
            body_score_map = dict(zip(unique_bodies_sorted, unique_body_scores)
                )
            df['selftext_toxicity'] = df['selftext_clean'].map(body_score_map)
        else:
            df['selftext_toxicity'] = 0.0
            print('   ⚠️  Selftext toxicity skipped')
        toxicity_detector.batch_size = original_batch
        df.to_csv(f'{config.data_dir}/.linguistics_checkpoint_5_toxicity.csv',
            index=False)
        print('   💾 Checkpoint saved')
    else:
        df['title_toxicity'] = 0.0
        df['selftext_toxicity'] = 0.0
        print('\n5️⃣  Toxicity detection skipped entirely')
    print('\n📊 Combining all linguistic features...')
    if feature_dfs:
        df = pd.concat([df] + list(feature_dfs.values()), axis=1)
        print(f'   ✓ Combined {len(feature_dfs)} feature sets')
    print('\n' + '=' * 70)
    print('FEATURE SUMMARY')
    print('=' * 70)
    ling_cols = [col for col in df.columns if any(prefix in col for prefix in
        ['title_', 'selftext_']) and col not in ['title', 'selftext']]
    summary_stats = df[ling_cols].describe()
    print(f'\n✓ Generated {len(ling_cols)} linguistic features')
    print(f'\nSample statistics:')
    print(summary_stats.iloc[:, :5].to_string())
    print('\n=== CORRELATION WITH REMOVAL (ROBUST) ===')
    ling_cols = [c for c in df.columns if (c.startswith('title_') or c.
        startswith('selftext_')) and c not in ['title', 'selftext',
        'title_clean', 'selftext_clean']]
    numeric_ling_cols = [c for c in ling_cols if pd.api.types.
        is_numeric_dtype(df[c])]
    targets = []
    if 'is_removed_inferred' in df.columns:
        targets.append('is_removed_inferred')
    if 'is_removed' in df.columns:
        targets.append('is_removed')
    if not targets:
        print(
            "⚠️  No removal indicator found (neither 'is_removed_inferred' nor 'is_removed'). Skipping correlation."
            )
    else:
        all_corr_rows = []
        print(f'Using targets: {targets}')
        for target_col in targets:
            target_series = df[target_col]
            valid_mask = target_series.notna()
            corr_df = df.loc[valid_mask, numeric_ling_cols + [target_col]]
            corr_series = corr_df.corr()[target_col].drop(labels=[target_col])
            corr_df_out = corr_series.rename('correlation').reset_index(
                ).rename(columns={'index': 'feature'})
            corr_df_out['abs_correlation'] = corr_df_out['correlation'].abs()
            corr_df_out['target'] = target_col
            all_corr_rows.append(corr_df_out)
            print(f'\nTop 10 absolute correlations with {target_col}:')
            top10 = corr_df_out.sort_values('abs_correlation', ascending=False
                ).head(10)
            for _, row in top10.iterrows():
                print(
                    f"  {row['feature']}: {row['correlation']:.4f} (|r|={row['abs_correlation']:.4f})"
                    )
        corr_all = pd.concat(all_corr_rows, ignore_index=True)
        corr_all.sort_values(['target', 'abs_correlation'], ascending=[True,
            False], inplace=True)
        corr_out_path = os.path.join(config.output_dir,
            'feature_correlations.csv')
        corr_all.to_csv(corr_out_path, index=False)
        print(f'\n✓ Saved feature correlations to: {corr_out_path}')
    print('\n' + '=' * 70)
    print('SAVING RESULTS')
    print('=' * 70)
    output_file = f'{config.data_dir}/combined_linguistics_enhanced.csv'
    df.to_csv(output_file, index=False)
    print(f'✓ Saved main dataset: {output_file}')
    print('\n🧹 Cleaning up checkpoint files...')
    checkpoint_files = glob.glob(
        f'{config.data_dir}/.linguistics_checkpoint_*.csv')
    for cp_file in checkpoint_files:
        try:
            os.remove(cp_file)
        except:
            pass
    print(f'   ✓ Removed {len(checkpoint_files)} checkpoint files')
    feature_list = {'extraction_date': datetime.now().isoformat(),
        'total_posts': len(df), 'features': ling_cols, 'feature_count': len
        (ling_cols), 'toxicity_enabled': toxicity_detector.enabled,
        'textstat_available': deps['textstat']}
    os.makedirs(config.output_dir, exist_ok=True)
    with open(f'{config.output_dir}/feature_metadata.json', 'w') as f:
        json.dump(feature_list, f, indent=2)
    print(f'✓ Saved metadata: {config.output_dir}/feature_metadata.json')
    print('\n📝 Saving keyword examples...')
    for category in keyword_detector.keyword_categories.keys():
        title_col = f'title_has_{category}'
        selftext_col = f'selftext_has_{category}'
        if title_col in df.columns and selftext_col in df.columns:
            flagged = df[(df[title_col] == 1) | (df[selftext_col] == 1)]
            if len(flagged) > 0:
                cols = ['subreddit', 'title', 'selftext']
                if 'is_removed_inferred' in flagged.columns:
                    cols.append('is_removed_inferred')
                sample = flagged[cols].head(20)
                sample.to_csv(
                    f'{config.output_dir}/sample_keyword_{category}.csv',
                    index=False)
    print(f'   ✓ Saved keyword samples to {config.output_dir}/')
    print('\n' + '=' * 70)
    print('✅ LINGUISTIC ANALYSIS COMPLETE!')
    print('=' * 70)
    print(f'\n📊 Results:')
    print(f'   • Total posts analyzed: {len(df):,}')
    print(f'   • Linguistic features: {len(ling_cols)}')
    print(f'   • Output file: {output_file}')
    print(
        f'   • Feature correlations: {config.output_dir}/feature_correlations.csv'
        )
    print(f'   • Feature metadata: {config.output_dir}/feature_metadata.json')
    print(f'\n🔬 Feature Categories:')
    feature_counts = {'Structural': len([c for c in ling_cols if c.endswith
        (('_length', '_word_count', '_avg_word_length',
        '_punctuation_density', '_all_caps_ratio')) and not 'readability' in
        c]), 'Sentiment': len([c for c in ling_cols if '_sentiment_' in c]),
        'Readability': len([c for c in ling_cols if '_readability_' in c]),
        'Keywords': len([c for c in ling_cols if c.startswith(('title_has_',
        'selftext_has_'))]), 'Toxicity': len([c for c in ling_cols if c.
        endswith('_toxicity')])}
    for cat, count in feature_counts.items():
        print(f'   • {cat}: {count} features')

if __name__ == '__main__':
    main()
