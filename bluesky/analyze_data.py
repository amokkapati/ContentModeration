import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from scipy import stats
from scipy.stats import chi2_contingency, mannwhitneyu, ttest_ind
import warnings
warnings.filterwarnings('ignore')

# Set publication-quality plotting defaults
sns.set_style("whitegrid")
sns.set_context("paper", font_scale=1.3)
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['savefig.bbox'] = 'tight'

class BlueskyDataAnalyzer:
    def __init__(self, csv_file):
        """Load and prepare the data."""
        print(f"Loading data from {csv_file}...")
        self.df = pd.read_csv(csv_file)
        print(f"✓ Loaded {len(self.df)} posts\n")
        
        # Parse JSON fields
        self.df['moderation_labels_parsed'] = self.df['moderation_labels'].apply(
            lambda x: json.loads(x) if pd.notna(x) and x != '[]' else []
        )
        
        # Create derived features
        self._create_derived_features()
        
    def _create_derived_features(self):
        """Create additional features for analysis."""
        # Total engagement
        self.df['total_engagement'] = (
            self.df['likes_count'] + 
            self.df['repost_count'] + 
            self.df['reply_count']
        )
        
        # Engagement rate (normalized by followers)
        self.df['engagement_rate'] = self.df.apply(
            lambda row: row['total_engagement'] / max(row['author_followers'], 1),
            axis=1
        )
        
        # Account age proxy (posts per follower)
        self.df['posts_per_follower'] = self.df.apply(
            lambda row: row['author_posts'] / max(row['author_followers'], 1),
            axis=1
        )
        
        # Categorize follower counts
        bins = [0, 100, 1000, 10000, 100000, float('inf')]
        labels = ['<100', '100-1K', '1K-10K', '10K-100K', '>100K']
        self.df['follower_category'] = pd.cut(
            self.df['author_followers'], 
            bins=bins, 
            labels=labels
        )
        
        print("✓ Created derived features")
    
    def calculate_confidence_interval(self, data, confidence=0.95):
        """Calculate confidence interval for a proportion."""
        n = len(data)
        if n == 0:
            return 0, 0, 0
        
        p = data.sum() / n
        z = stats.norm.ppf((1 + confidence) / 2)
        margin = z * np.sqrt(p * (1 - p) / n)
        
        return p, max(0, p - margin), min(1, p + margin)
    
    def cohens_d(self, group1, group2):
        """Calculate Cohen's d effect size."""
        n1, n2 = len(group1), len(group2)
        var1, var2 = np.var(group1, ddof=1), np.var(group2, ddof=1)
        pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
        
        if pooled_std == 0:
            return 0
        
        return (np.mean(group1) - np.mean(group2)) / pooled_std
    
    def cramers_v(self, confusion_matrix):
        """Calculate Cramér's V effect size for categorical data."""
        chi2 = stats.chi2_contingency(confusion_matrix)[0]
        n = confusion_matrix.sum()
        min_dim = min(confusion_matrix.shape) - 1
        
        if n == 0 or min_dim == 0:
            return 0
        
        return np.sqrt(chi2 / (n * min_dim))
    
    def basic_statistics(self):
        """Calculate and display basic statistics with confidence intervals."""
        print("=" * 80)
        print("BASIC STATISTICS")
        print("=" * 80)
        
        total_posts = len(self.df)
        moderated_posts = self.df['has_moderation'].sum()
        
        # Calculate confidence interval for moderation rate
        mod_rate, mod_ci_low, mod_ci_high = self.calculate_confidence_interval(
            self.df['has_moderation']
        )
        
        print(f"Total posts collected: {total_posts:,}")
        print(f"Posts with moderation labels: {moderated_posts:,}")
        print(f"Overall moderation rate: {mod_rate*100:.2f}% (95% CI: [{mod_ci_low*100:.2f}%, {mod_ci_high*100:.2f}%])")
        print(f"\nUnique authors: {self.df['author_handle'].nunique():,}")
        print(f"Search terms used: {self.df['search_term'].nunique()}")
        
        print(f"\nEngagement Metrics:")
        engagement_stats = self.df[['likes_count', 'repost_count', 'reply_count', 'total_engagement']].describe()
        print(engagement_stats.round(2).to_string())
        
        print(f"\nAuthor Demographics:")
        author_stats = self.df[['author_followers', 'author_following', 'author_posts']].describe()
        print(author_stats.round(2).to_string())
        
        return {
            'total_posts': total_posts,
            'moderated_posts': moderated_posts,
            'moderation_rate': mod_rate,
            'ci_low': mod_ci_low,
            'ci_high': mod_ci_high
        }
    
    def analyze_by_search_term(self):
        """Analyze moderation rates by search term with statistical tests."""
        print("\n" + "=" * 80)
        print("MODERATION BY SEARCH TERM (WITH STATISTICAL TESTS)")
        print("=" * 80)
        
        term_analysis = []
        
        for term in self.df['search_term'].unique():
            term_data = self.df[self.df['search_term'] == term]['has_moderation']
            
            rate, ci_low, ci_high = self.calculate_confidence_interval(term_data)
            
            term_analysis.append({
                'Search Term': term,
                'Total': len(term_data),
                'Moderated': term_data.sum(),
                'Rate (%)': rate * 100,
                'CI Low (%)': ci_low * 100,
                'CI High (%)': ci_high * 100
            })
        
        term_df = pd.DataFrame(term_analysis).sort_values('Rate (%)', ascending=False)
        print(term_df.to_string(index=False))
        
        # Chi-square test for independence
        if len(term_df) > 1:
            contingency = pd.crosstab(
                self.df['search_term'], 
                self.df['has_moderation']
            )
            chi2, p_value, dof, expected = chi2_contingency(contingency)
            cramers = self.cramers_v(contingency.values)
            
            print(f"\nChi-square test for independence:")
            print(f"  χ² = {chi2:.2f}, df = {dof}, p = {p_value:.4f}")
            print(f"  Cramér's V (effect size) = {cramers:.3f}")
            
            if p_value < 0.05:
                print(f"  ✓ Significant difference in moderation rates across search terms (p < 0.05)")
            else:
                print(f"  ✗ No significant difference in moderation rates across search terms (p ≥ 0.05)")
        
        # Visualization with error bars
        plt.figure(figsize=(14, 7))
        x = range(len(term_df))
        plt.bar(x, term_df['Rate (%)'], alpha=0.7, color='steelblue')
        plt.errorbar(
            x, 
            term_df['Rate (%)'], 
            yerr=[
                term_df['Rate (%)'] - term_df['CI Low (%)'],
                term_df['CI High (%)'] - term_df['Rate (%)']
            ],
            fmt='none',
            ecolor='black',
            capsize=5,
            alpha=0.7
        )
        plt.xlabel('Search Term', fontsize=12, fontweight='bold')
        plt.ylabel('Moderation Rate (%)', fontsize=12, fontweight='bold')
        plt.title('Moderation Rate by Search Term (with 95% Confidence Intervals)', 
                 fontsize=14, fontweight='bold')
        plt.xticks(x, term_df['Search Term'], rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig('moderation_by_term_enhanced.png')
        print("\n✓ Saved chart: moderation_by_term_enhanced.png")
        plt.close()
        
        return term_df
    
    def analyze_by_follower_count(self):
        """Analyze moderation patterns by follower count with statistical tests."""
        print("\n" + "=" * 80)
        print("MODERATION BY FOLLOWER COUNT (WITH STATISTICAL TESTS)")
        print("=" * 80)
        
        follower_analysis = []
        
        for category in self.df['follower_category'].cat.categories:
            cat_data = self.df[self.df['follower_category'] == category]['has_moderation']
            
            if len(cat_data) == 0:
                continue
            
            rate, ci_low, ci_high = self.calculate_confidence_interval(cat_data)
            
            follower_analysis.append({
                'Follower Category': category,
                'Total': len(cat_data),
                'Moderated': cat_data.sum(),
                'Rate (%)': rate * 100,
                'CI Low (%)': ci_low * 100,
                'CI High (%)': ci_high * 100
            })
        
        follower_df = pd.DataFrame(follower_analysis)
        print(follower_df.to_string(index=False))
        
        # Chi-square test
        contingency = pd.crosstab(
            self.df['follower_category'], 
            self.df['has_moderation']
        )
        chi2, p_value, dof, expected = chi2_contingency(contingency)
        cramers = self.cramers_v(contingency.values)
        
        print(f"\nChi-square test for independence:")
        print(f"  χ² = {chi2:.2f}, df = {dof}, p = {p_value:.4f}")
        print(f"  Cramér's V (effect size) = {cramers:.3f}")
        
        if p_value < 0.05:
            print(f"  ✓ Significant relationship between account size and moderation (p < 0.05)")
        else:
            print(f"  ✗ No significant relationship between account size and moderation (p ≥ 0.05)")
        
        # Spearman correlation (follower count as continuous variable)
        correlation, corr_p = stats.spearmanr(
            self.df['author_followers'], 
            self.df['has_moderation']
        )
        print(f"\nSpearman correlation (followers vs. moderation):")
        print(f"  ρ = {correlation:.3f}, p = {corr_p:.4f}")
        
        # Visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Bar chart with error bars
        x = range(len(follower_df))
        ax1.bar(x, follower_df['Rate (%)'], alpha=0.7, color='coral')
        ax1.errorbar(
            x, 
            follower_df['Rate (%)'], 
            yerr=[
                follower_df['Rate (%)'] - follower_df['CI Low (%)'],
                follower_df['CI High (%)'] - follower_df['Rate (%)']
            ],
            fmt='none',
            ecolor='black',
            capsize=5,
            alpha=0.7
        )
        ax1.set_xlabel('Follower Count Category', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Moderation Rate (%)', fontsize=12, fontweight='bold')
        ax1.set_title('Moderation Rate by Account Size', fontsize=14, fontweight='bold')
        ax1.set_xticks(x)
        ax1.set_xticklabels(follower_df['Follower Category'])
        ax1.grid(axis='y', alpha=0.3)
        
        # Sample size chart
        ax2.bar(x, follower_df['Total'], alpha=0.7, color='lightblue')
        ax2.set_xlabel('Follower Count Category', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Posts', fontsize=12, fontweight='bold')
        ax2.set_title('Sample Size by Category', fontsize=14, fontweight='bold')
        ax2.set_xticks(x)
        ax2.set_xticklabels(follower_df['Follower Category'])
        ax2.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('moderation_by_followers_enhanced.png')
        print("\n✓ Saved chart: moderation_by_followers_enhanced.png")
        plt.close()
        
        return follower_df
    
    def analyze_moderation_labels(self):
        """Analyze types of moderation labels with detailed statistics."""
        print("\n" + "=" * 80)
        print("MODERATION LABEL ANALYSIS")
        print("=" * 80)
        
        all_labels = []
        label_sources = []
        label_types_list = []
        
        for labels_list in self.df['moderation_labels_parsed']:
            for label in labels_list:
                if isinstance(label, dict) and 'value' in label:
                    all_labels.append(label['value'])
                    label_sources.append(label.get('source', 'unknown'))
                    label_types_list.append(label.get('type', 'unknown'))
        
        if not all_labels:
            print("No moderation labels found in dataset.")
            return None
        
        label_counts = Counter(all_labels)
        source_counts = Counter(label_sources)
        type_counts = Counter(label_types_list)
        
        print(f"Total labels applied: {len(all_labels):,}")
        print(f"Unique label types: {len(label_counts)}")
        print(f"Posts with labels: {self.df['has_moderation'].sum():,}")
        print(f"Average labels per moderated post: {len(all_labels) / max(self.df['has_moderation'].sum(), 1):.2f}")
        
        print("\nTop 10 Label Types:")
        for label, count in label_counts.most_common(10):
            pct = (count / len(all_labels)) * 100
            print(f"  {label:20s}: {count:5d} ({pct:5.2f}%)")
        
        print("\nLabel Sources:")
        for source, count in source_counts.most_common():
            pct = (count / len(all_labels)) * 100
            print(f"  {source:20s}: {count:5d} ({pct:5.2f}%)")
        
        print("\nLabel Application Types:")
        for ltype, count in type_counts.most_common():
            pct = (count / len(all_labels)) * 100
            print(f"  {ltype:20s}: {count:5d} ({pct:5.2f}%)")
        
        # Visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Top labels
        top_labels = label_counts.most_common(10)
        labels, counts = zip(*top_labels) if top_labels else ([], [])
        ax1.barh(range(len(labels)), counts, color='mediumpurple', alpha=0.7)
        ax1.set_yticks(range(len(labels)))
        ax1.set_yticklabels(labels)
        ax1.set_xlabel('Count', fontsize=12, fontweight='bold')
        ax1.set_title('Top 10 Moderation Labels', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)
        
        # Label sources pie chart
        if source_counts:
            sources, source_vals = zip(*source_counts.most_common())
            ax2.pie(source_vals, labels=sources, autopct='%1.1f%%', startangle=90)
            ax2.set_title('Label Sources', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('moderation_label_analysis.png')
        print("\n✓ Saved chart: moderation_label_analysis.png")
        plt.close()
        
        return {
            'label_counts': label_counts,
            'source_counts': source_counts,
            'type_counts': type_counts
        }
    
    def analyze_engagement_moderation(self):
        """Compare engagement between moderated and non-moderated posts with statistical tests."""
        print("\n" + "=" * 80)
        print("ENGAGEMENT ANALYSIS: MODERATED vs NON-MODERATED")
        print("=" * 80)
        
        moderated = self.df[self.df['has_moderation'] == True]
        non_moderated = self.df[self.df['has_moderation'] == False]
        
        if len(moderated) == 0:
            print("No moderated posts to compare.")
            return None
        
        metrics = ['likes_count', 'repost_count', 'reply_count', 'total_engagement']
        metric_names = ['Likes', 'Reposts', 'Replies', 'Total Engagement']
        
        results = []
        
        for metric, name in zip(metrics, metric_names):
            mod_vals = moderated[metric]
            non_mod_vals = non_moderated[metric]
            
            # Calculate statistics
            mod_mean = mod_vals.mean()
            non_mod_mean = non_mod_vals.mean()
            mod_median = mod_vals.median()
            non_mod_median = non_mod_vals.median()
            
            # Mann-Whitney U test (non-parametric, better for skewed data)
            statistic, p_value = mannwhitneyu(mod_vals, non_mod_vals, alternative='two-sided')
            
            # Cohen's d effect size
            effect_size = self.cohens_d(mod_vals, non_mod_vals)
            
            results.append({
                'Metric': name,
                'Moderated Mean': mod_mean,
                'Non-Moderated Mean': non_mod_mean,
                'Difference': mod_mean - non_mod_mean,
                'p-value': p_value,
                'Effect Size (d)': effect_size
            })
            
            print(f"\n{name}:")
            print(f"  Moderated: μ = {mod_mean:.2f}, median = {mod_median:.2f}")
            print(f"  Non-Moderated: μ = {non_mod_mean:.2f}, median = {non_mod_median:.2f}")
            print(f"  Mann-Whitney U: p = {p_value:.4f}")
            print(f"  Cohen's d: {effect_size:.3f}", end="")
            
            if abs(effect_size) < 0.2:
                print(" (negligible)")
            elif abs(effect_size) < 0.5:
                print(" (small)")
            elif abs(effect_size) < 0.8:
                print(" (medium)")
            else:
                print(" (large)")
            
            if p_value < 0.05:
                print(f"  ✓ Significant difference (p < 0.05)")
            else:
                print(f"  ✗ No significant difference (p ≥ 0.05)")
        
        results_df = pd.DataFrame(results)
        
        # Visualization
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.ravel()
        
        for idx, (metric, name) in enumerate(zip(metrics, metric_names)):
            ax = axes[idx]
            
            data_to_plot = [
                non_moderated[metric].values,
                moderated[metric].values
            ]
            
            bp = ax.boxplot(data_to_plot, labels=['Non-Moderated', 'Moderated'],
                           patch_artist=True, showfliers=False)
            
            # Color boxes
            bp['boxes'][0].set_facecolor('lightblue')
            bp['boxes'][1].set_facecolor('lightcoral')
            
            ax.set_ylabel(name, fontsize=11, fontweight='bold')
            ax.set_title(f'{name} Distribution', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            
            # Add p-value annotation
            p_val = results_df[results_df['Metric'] == name]['p-value'].values[0]
            if p_val < 0.001:
                sig_text = "p < 0.001***"
            elif p_val < 0.01:
                sig_text = f"p = {p_val:.3f}**"
            elif p_val < 0.05:
                sig_text = f"p = {p_val:.3f}*"
            else:
                sig_text = f"p = {p_val:.3f} (n.s.)"
            
            ax.text(0.5, 0.95, sig_text, transform=ax.transAxes,
                   ha='center', va='top', fontsize=10,
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        plt.savefig('engagement_comparison_enhanced.png')
        print("\n✓ Saved chart: engagement_comparison_enhanced.png")
        plt.close()
        
        return results_df
    
    def analyze_moderation_consistency(self):
        """Analyze consistency of moderation across similar content."""
        print("\n" + "=" * 80)
        print("MODERATION CONSISTENCY ANALYSIS")
        print("=" * 80)
        
        # Group by search term and follower category
        consistency_analysis = self.df.groupby(['search_term', 'follower_category']).agg({
            'has_moderation': ['sum', 'count', 'mean']
        }).round(4)
        
        consistency_analysis.columns = ['Moderated', 'Total', 'Rate']
        consistency_analysis = consistency_analysis[consistency_analysis['Total'] >= 10]  # Min sample size
        consistency_analysis = consistency_analysis.reset_index()
        
        if len(consistency_analysis) == 0:
            print("Insufficient data for consistency analysis")
            return None
        
        # Calculate coefficient of variation for each search term
        cv_by_term = []
        for term in consistency_analysis['search_term'].unique():
            term_data = consistency_analysis[consistency_analysis['search_term'] == term]
            if len(term_data) > 1:
                rates = term_data['Rate'].values
                cv = np.std(rates) / np.mean(rates) if np.mean(rates) > 0 else 0
                cv_by_term.append({
                    'search_term': term,
                    'cv': cv,
                    'n_categories': len(term_data)
                })
        
        cv_df = pd.DataFrame(cv_by_term).sort_values('cv', ascending=False)
        
        print("\nCoefficient of Variation by Search Term:")
        print("(Lower CV = more consistent moderation across account sizes)")
        print(cv_df.to_string(index=False))
        
        print("\n" + consistency_analysis.to_string())
        
        return consistency_analysis
    
    def temporal_analysis(self):
        """Analyze temporal patterns in moderation if timestamp data available."""
        if 'created_at' not in self.df.columns:
            print("\n⊙ Temporal analysis skipped: no timestamp data")
            return None
        
        print("\n" + "=" * 80)
        print("TEMPORAL ANALYSIS")
        print("=" * 80)
        
        try:
            self.df['created_datetime'] = pd.to_datetime(self.df['created_at'])
            self.df['hour_of_day'] = self.df['created_datetime'].dt.hour
            self.df['day_of_week'] = self.df['created_datetime'].dt.day_name()
            
            # Analysis by hour
            hourly = self.df.groupby('hour_of_day').agg({
                'has_moderation': ['sum', 'count', 'mean']
            })
            hourly.columns = ['Moderated', 'Total', 'Rate']
            
            print("\nModeration Rate by Hour of Day:")
            print(hourly.round(4).to_string())
            
            # Visualization
            plt.figure(figsize=(12, 5))
            plt.plot(hourly.index, hourly['Rate'] * 100, marker='o', linewidth=2, markersize=6)
            plt.xlabel('Hour of Day (UTC)', fontsize=12, fontweight='bold')
            plt.ylabel('Moderation Rate (%)', fontsize=12, fontweight='bold')
            plt.title('Moderation Rate by Time of Day', fontsize=14, fontweight='bold')
            plt.grid(alpha=0.3)
            plt.xticks(range(0, 24, 2))
            plt.tight_layout()
            plt.savefig('temporal_analysis.png')
            print("\n✓ Saved chart: temporal_analysis.png")
            plt.close()
            
            return hourly
        except Exception as e:
            print(f"Error in temporal analysis: {e}")
            return None
    
    def generate_statistical_summary(self, output_file="statistical_summary.txt"):
        """Generate comprehensive statistical summary report."""
        print("\n" + "=" * 80)
        print("GENERATING STATISTICAL SUMMARY REPORT")
        print("=" * 80)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("BLUESKY CONTENT MODERATION - STATISTICAL ANALYSIS REPORT\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Generated: {pd.Timestamp.now()}\n")
            f.write(f"Dataset: {len(self.df):,} posts\n\n")
            
            # Basic stats
            stats = self.basic_statistics()
            f.write(f"Overall Moderation Rate: {stats['moderation_rate']*100:.2f}%\n")
            f.write(f"95% Confidence Interval: [{stats['ci_low']*100:.2f}%, {stats['ci_high']*100:.2f}%]\n\n")
            
            # Key findings
            f.write("KEY STATISTICAL FINDINGS:\n")
            f.write("-" * 80 + "\n\n")
            
            # By search term
            term_analysis = self.analyze_by_search_term()
            if term_analysis is not None and len(term_analysis) > 0:
                top_term = term_analysis.iloc[0]
                f.write(f"1. Highest moderation rate: '{top_term['Search Term']}' ")
                f.write(f"({top_term['Rate (%)']:.2f}%, n={top_term['Total']})\n")
            
            # By follower count
            follower_analysis = self.analyze_by_follower_count()
            if follower_analysis is not None and len(follower_analysis) > 0:
                f.write(f"\n2. Moderation rates by account size:\n")
                for _, row in follower_analysis.iterrows():
                    f.write(f"   - {row['Follower Category']:10s}: {row['Rate (%)']:6.2f}% ")
                    f.write(f"(95% CI: [{row['CI Low (%)']:.2f}%, {row['CI High (%)']:.2f}%])\n")
            
            # Label analysis
            label_analysis = self.analyze_moderation_labels()
            if label_analysis and 'label_counts' in label_analysis:
                f.write(f"\n3. Most common moderation labels:\n")
                for label, count in label_analysis['label_counts'].most_common(5):
                    pct = (count / len(self.df)) * 100
                    f.write(f"   - {label}: {count:,} posts ({pct:.2f}%)\n")
            
            # Engagement comparison
            engagement_results = self.analyze_engagement_moderation()
            if engagement_results is not None:
                f.write(f"\n4. Engagement differences (Moderated vs. Non-Moderated):\n")
                for _, row in engagement_results.iterrows():
                    f.write(f"   - {row['Metric']}: ")
                    f.write(f"Δ = {row['Difference']:.2f}, ")
                    f.write(f"d = {row['Effect Size (d)']:.3f}, ")
                    f.write(f"p = {row['p-value']:.4f}\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("INTERPRETATION GUIDELINES:\n")
            f.write("-" * 80 + "\n")
            f.write("p-value: probability of observing results if null hypothesis is true\n")
            f.write("  - p < 0.05: statistically significant\n")
            f.write("  - p < 0.01: highly significant\n")
            f.write("  - p < 0.001: very highly significant\n\n")
            f.write("Effect size (Cohen's d):\n")
            f.write("  - |d| < 0.2: negligible effect\n")
            f.write("  - 0.2 ≤ |d| < 0.5: small effect\n")
            f.write("  - 0.5 ≤ |d| < 0.8: medium effect\n")
            f.write("  - |d| ≥ 0.8: large effect\n\n")
            f.write("Cramér's V:\n")
            f.write("  - V < 0.1: negligible association\n")
            f.write("  - 0.1 ≤ V < 0.3: weak association\n")
            f.write("  - 0.3 ≤ V < 0.5: moderate association\n")
            f.write("  - V ≥ 0.5: strong association\n")
        
        print(f"✓ Saved statistical summary: {output_file}")


def main():
    """Main execution function."""
    import sys
    
    print("=" * 80)
    print("BLUESKY MODERATION DATA - ENHANCED STATISTICAL ANALYSIS")
    print("=" * 80)
    print()
    
    # Get CSV file from command line or use default
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'moderation_data.csv'
    
    if not pd.io.common.file_exists(csv_file):
        print(f"Error: File '{csv_file}' not found")
        print("Usage: python analyze_data_enhanced.py [csv_file]")
        return
    
    # Create analyzer
    analyzer = BlueskyDataAnalyzer(csv_file)
    
    # Run all analyses
    print("\n[1/7] Basic Statistics...")
    analyzer.basic_statistics()
    
    print("\n[2/7] Search Term Analysis...")
    analyzer.analyze_by_search_term()
    
    print("\n[3/7] Follower Count Analysis...")
    analyzer.analyze_by_follower_count()
    
    print("\n[4/7] Label Analysis...")
    analyzer.analyze_moderation_labels()
    
    print("\n[5/7] Engagement Analysis...")
    analyzer.analyze_engagement_moderation()
    
    print("\n[6/7] Consistency Analysis...")
    analyzer.analyze_moderation_consistency()
    
    print("\n[7/7] Temporal Analysis...")
    analyzer.temporal_analysis()
    
    # Generate summary report
    analyzer.generate_statistical_summary()
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE!")
    print("=" * 80)
    print("\nGenerated files:")
    print("  - moderation_by_term_enhanced.png")
    print("  - moderation_by_followers_enhanced.png")
    print("  - moderation_label_analysis.png")
    print("  - engagement_comparison_enhanced.png")
    print("  - temporal_analysis.png (if applicable)")
    print("  - statistical_summary.txt")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()