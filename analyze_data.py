import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

class BlueskyDataAnalyzer:
    def __init__(self, csv_file):
        """Load and prepare the data."""
        print(f"Loading data from {csv_file}...")
        self.df = pd.read_csv(csv_file)
        print(f"✓ Loaded {len(self.df)} posts\n")
        
        self.df['moderation_labels_parsed'] = self.df['moderation_labels'].apply(
            lambda x: json.loads(x) if pd.notna(x) and x != '[]' else []
        )
        
    def basic_statistics(self):
        """Calculate and display basic statistics."""
        print("=" * 70)
        print("BASIC STATISTICS")
        print("=" * 70)
        
        total_posts = len(self.df)
        moderated_posts = self.df['has_moderation'].sum()
        moderation_rate = (moderated_posts / total_posts) * 100
        
        print(f"Total posts collected: {total_posts}")
        print(f"Posts with moderation labels: {moderated_posts}")
        print(f"Overall moderation rate: {moderation_rate:.2f}%")
        print(f"\nUnique authors: {self.df['author_handle'].nunique()}")
        print(f"Search terms used: {self.df['search_term'].nunique()}")
        
        print(f"\nEngagement Metrics (Average):")
        print(f"  Likes: {self.df['likes_count'].mean():.1f}")
        print(f"  Reposts: {self.df['repost_count'].mean():.1f}")
        print(f"  Replies: {self.df['reply_count'].mean():.1f}")
        
        return {
            'total_posts': total_posts,
            'moderated_posts': moderated_posts,
            'moderation_rate': moderation_rate
        }
    
    def analyze_by_search_term(self):
        """Analyze moderation rates by search term."""
        print("\n" + "=" * 70)
        print("MODERATION BY SEARCH TERM")
        print("=" * 70)
        
        term_analysis = self.df.groupby('search_term').agg({
            'has_moderation': ['sum', 'count', 'mean']
        }).round(4)
        
        term_analysis.columns = ['Moderated', 'Total', 'Rate']
        term_analysis['Rate'] = term_analysis['Rate'] * 100
        term_analysis = term_analysis.sort_values('Rate', ascending=False)
        
        print(term_analysis.to_string())
        
        plt.figure(figsize=(10, 6))
        plt.bar(range(len(term_analysis)), term_analysis['Rate'])
        plt.xlabel('Search Term')
        plt.ylabel('Moderation Rate (%)')
        plt.title('Moderation Rate by Search Term')
        plt.xticks(range(len(term_analysis)), term_analysis.index, rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig('moderation_by_term.png', dpi=300, bbox_inches='tight')
        print("\n✓ Saved chart: moderation_by_term.png")
        plt.close()
        
        return term_analysis
    
    def analyze_by_follower_count(self):
        """Analyze moderation patterns by author follower count."""
        print("\n" + "=" * 70)
        print("MODERATION BY FOLLOWER COUNT")
        print("=" * 70)
        
        bins = [0, 100, 1000, 10000, float('inf')]
        labels = ['<100', '100-1K', '1K-10K', '>10K']
        self.df['follower_category'] = pd.cut(
            self.df['author_followers'], 
            bins=bins, 
            labels=labels
        )
        
        follower_analysis = self.df.groupby('follower_category').agg({
            'has_moderation': ['sum', 'count', 'mean']
        }).round(4)
        
        follower_analysis.columns = ['Moderated', 'Total', 'Rate']
        follower_analysis['Rate'] = follower_analysis['Rate'] * 100
        
        print(follower_analysis.to_string())
        
        plt.figure(figsize=(10, 6))
        plt.bar(range(len(follower_analysis)), follower_analysis['Rate'])
        plt.xlabel('Follower Count Category')
        plt.ylabel('Moderation Rate (%)')
        plt.title('Moderation Rate by Account Size')
        plt.xticks(range(len(follower_analysis)), follower_analysis.index)
        plt.tight_layout()
        plt.savefig('moderation_by_followers.png', dpi=300, bbox_inches='tight')
        print("\n✓ Saved chart: moderation_by_followers.png")
        plt.close()
        
        return follower_analysis
    
    def analyze_moderation_labels(self):
        """Analyze types of moderation labels applied."""
        print("\n" + "=" * 70)
        print("MODERATION LABEL TYPES")
        print("=" * 70)
        
        all_labels = []
        for labels_list in self.df['moderation_labels_parsed']:
            for label in labels_list:
                if isinstance(label, dict) and 'value' in label:
                    all_labels.append(label['value'])
        
        if not all_labels:
            print("No moderation labels found in dataset.")
            return None
        
        label_counts = Counter(all_labels)
        
        print(f"Total labels applied: {len(all_labels)}")
        print(f"Unique label types: {len(label_counts)}\n")
        print("Label Distribution:")
        for label, count in label_counts.most_common():
            print(f"  {label}: {count}")
        
        if label_counts:
            plt.figure(figsize=(10, 6))
            labels, counts = zip(*label_counts.most_common(10))
            plt.barh(range(len(labels)), counts)
            plt.yticks(range(len(labels)), labels)
            plt.xlabel('Count')
            plt.title('Most Common Moderation Labels')
            plt.tight_layout()
            plt.savefig('moderation_label_types.png', dpi=300, bbox_inches='tight')
            print("\n✓ Saved chart: moderation_label_types.png")
            plt.close()
        
        return label_counts
    
    def analyze_engagement_moderation(self):
        """Compare engagement between moderated and non-moderated posts."""
        print("\n" + "=" * 70)
        print("ENGAGEMENT: MODERATED vs NON-MODERATED POSTS")
        print("=" * 70)
        
        moderated = self.df[self.df['has_moderation'] == True]
        non_moderated = self.df[self.df['has_moderation'] == False]
        
        if len(moderated) == 0:
            print("No moderated posts to compare.")
            return None
        
        metrics = ['likes_count', 'repost_count', 'reply_count']
        
        comparison = pd.DataFrame({
            'Moderated': [moderated[m].mean() for m in metrics],
            'Non-Moderated': [non_moderated[m].mean() for m in metrics]
        }, index=['Likes', 'Reposts', 'Replies'])
        
        print(comparison.round(2).to_string())
        
        comparison.plot(kind='bar', figsize=(10, 6))
        plt.title('Average Engagement: Moderated vs Non-Moderated Posts')
        plt.ylabel('Average Count')
        plt.xlabel('Engagement Type')
        plt.xticks(rotation=0)
        plt.legend(title='Post Type')
        plt.tight_layout()
        plt.savefig('engagement_comparison.png', dpi=300, bbox_inches='tight')
        print("\n✓ Saved chart: engagement_comparison.png")
        plt.close()
        
        return comparison
    
    def find_interesting_examples(self):
        """Find interesting examples of moderated content."""
        print("\n" + "=" * 70)
        print("SAMPLE MODERATED POSTS")
        print("=" * 70)
        
        moderated = self.df[self.df['has_moderation'] == True]
        
        if len(moderated) == 0:
            print("No moderated posts found.")
            return None
        
        print(f"\nShowing up to 5 examples:\n")
        
        for idx, row in moderated.head(5).iterrows():
            print(f"Post #{idx + 1}")
            print(f"  Author: {row['author_handle']} ({row['author_followers']} followers)")
            print(f"  Text: {row['post_text'][:100]}...")
            print(f"  Search term: {row['search_term']}")
            labels = row['moderation_labels_parsed']
            if labels:
                label_values = [l.get('value', 'unknown') for l in labels if isinstance(l, dict)]
                print(f"  Labels: {', '.join(label_values)}")
            print(f"  Engagement: {row['likes_count']} likes, {row['repost_count']} reposts")
            print()
        
    def generate_summary_report(self, output_file="analysis_summary.txt"):
        """Generate a text summary report."""
        print("\n" + "=" * 70)
        print("GENERATING SUMMARY REPORT")
        print("=" * 70)
        
        with open(output_file, 'w') as f:
            f.write("BLUESKY CONTENT MODERATION ANALYSIS\n")
            f.write("=" * 70 + "\n\n")
            
            stats = self.basic_statistics()
            f.write(f"Total posts: {stats['total_posts']}\n")
            f.write(f"Moderated posts: {stats['moderated_posts']}\n")
            f.write(f"Moderation rate: {stats['moderation_rate']:.2f}%\n\n")
            
            f.write("KEY FINDINGS:\n")
            f.write("-" * 70 + "\n")
            
            term_analysis = self.df.groupby('search_term')['has_moderation'].mean()
            if len(term_analysis) > 0:
                top_term = term_analysis.idxmax()
                top_rate = term_analysis.max() * 100
                f.write(f"1. Search term '{top_term}' had highest moderation rate ({top_rate:.2f}%)\n")
            
            if 'follower_category' in self.df.columns:
                follower_analysis = self.df.groupby('follower_category')['has_moderation'].mean()
                if len(follower_analysis) > 0:
                    f.write(f"2. Moderation rates by follower count:\n")
                    for cat, rate in follower_analysis.items():
                        f.write(f"   - {cat}: {rate*100:.2f}%\n")
            
            f.write(f"\nGenerated: {pd.Timestamp.now()}\n")
        
        print(f"✓ Saved summary report: {output_file}")


def main():
    """Main execution function."""
    print("=" * 70)
    print("BLUESKY MODERATION DATA ANALYSIS")
    print("=" * 70)
    print()
    
    analyzer = BlueskyDataAnalyzer('moderation_data.csv')
    
    analyzer.basic_statistics()
    analyzer.analyze_by_search_term()
    analyzer.analyze_by_follower_count()
    analyzer.analyze_moderation_labels()
    analyzer.analyze_engagement_moderation()
    analyzer.find_interesting_examples()
    analyzer.generate_summary_report()
    
    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE!")
    print("=" * 70)
    print("\nGenerated files:")
    print("  - moderation_by_term.png")
    print("  - moderation_by_followers.png")
    print("  - moderation_label_types.png")
    print("  - engagement_comparison.png")
    print("  - analysis_summary.txt")
    print("\nOpen these files to see your results!")


if __name__ == "__main__":
    main()