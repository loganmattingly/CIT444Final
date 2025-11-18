import pandas as pd
from collections import Counter
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import os

# Download required NLTK data
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('wordnet', quiet=True)
except:
    print("NLTK downloads may require internet connection")

class WordFrequencyAnalyzer:
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words('english'))
        
        # Add custom stop words
        self.stop_words.update(['hotel', 'room', 'stay', 'stayed', 'would', 'could', 'also', 'us', 'we', 'i', 'the', 'a', 'an'])
    
    def process_review_text(self, review_text):
        """Process and tokenize review text"""
        if not isinstance(review_text, str):
            return []
        
        try:
            tokens = word_tokenize(review_text.lower())
            
            processed_tokens = [
                self.lemmatizer.lemmatize(word)
                for word in tokens 
                if word.isalnum() 
                and word not in self.stop_words
                and len(word) > 2  # Remove very short words
            ]
            
            return processed_tokens
        except:
            return []
    
    def analyze_reviews_file(self, reviews_file):
        """Analyze word frequencies in reviews file"""
        print(f"Analyzing word frequencies in: {reviews_file}")
        
        try:
            reviews_df = pd.read_csv(reviews_file)
            
            if 'REVIEW' not in reviews_df.columns:
                print("Error: 'REVIEW' column not found in the file")
                return None
            
            word_counter = Counter()
            total_reviews = len(reviews_df)
            
            print(f"Processing {total_reviews} reviews...")
            
            for review in reviews_df['REVIEW']:
                tokens = self.process_review_text(review)
                word_counter.update(tokens)
            
            return word_counter
            
        except Exception as e:
            print(f"Error processing file {reviews_file}: {e}")
            return None
    
    def analyze_all_chunks(self, input_dir='../processed_data', output_file='word_frequency_analysis.csv'):
        """Analyze all review chunks and combine results"""
        chunk_files = [f for f in os.listdir(input_dir) if f.startswith('reviews_chunk_') and f.endswith('.csv')]
        
        if not chunk_files:
            print("No review chunks found!")
            return None
        
        print(f"Found {len(chunk_files)} review chunks to analyze")
        
        combined_counter = Counter()
        total_words = 0
        
        for chunk_file in sorted(chunk_files):
            chunk_path = os.path.join(input_dir, chunk_file)
            chunk_counter = self.analyze_reviews_file(chunk_path)
            
            if chunk_counter:
                combined_counter.update(chunk_counter)
                total_words += sum(chunk_counter.values())
                print(f"  Processed {chunk_file}: {len(chunk_counter)} unique words")
        
        # Get top words
        top_words = combined_counter.most_common(100)
        
        # Create results DataFrame
        results_df = pd.DataFrame(top_words, columns=['Word', 'Frequency'])
        
        # Calculate percentage
        results_df['Percentage'] = (results_df['Frequency'] / total_words * 100).round(2)
        
        # Save results
        output_path = os.path.join(input_dir, output_file)
        results_df.to_csv(output_path, index=False)
        
        print(f"\n=== Word Frequency Analysis Complete ===")
        print(f"✓ Total unique words: {len(combined_counter)}")
        print(f"✓ Total word occurrences: {total_words}")
        print(f"✓ Top 10 most frequent words:")
        
        for word, freq, perc in results_df.head(10).itertuples(index=False):
            print(f"    {word}: {freq} ({perc}%)")
        
        print(f"✓ Results saved to: {output_path}")
        
        return results_df

def main():
    analyzer = WordFrequencyAnalyzer()
    analyzer.analyze_all_chunks()

if __name__ == "__main__":
    main()