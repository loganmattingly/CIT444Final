import os
import pandas as pd
import torch
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sentiment_analysis.log')
    ]
)

class EnhancedReviewAnalyzer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing sentiment analysis model...")
        
        try:
            self.sentiment_analyzer = pipeline(
                "sentiment-analysis",
                model="nlptown/bert-base-multilingual-uncased-sentiment",
                tokenizer="nlptown/bert-base-multilingual-uncased-sentiment",
                device=0 if torch.cuda.is_available() else -1
            )
            self.logger.info("Model loaded successfully")
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            self.sentiment_analyzer = None
        
        # Enhanced category keywords with weights
        self.categories = {
            'cleanliness': {
                'positive': ['clean', 'spotless', 'hygienic', 'tidy', 'immaculate', 'fresh', 'sanitized', 'well-maintained'],
                'negative': ['dirty', 'filthy', 'stained', 'dusty', 'messy', 'unclean', 'smelly', 'moldy', 'stain']
            },
            'price': {
                'positive': ['affordable', 'reasonable', 'worth', 'value', 'cheap', 'budget', 'inexpensive', 'good value'],
                'negative': ['expensive', 'overpriced', 'costly', 'pricey', 'waste', 'rip-off', 'overcharged', 'not worth']
            },
            'service': {
                'positive': ['helpful', 'friendly', 'professional', 'attentive', 'efficient', 'courteous', 'responsive', 'excellent service'],
                'negative': ['rude', 'slow', 'unprofessional', 'ignored', 'poor service', 'inattentive', 'unhelpful', 'bad service']
            },
            'location': {
                'positive': ['convenient', 'central', 'accessible', 'close', 'near', 'walkable', 'great location', 'perfect location'],
                'negative': ['remote', 'far', 'inconvenient', 'noisy', 'dangerous', 'isolated', 'bad location', 'far from']
            }
        }
    
    def analyze_sentiment_score(self, text):
        """Get sentiment score from 1-5 (matching the BERT model)"""
        if self.sentiment_analyzer is None:
            return 3  # Default neutral if model fails
        
        try:
            # Truncate very long texts to avoid token limits
            if len(text) > 1000:
                text = text[:1000]
            
            result = self.sentiment_analyzer(text)[0]
            label = result['label']
            
            # Convert labels like "4 stars" to numeric (1-5)
            if 'star' in label:
                return int(label.split()[0])
            else:
                # Fallback for other label formats
                sentiment_map = {
                    'very negative': 1, 
                    'negative': 2, 
                    'neutral': 3, 
                    'positive': 4, 
                    'very positive': 5
                }
                return sentiment_map.get(label.lower(), 3)
                
        except Exception as e:
            self.logger.error(f"Sentiment analysis error: {e}")
            return 3  # Default neutral
    
    def analyze_categories(self, text, base_sentiment):
        """Analyze specific categories with keyword matching"""
        if not text or not isinstance(text, str):
            return {category: base_sentiment for category in self.categories.keys()}
        
        text_lower = text.lower()
        category_scores = {}
        
        for category, keywords in self.categories.items():
            positive_matches = sum(1 for word in keywords['positive'] if word in text_lower)
            negative_matches = sum(1 for word in keywords['negative'] if word in text_lower)
            
            # Calculate adjusted score
            score = base_sentiment
            
            # Adjust based on keyword matches
            if positive_matches > 0 and negative_matches == 0:
                # Strong positive adjustment for multiple positive keywords
                score = min(5, score + min(positive_matches, 2))
            elif negative_matches > 0 and positive_matches == 0:
                # Strong negative adjustment for multiple negative keywords
                score = max(1, score - min(negative_matches, 2))
            elif positive_matches > negative_matches:
                # Slight positive adjustment
                score = min(5, score + 1)
            elif negative_matches > positive_matches:
                # Slight negative adjustment
                score = max(1, score - 1)
            
            category_scores[category] = int(round(score))
        
        return category_scores
    
    def process_review_batch(self, reviews_df, batch_size=8):
        """Process a batch of reviews with progress tracking"""
        if self.sentiment_analyzer is None:
            self.logger.info("Model not available, using default scores")
            return self._create_default_scores(reviews_df)
        
        results = []
        total_reviews = len(reviews_df)
        
        self.logger.info(f"Processing {total_reviews} reviews in batches of {batch_size}...")
        
        for i in tqdm(range(0, total_reviews, batch_size), desc="Analyzing reviews"):
            batch_end = min(i + batch_size, total_reviews)
            batch_df = reviews_df.iloc[i:batch_end]
            
            batch_results = []
            for _, review in batch_df.iterrows():
                try:
                    base_sentiment = self.analyze_sentiment_score(review['REVIEW'])
                    category_scores = self.analyze_categories(review['REVIEW'], base_sentiment)
                    
                    batch_results.append({
                        'REVIEWID': review['IDREVIEW'],
                        'HOTELID': review['HOTELID'],
                        'SERVICE': category_scores['service'],
                        'PRICE': category_scores['price'],
                        'ROOM': category_scores['cleanliness'],  # Using cleanliness for room quality
                        'LOCATION': category_scores['location'],
                        'OVERALL': base_sentiment
                    })
                except Exception as e:
                    self.logger.error(f"Error processing review {review['IDREVIEW']}: {e}")
                    # Add default scores for failed analyses
                    batch_results.append({
                        'REVIEWID': review['IDREVIEW'],
                        'HOTELID': review['HOTELID'],
                        'SERVICE': 3,
                        'PRICE': 3,
                        'ROOM': 3,
                        'LOCATION': 3,
                        'OVERALL': 3
                    })
            
            results.extend(batch_results)
            
            # Small delay to avoid overwhelming the system
            if i + batch_size < total_reviews:
                time.sleep(0.1)
        
        return pd.DataFrame(results)
    
    def _create_default_scores(self, reviews_df):
        """Create default scores when model is unavailable"""
        results = []
        for _, review in reviews_df.iterrows():
            results.append({
                'REVIEWID': review['IDREVIEW'],
                'HOTELID': review['HOTELID'],
                'SERVICE': 3,
                'PRICE': 3,
                'ROOM': 3,
                'LOCATION': 3,
                'OVERALL': 3
            })
        return pd.DataFrame(results)

def find_review_chunks(processed_data_path):
    """Find all review chunk files with proper pattern matching"""
    
    if not processed_data_path.exists():
        logging.error(f"Processed data directory not found: {processed_data_path}")
        return []
    
    # Look for files matching the pattern
    chunk_files = []
    for file_path in processed_data_path.iterdir():
        if file_path.is_file() and file_path.name.startswith('reviews_chunk_') and file_path.name.endswith('.csv'):
            chunk_files.append(file_path)
    
    # Sort by chunk number
    chunk_files.sort(key=lambda x: int(x.name.split('_')[2].split('.')[0]))
    
    return chunk_files

def process_all_chunks(processed_data_path, output_file='final_ratings.csv'):
    """Process all review chunks and combine results"""
    analyzer = EnhancedReviewAnalyzer()
    all_results = []
    
    # Find all review chunks
    chunk_files = find_review_chunks(processed_data_path)
    
    if not chunk_files:
        logging.error(f"No review chunks found in {processed_data_path}!")
        logging.info(f"Files in directory: {list(processed_data_path.iterdir()) if processed_data_path.exists() else 'Directory not found'}")
        return None
    
    logging.info(f"Found {len(chunk_files)} review chunks to process")
    
    total_reviews = 0
    for chunk_file in chunk_files:
        logging.info(f"\nProcessing {chunk_file.name}...")
        
        try:
            reviews_df = pd.read_csv(chunk_file)
            logging.info(f"  Loaded {len(reviews_df)} reviews from {chunk_file.name}")
            
            results_df = analyzer.process_review_batch(reviews_df)
            all_results.append(results_df)
            total_reviews += len(reviews_df)
            
        except Exception as e:
            logging.error(f"Error processing chunk {chunk_file}: {e}")
            continue
    
    # Combine all results
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        output_path = processed_data_path / output_file
        final_df.to_csv(output_path, index=False)
        
        # Print summary statistics
        logging.info(f"\n=== Analysis Complete ===")
        logging.info(f"Processed {total_reviews} total reviews")
        logging.info(f"Output file: {output_path}")
        logging.info(f"Average scores:")
        for column in ['SERVICE', 'PRICE', 'ROOM', 'LOCATION', 'OVERALL']:
            avg_score = final_df[column].mean()
            logging.info(f"  {column}: {avg_score:.2f}")
        
        return final_df
    else:
        logging.error("No results generated!")
        return None

def main():
    """Main function to run sentiment analysis"""
    print("Starting CIT444 Sentiment Analysis")
    print("=" * 60)
    
    # Set up paths - FIXED: Use absolute path directly
    scripts_dir = Path(__file__).parent
    processed_data_path = scripts_dir.parent / "processed_data"
    
    print(f"Looking for review chunks in: {processed_data_path}")
    print(f"Absolute path: {processed_data_path.absolute()}")
    
    if not processed_data_path.exists():
        print(f"Error: Processed data directory not found: {processed_data_path}")
        print("Please run data_processor.py first to generate review chunks.")
        return
    
    # Check what files exist
    files = list(processed_data_path.iterdir())
    csv_files = [f for f in files if f.name.endswith('.csv')]
    print(f"Found {len(csv_files)} CSV files in processed_data")
    
    # Look for review chunks specifically
    chunk_files = [f for f in files if f.name.startswith('reviews_chunk_') and f.name.endswith('.csv')]
    
    if not chunk_files:
        print("No review chunks found!")
        print("Expected files: reviews_chunk_1.csv, reviews_chunk_2.csv, etc.")
        print(f"Available files: {[f.name for f in csv_files[:10]]}...")  # Show first 10
        return
    
    print(f"Found {len(chunk_files)} review chunks")
    
    # Run the analysis with the correct absolute path
    results_df = process_all_chunks(processed_data_path)
    
    if results_df is not None:
        print(f"\nSuccessfully processed {len(results_df)} reviews!")
        print(f"Results saved to: {processed_data_path / 'final_ratings.csv'}")
    else:
        print("\nSentiment analysis failed!")

if __name__ == "__main__":
    main()