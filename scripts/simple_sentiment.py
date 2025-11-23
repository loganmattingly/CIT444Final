"""
Simple sentiment analyzer that processes chunks in small batches
"""
import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def simple_sentiment_analysis():
    """Simple sentiment analysis with manual scoring"""
    scripts_dir = Path(__file__).parent
    processed_data_path = scripts_dir.parent / "processed_data"
    
    print("Simple Sentiment Analysis")
    print("=" * 50)
    print(f"Processing data from: {processed_data_path}")
    
    if not processed_data_path.exists():
        print("❌ processed_data folder not found!")
        return
    
    # Find all chunk files
    chunk_files = list(processed_data_path.glob('reviews_chunk_*.csv'))
    chunk_files.sort(key=lambda x: int(x.name.split('_')[2].split('.')[0]))
    
    print(f"Found {len(chunk_files)} chunk files")
    
    all_results = []
    total_reviews = 0
    
    # Process each chunk
    for i, chunk_file in enumerate(chunk_files, 1):
        print(f"Processing {chunk_file.name} ({i}/{len(chunk_files)})...")
        
        try:
            # Read the chunk
            df = pd.read_csv(chunk_file)
            print(f"  Loaded {len(df)} reviews")
            
            # Simple sentiment analysis based on keywords
            chunk_results = []
            for _, review in df.iterrows():
                sentiment_score = analyze_sentiment_simple(review['REVIEW'])
                
                chunk_results.append({
                    'REVIEWID': review['IDREVIEW'],
                    'HOTELID': review['HOTELID'],
                    'SERVICE': sentiment_score,
                    'PRICE': sentiment_score,
                    'ROOM': sentiment_score,
                    'LOCATION': sentiment_score,
                    'OVERALL': sentiment_score
                })
            
            all_results.extend(chunk_results)
            total_reviews += len(df)
            print(f"  Processed {len(df)} reviews")
            
        except Exception as e:
            print(f"  Error processing {chunk_file}: {e}")
    
    # Save results
    if all_results:
        results_df = pd.DataFrame(all_results)
        output_file = processed_data_path / 'final_ratings_simple.csv'
        results_df.to_csv(output_file, index=False)
        
        print(f"\n✅ Successfully processed {total_reviews} reviews!")
        print(f"📁 Results saved to: {output_file}")
        
        # Show summary
        print(f"\n📊 Summary:")
        print(f"Total reviews: {total_reviews}")
        print(f"Average overall score: {results_df['OVERALL'].mean():.2f}")
    else:
        print("❌ No results generated!")

def analyze_sentiment_simple(text):
    """Simple sentiment analysis using keyword matching"""
    if not isinstance(text, str):
        return 3
    
    text_lower = text.lower()
    
    # Positive keywords
    positive_words = ['excellent', 'amazing', 'great', 'good', 'wonderful', 'fantastic', 'clean', 'friendly', 'helpful', 'comfortable', 'beautiful', 'perfect', 'love', 'best']
    # Negative keywords  
    negative_words = ['terrible', 'awful', 'horrible', 'bad', 'poor', 'dirty', 'unclean', 'rude', 'unfriendly', 'uncomfortable', 'broken', 'worst', 'hate', 'disappointing']
    
    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)
    
    # Calculate score (1-5 scale)
    if positive_count > negative_count:
        return min(5, 3 + positive_count - negative_count)
    elif negative_count > positive_count:
        return max(1, 3 - (negative_count - positive_count))
    else:
        return 3

if __name__ == "__main__":
    simple_sentiment_analysis()