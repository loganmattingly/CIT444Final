"""
Diagnostic script to check review chunks and help debug issues
"""
from pathlib import Path
import pandas as pd

def check_processed_data():
    """Check what's in the processed_data directory"""
    scripts_dir = Path(__file__).parent
    processed_data_path = scripts_dir.parent / "processed_data"
    
    print("🔍 Checking processed_data directory...")
    print(f"Path: {processed_data_path}")
    print(f"Exists: {processed_data_path.exists()}")
    
    if not processed_data_path.exists():
        print("❌ processed_data directory not found!")
        return
    
    # List all files
    files = list(processed_data_path.iterdir())
    print(f"\n📁 Files in processed_data ({len(files)} total):")
    for file in sorted(files):
        print(f"  - {file.name} ({file.stat().st_size} bytes)")
    
    # Check for review chunks
    chunk_files = [f for f in files if f.name.startswith('reviews_chunk_') and f.name.endswith('.csv')]
    print(f"\n📊 Review chunks found: {len(chunk_files)}")
    
    for chunk_file in sorted(chunk_files)[:5]:  # Show first 5
        try:
            df = pd.read_csv(chunk_file)
            print(f"  {chunk_file.name}: {len(df)} reviews, columns: {list(df.columns)}")
            if len(df) > 0:
                print(f"    Sample review: {df['REVIEW'].iloc[0][:100]}...")
        except Exception as e:
            print(f"  {chunk_file.name}: ERROR - {e}")
    
    # Check hotels.csv
    hotels_file = processed_data_path / "hotels.csv"
    if hotels_file.exists():
        try:
            hotels_df = pd.read_csv(hotels_file)
            print(f"\n🏨 Hotels file: {len(hotels_df)} hotels")
            print(f"  Columns: {list(hotels_df.columns)}")
            print(f"  Sample hotels:")
            for _, hotel in hotels_df.head(3).iterrows():
                print(f"    - {hotel['NAME']} (ID: {hotel['HOTELID']}, {hotel['CITY']})")
        except Exception as e:
            print(f"❌ Error reading hotels.csv: {e}")

def check_chunk_patterns():
    """Check different patterns for finding chunks"""
    scripts_dir = Path(__file__).parent
    processed_data_path = scripts_dir.parent / "processed_data"
    
    if not processed_data_path.exists():
        return
    
    print(f"\n🎯 Testing chunk file patterns:")
    
    patterns = [
        'reviews_chunk_*.csv',
        'reviews_chunk_*',
        '*chunk*.csv',
        '*.csv'
    ]
    
    import glob
    for pattern in patterns:
        files = list(processed_data_path.glob(pattern))
        print(f"  Pattern '{pattern}': {len(files)} files")
        if files:
            for f in sorted(files)[:3]:
                print(f"    - {f.name}")

if __name__ == "__main__":
    print("CIT444 Chunk Diagnostic Tool")
    print("=" * 50)
    check_processed_data()
    check_chunk_patterns()