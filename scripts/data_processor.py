import os
import pandas as pd
import re
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_processing.log')
    ]
)

class DataProcessor:
    def __init__(self, raw_data_path='raw_data', output_path='processed_data'):
        # Use absolute paths based on script location
        self.scripts_dir = Path(__file__).parent
        self.project_root = self.scripts_dir.parent
        
        # Use direct paths without relative navigation
        self.raw_data_path = self.project_root / raw_data_path
        self.output_path = self.project_root / output_path
        
        # Create output directory if it doesn't exist
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        
        # Common review file patterns (files in city folders)
        self.review_file_patterns = [
            '*hotel*', '*review*', '*comment*', '*feedback*'
        ]
        
        self.logger.info(f"Raw data path: {self.raw_data_path}")
        self.logger.info(f"Output path: {self.output_path}")
    
    def discover_hotels(self):
        """Discover all hotels from files in city folders"""
        hotels = []
        hotel_id = 1
        
        self.logger.info("Discovering hotels from city folders...")
        
        # Check if raw_data path exists
        if not self.raw_data_path.exists():
            self.logger.error(f"Raw data directory not found: {self.raw_data_path}")
            self.logger.info("Please create the raw_data folder with your hotel data")
            return hotels
        
        try:
            for city in os.listdir(self.raw_data_path):
                city_path = self.raw_data_path / city
                if city_path.is_dir():
                    self.logger.info(f"Processing city: {city}")
                    
                    # Find all files in this city folder (not subfolders)
                    city_files = []
                    for item in city_path.iterdir():
                        if item.is_file():
                            city_files.append(item)
                            self.logger.info(f"  Found file: {item.name}")
                    
                    if not city_files:
                        self.logger.warning(f"  No files found in {city}")
                        continue
                    
                    # Each file represents a hotel
                    for file_path in city_files:
                        hotel_name = self._extract_hotel_name(file_path.name, city)
                        country = self._infer_country(city)
                        
                        hotels.append({
                            'HOTELID': hotel_id,
                            'NAME': hotel_name,
                            'CITY': city.title(),
                            'COUNTRY': country,
                            'FILE_PATH': file_path
                        })
                        self.logger.info(f"  Created hotel: {hotel_name} (ID: {hotel_id}) from {file_path.name}")
                        hotel_id += 1
                        
        except Exception as e:
            self.logger.error(f"Error discovering hotels: {e}")
            raise
        
        return hotels
    
    def _extract_hotel_name(self, filename, city):
        """Convert filename to readable hotel name"""
        # Remove common file extensions
        name = re.sub(r'\.(txt|csv|json|xml)$', '', filename)
        
        # Replace underscores and hyphens with spaces
        name = name.replace('_', ' ').replace('-', ' ')
        
        # Remove city name if it appears in filename
        name = re.sub(city, '', name, flags=re.IGNORECASE)
        
        # Title case and clean up
        name = name.strip().title()
        
        # If name is empty after processing, use a generic name
        if not name:
            name = f"Hotel in {city.title()}"
        
        return name
    
    def _infer_country(self, city):
        """Simple country inference based on city name"""
        country_map = {
            'beijing': 'China',
            'shanghai': 'China',
            'chicago': 'USA',
            'las-vegas': 'USA',
            'london': 'UK',
            'montreal': 'Canada',
            'new-delhi': 'India',
            'new-york-city': 'USA',
            'san-francisco': 'USA',
            'shanghai': 'China'
        }
        return country_map.get(city.lower(), 'Unknown')
    
    def extract_reviews_from_hotel(self, hotel_info):
        """Extract reviews from a hotel file"""
        reviews = []
        review_id = 1
        
        file_path = hotel_info['FILE_PATH']
        hotel_id = hotel_info['HOTELID']
        
        self.logger.info(f"Extracting reviews from: {file_path.name}")
        
        try:
            file_reviews = self._parse_file(file_path, review_id, hotel_id)
            reviews.extend(file_reviews)
            self.logger.info(f"  Found {len(file_reviews)} reviews")
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
        
        return reviews
    
    def _parse_file(self, file_path, start_id, hotel_id):
        """Parse any file type and extract reviews"""
        reviews = []
        current_id = start_id
        
        try:
            # Try to detect file type by content
            file_type = self._detect_file_type(file_path)
            self.logger.info(f"    Detected file type: {file_type}")
            
            if file_type == 'text':
                reviews = self._parse_text_file(file_path, current_id, hotel_id)
            elif file_type == 'csv':
                reviews = self._parse_csv_file(file_path, current_id, hotel_id)
            else:
                # Fallback: treat as text
                reviews = self._parse_text_file(file_path, current_id, hotel_id)
                
        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {e}")
        
        return reviews
    
    def _detect_file_type(self, file_path):
        """Detect file type by content and extension hints"""
        filename = file_path.name.lower()
        
        # Check filename for hints
        if any(ext in filename for ext in ['.csv', 'csv']):
            return 'csv'
        elif any(ext in filename for ext in ['.json', 'json']):
            return 'json'
        
        # Check content
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
                
                # CSV detection: comma separated values
                if ',' in first_line and len(first_line.split(',')) > 2:
                    return 'csv'
                # JSON detection: starts with { or [
                elif first_line.startswith('{') or first_line.startswith('['):
                    return 'json'
                # Default to text
                else:
                    return 'text'
        except:
            return 'text'
    
    def _parse_text_file(self, file_path, start_id, hotel_id):
        """Parse text files (one review per line or block)"""
        reviews = []
        current_id = start_id
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Try different parsing strategies
            parsing_strategies = [
                self._parse_as_lines,
                self._parse_as_blocks,
            ]
            
            for strategy in parsing_strategies:
                parsed_reviews = strategy(content, current_id, hotel_id, file_path)
                if parsed_reviews and len(parsed_reviews) > 0:
                    reviews.extend(parsed_reviews)
                    break
            
            self.logger.info(f"    Extracted {len(reviews)} reviews")
            
        except Exception as e:
            self.logger.error(f"Error reading text file {file_path}: {e}")
        
        return reviews
    
    def _parse_as_lines(self, content, start_id, hotel_id, file_path):
        """Parse content as one review per line"""
        reviews = []
        current_id = start_id
        
        lines = content.split('\n')
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            # Only consider lines that look like actual reviews
            if self._is_valid_review(line):
                reviews.append({
                    'IDREVIEW': current_id,
                    'HOTELID': hotel_id,
                    'REVIEW': line,
                    'FILE_SOURCE': file_path.name,
                    'LINE_NUMBER': line_num
                })
                current_id += 1
        
        return reviews
    
    def _parse_as_blocks(self, content, start_id, hotel_id, file_path):
        """Parse content as reviews separated by blank lines"""
        reviews = []
        current_id = start_id
        
        # Split by multiple newlines
        blocks = re.split(r'\n\s*\n', content)
        for block_num, block in enumerate(blocks, 1):
            block = block.strip()
            if self._is_valid_review(block):
                reviews.append({
                    'IDREVIEW': current_id,
                    'HOTELID': hotel_id,
                    'REVIEW': block,
                    'FILE_SOURCE': file_path.name,
                    'LINE_NUMBER': block_num
                })
                current_id += 1
        
        return reviews
    
    def _parse_csv_file(self, file_path, start_id, hotel_id):
        """Parse CSV files"""
        reviews = []
        current_id = start_id
        
        try:
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                # If all encodings fail, use utf-8 with error handling
                df = pd.read_csv(file_path, encoding='utf-8', errors='ignore')
            
            # Try to identify review column automatically
            review_col = None
            possible_columns = ['review', 'text', 'comment', 'content', 'description', 'feedback']
            
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in possible_columns):
                    review_col = col
                    break
            
            # If no obvious review column, use the first string column
            if review_col is None:
                for col in df.columns:
                    if df[col].dtype == 'object':
                        review_col = col
                        break
            
            if review_col:
                for idx, row in df.iterrows():
                    review_text = str(row[review_col]) if pd.notna(row[review_col]) else ""
                    if self._is_valid_review(review_text):
                        reviews.append({
                            'IDREVIEW': current_id,
                            'HOTELID': hotel_id,
                            'REVIEW': review_text,
                            'FILE_SOURCE': file_path.name,
                            'LINE_NUMBER': idx + 1
                        })
                        current_id += 1
            else:
                self.logger.warning(f"No review column found in CSV file: {file_path}")
                        
        except Exception as e:
            self.logger.error(f"Error reading CSV file {file_path}: {e}")
        
        return reviews
    
    def _is_valid_review(self, text):
        """Check if text looks like a valid review"""
        if not text or not isinstance(text, str):
            return False
        
        text = text.strip()
        
        # Basic validation rules
        if len(text) < 10:  # Too short
            return False
        
        if text.upper() == text and len(text) > 20:  # ALL CAPS (likely not a review)
            return False
        
        if text.startswith('#') or text.startswith('//'):  # Comment line
            return False
        
        # Check if it contains typical review words
        review_keywords = ['hotel', 'room', 'stay', 'service', 'clean', 'dirty', 'staff', 'price', 'location']
        text_lower = text.lower()
        if any(keyword in text_lower for keyword in review_keywords):
            return True
        
        # If no keywords but reasonable length, accept it
        return len(text) >= 20
    
    def generate_hotels_csv(self):
        """Generate the hotels CSV file"""
        self.logger.info("\n=== Generating Hotels CSV ===")
        hotels = self.discover_hotels()
        
        if not hotels:
            self.logger.error("No hotels found!")
            self.logger.info(f"Please check that your raw data is in: {self.raw_data_path}")
            self.logger.info("Expected structure: raw_data/city_name/hotel_files")
            return None
        
        hotels_df = pd.DataFrame(hotels)
        # Remove the FILE_PATH column for the final CSV
        hotels_export = hotels_df[['HOTELID', 'NAME', 'CITY', 'COUNTRY']]
        
        output_file = self.output_path / 'hotels.csv'
        hotels_export.to_csv(output_file, index=False)
        self.logger.info(f"Generated hotels CSV with {len(hotels)} hotels: {output_file}")
        
        return hotels_df
    
    def generate_reviews_chunks(self, chunk_size=500):
        """Generate chunked review files for processing"""
        self.logger.info("\n=== Generating Review Chunks ===")
        all_reviews = []
        hotels = self.discover_hotels()
        
        if not hotels:
            self.logger.error("No hotels found to process!")
            return 0
        
        total_hotels = len(hotels)
        for i, hotel in enumerate(hotels, 1):
            self.logger.info(f"Processing hotel {i}/{total_hotels}: {hotel['NAME']}")
            hotel_reviews = self.extract_reviews_from_hotel(hotel)
            all_reviews.extend(hotel_reviews)
        
        if not all_reviews:
            self.logger.error("No reviews found in any hotel files!")
            return 0
        
        # Split into chunks
        total_chunks = (len(all_reviews) + chunk_size - 1) // chunk_size
        self.logger.info(f"Splitting {len(all_reviews)} reviews into {total_chunks} chunks...")
        
        for i in range(0, len(all_reviews), chunk_size):
            chunk = all_reviews[i:i + chunk_size]
            chunk_df = pd.DataFrame(chunk)
            chunk_file = self.output_path / f'reviews_chunk_{i//chunk_size + 1}.csv'
            
            # Only keep essential columns for processing
            chunk_export = chunk_df[['IDREVIEW', 'HOTELID', 'REVIEW']]
            chunk_export.to_csv(chunk_file, index=False)
            
            self.logger.info(f"Generated chunk {i//chunk_size + 1}: {len(chunk)} reviews -> {chunk_file}")
        
        self.logger.info(f"Total reviews processed: {len(all_reviews)}")
        return len(all_reviews)

def main():
    """Main function to run data processing"""
    processor = DataProcessor()
    
    print("Starting CIT444 Data Processing")
    print("=" * 60)
    print(f"Looking for data in: {processor.raw_data_path}")
    print(f"Output will be in: {processor.output_path}")
    print("=" * 60)
    print("NOTE: This version expects hotel files directly in city folders")
    print("Structure: raw_data/city_name/hotel_file")
    print("=" * 60)
    
    hotels_df = processor.generate_hotels_csv()
    
    if hotels_df is not None:
        total_reviews = processor.generate_reviews_chunks()
        print(f"\nProcessing complete! Found {len(hotels_df)} hotels and {total_reviews} reviews.")
        print(f"Check the 'processed_data' folder for output files.")
    else:
        print("\nProcessing failed - no hotels found.")
        print("\nYour current folder structure:")
        print("CIT444_Final_Project/")
        print("├── raw_data/")
        print("│   ├── beijing/")
        print("│   │   ├── china_hotel_file     <- This should be a file with reviews")
        print("│   │   └── other_hotel_file")
        print("│   ├── chicago/")
        print("│   │   └── chicago_hotel_file")
        print("│   └── ...")
        print("├── scripts/")
        print("├── processed_data/")
        print("└── ...")
        print("\nPlease make sure your hotel files contain review text (one review per line).")

if __name__ == "__main__":
    main()