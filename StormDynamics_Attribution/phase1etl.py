import pandas as pd
import numpy as np
import requests
import sqlite3
from datetime import datetime, timedelta
import logging
from pathlib import Path
import json
from typing import Dict, List, Tuple
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StormDataETL:
    def __init__(self, config_path: str = 'config.json'):
        """Initialize ETL process with configuration."""
        self.config = self._load_config(config_path)
        self.raw_data_dir = Path('data/raw')
        self.processed_data_dir = Path('data/processed')
        self._setup_directories()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found. Using default configuration.")
            return {
                "noaa_api_key": os.getenv("NOAA_API_KEY", ""),
                "database_path": "data/storm_data.db",
                "start_date": "2020-01-01",
                "end_date": "2023-12-31"
            }

    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)

    def extract(self) -> pd.DataFrame:
        """Extract storm data from NOAA API."""
        logger.info("Starting data extraction...")
        
        try:
            # Simulate API call with sample data (replace with actual NOAA API call)
            dates = pd.date_range(start=self.config['start_date'], 
                                end=self.config['end_date'], 
                                freq='D')
            
            # Generate sample data
            data = {
                'date': dates,
                'cape': np.random.normal(1500, 500, len(dates)),
                'shear': np.random.normal(20, 5, len(dates)),
                'temperature': np.random.normal(25, 2, len(dates)),
                'humidity': np.random.normal(70, 10, len(dates))
            }
            
            df = pd.DataFrame(data)
            
            # Save raw data
            raw_file = self.raw_data_dir / f'raw_storm_data_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(raw_file, index=False)
            logger.info(f"Raw data saved to {raw_file}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error during data extraction: {str(e)}")
            raise

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the extracted data."""
        logger.info("Starting data transformation...")
        
        try:
            # Create a copy to avoid modifying the original
            transformed_df = df.copy()
            
            # Add derived columns
            transformed_df['scs_index'] = (
                0.6 * (transformed_df['cape'] - transformed_df['cape'].mean()) / transformed_df['cape'].std() +
                0.4 * (transformed_df['shear'] - transformed_df['shear'].mean()) / transformed_df['shear'].std()
            )
            
            # Add storm event classification
            transformed_df['storm_event'] = (
                (transformed_df['scs_index'] > 1.0) & 
                (transformed_df['temperature'] > 20)
            ).astype(int)
            
            # Add season column
            transformed_df['season'] = transformed_df['date'].dt.month.map({
                12: 'Winter', 1: 'Winter', 2: 'Winter',
                3: 'Spring', 4: 'Spring', 5: 'Spring',
                6: 'Summer', 7: 'Summer', 8: 'Summer',
                9: 'Fall', 10: 'Fall', 11: 'Fall'
            })
            
            # Save transformed data
            processed_file = self.processed_data_dir / f'processed_storm_data_{datetime.now().strftime("%Y%m%d")}.csv'
            transformed_df.to_csv(processed_file, index=False)
            logger.info(f"Transformed data saved to {processed_file}")
            
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise

    def load(self, df: pd.DataFrame):
        """Load the transformed data into SQLite database."""
        logger.info("Starting data loading...")
        
        try:
            # Create database connection
            conn = sqlite3.connect(self.config['database_path'])
            
            # Create tables if they don't exist
            conn.execute('''
                CREATE TABLE IF NOT EXISTS storm_events (
                    date TEXT,
                    cape REAL,
                    shear REAL,
                    temperature REAL,
                    humidity REAL,
                    scs_index REAL,
                    storm_event INTEGER,
                    season TEXT,
                    processed_date TEXT
                )
            ''')
            
            # Add processed date
            df['processed_date'] = datetime.now().strftime('%Y-%m-%d')
            
            # Load data into database
            df.to_sql('storm_events', conn, if_exists='append', index=False)
            
            # Create indexes for common queries
            conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON storm_events(date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_storm_event ON storm_events(storm_event)')
            
            conn.close()
            logger.info("Data successfully loaded into database")
            
        except Exception as e:
            logger.error(f"Error during data loading: {str(e)}")
            raise

    def run_etl(self):
        """Run the complete ETL process."""
        try:
            logger.info("Starting ETL process...")
            
            # Extract
            raw_data = self.extract()
            logger.info(f"Extracted {len(raw_data)} records")
            
            # Transform
            transformed_data = self.transform(raw_data)
            logger.info(f"Transformed {len(transformed_data)} records")
            
            # Load
            self.load(transformed_data)
            logger.info("ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise

def main():
    """Main function to run the ETL process."""
    try:
        etl = StormDataETL()
        etl.run_etl()
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 