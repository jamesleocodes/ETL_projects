import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
import sqlite3
import json
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_path: str = 'config.json') -> dict:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Config file {config_path} not found. Using default configuration.")
        return {
            "database_path": "data/storm_data.db"
        }

def load_data_from_db(db_path: str) -> pd.DataFrame:
    """Load processed data from SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        query = """
        SELECT date, cape, shear, temperature, humidity, scs_index, storm_event, season
        FROM storm_events
        ORDER BY date
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert date string to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        logger.info(f"Loaded {len(df)} records from database")
        return df
    except Exception as e:
        logger.error(f"Error loading data from database: {str(e)}")
        raise

def compute_scs_index(df: pd.DataFrame) -> pd.DataFrame:
    """Compute SCS Index using the processed data."""
    try:
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # The SCS Index is already computed in the ETL process
        # We'll just verify the calculation
        scaler = StandardScaler()
        cape_norm = scaler.fit_transform(result_df[['cape']])
        shear_norm = scaler.fit_transform(result_df[['shear']])
        
        # Verify the SCS Index calculation
        expected_index = 0.6 * cape_norm + 0.4 * shear_norm
        result_df['verified_scs_index'] = expected_index
        
        # Check if our verification matches the stored index
        max_diff = np.max(np.abs(result_df['scs_index'] - result_df['verified_scs_index']))
        if max_diff > 1e-10:
            logger.warning(f"SCS Index verification shows differences up to {max_diff}")
        
        return result_df
    except Exception as e:
        logger.error(f"Error computing SCS Index: {str(e)}")
        raise

def perform_attribution_analysis(df: pd.DataFrame) -> tuple:
    """Perform attribution analysis using the processed data."""
    try:
        # Prepare features for attribution
        X = df[['temperature', 'humidity']]
        y = df['storm_event']
        
        # Fit logistic regression
        model = LogisticRegression(random_state=42)
        model.fit(X, y)
        
        # Get coefficients
        temp_coef = model.coef_[0][0]
        humidity_coef = model.coef_[0][1]
        
        logger.info(f"Attribution analysis completed. Temperature coefficient: {temp_coef:.3f}, Humidity coefficient: {humidity_coef:.3f}")
        return temp_coef, humidity_coef
    except Exception as e:
        logger.error(f"Error performing attribution analysis: {str(e)}")
        raise

def plot_results(df: pd.DataFrame, temp_coef: float, humidity_coef: float):
    """Create visualization of the analysis results."""
    try:
        plt.figure(figsize=(12, 6))
        
        # Plot SCS Index vs Storm Events
        sns.scatterplot(data=df, x='scs_index', y='storm_event', 
                       hue='storm_event', palette=['blue', 'red'], alpha=0.6)
        
        plt.title('SCS Index vs Storm Events\n' +
                 f'Temperature Coefficient: {temp_coef:.3f}, Humidity Coefficient: {humidity_coef:.3f}')
        plt.xlabel('SCS Index')
        plt.ylabel('Storm Event (0/1)')
        
        # Add legend
        plt.legend(title='Storm Event', labels=['No Storm', 'Storm'])
        
        # Save plot
        plt.savefig('scs_index_plot.png', dpi=300, bbox_inches='tight')
        logger.info("Plot saved as scs_index_plot.png")
        
        # Save data
        df.to_csv('scs_index_data.csv', index=False)
        logger.info("Data saved as scs_index_data.csv")
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        raise

def main():
    """Main function to run the analysis."""
    try:
        logger.info("Starting analysis...")
        
        # Load configuration
        config = load_config()
        
        # Load data from database
        df = load_data_from_db(config['database_path'])
        
        # Compute SCS Index
        df = compute_scs_index(df)
        
        # Perform attribution analysis
        temp_coef, humidity_coef = perform_attribution_analysis(df)
        
        # Create visualization
        plot_results(df, temp_coef, humidity_coef)
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()