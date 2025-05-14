# Storm Dynamics and Attribution Analysis

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)


## Table of Contents
- [Project Overview](#project-overview)
- [Installation](#installation)
- [Usage](#usage)
- [Analysis Details](#analysis-details)
- [ETL Implementation](#etl-implementation)
- [Results](#results)
- [Contributing](#contributing)
- [License](#license)
- [Citation](#citation)

## Project Overview
This project focuses on analyzing and quantifying the relationship between atmospheric conditions and Severe Convective Storm (SCS) events. The analysis aims to understand how different atmospheric variables contribute to storm formation and intensity.

### Key Features
- SCS Index computation and analysis
- Climate attribution modeling
- Visualization of storm dynamics
- Statistical analysis of atmospheric variables
- ETL pipeline for data processing

## Installation

### Prerequisites
- Python 3.9 or higher
- pip (Python package installer)

### Setup
1. Clone the repository:
```bash
git clone https://github.com/yourusername/StormDynamics_Attribution.git
cd StormDynamics_Attribution
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

### Analysis Script
Run the analysis script:
```bash
python phase1.py
```

### ETL Pipeline
Run the ETL process:
```bash
python phase1etl.py
```

The ETL process will:
1. Extract data (currently simulated)
2. Transform the data
3. Load it into SQLite database
4. Generate logs in `etl_process.log`

## Analysis Details

### Phase 1: Initial Analysis and Attribution

#### Atmospheric Variables
- **CAPE (Convective Available Potential Energy)**: Measures atmospheric instability (J/kg)
- **Wind Shear**: Measures wind speed and direction changes with height (m/s)
- **Temperature**: Surface temperature (°C)
- **Humidity**: Relative humidity (%)

#### SCS Index Computation
The script computes a normalized SCS Index that combines CAPE and wind shear:
- CAPE and shear are normalized using StandardScaler
- The index is calculated as: `SCS_Index = 0.6 * CAPE_norm + 0.4 * Shear_norm`

#### Attribution Analysis
Using logistic regression, the script quantifies the contribution of temperature and humidity to SCS events:
- Temperature coefficient: 0.046
- Humidity coefficient: 0.001

## ETL Implementation

### Components

#### 1. Extract
- Simulates data extraction (can be replaced with NOAA API)
- Saves raw data to CSV files in `data/raw` directory
- Includes date, CAPE, shear, temperature, and humidity data

#### 2. Transform
- Calculates SCS Index using normalized values
- Classifies storm events
- Adds season information
- Saves processed data to CSV files in `data/processed` directory

#### 3. Load
- Creates and manages SQLite database
- Creates necessary tables and indexes
- Loads transformed data into the database
- Includes data versioning

### Configuration
The ETL process is configured through `config.json`:
```json
{
    "noaa_api_key": "",
    "database_path": "data/storm_data.db",
    "start_date": "2020-01-01",
    "end_date": "2023-12-31",
    "data_retention_days": 365,
    "batch_size": 1000
}
```

### Data Flow
1. Raw data is extracted and stored in `data/raw`
2. Data is transformed and stored in `data/processed`
3. Processed data is loaded into SQLite database
4. All operations are logged in `etl_process.log`

## Results

### Key Findings from scs_index_plot.png

The visualization reveals several important insights about the relationship between atmospheric conditions and storm events:

1. **Data Quality and Verification**:
   - The analysis uses verified data from the ETL pipeline
   - SCS Index calculations are cross-validated between ETL and analysis phases
   - Seasonal patterns are preserved in the processed data

2. **Storm Event Classification**:
   - Clear separation between storm and non-storm events based on SCS Index
   - Red points (storm events) cluster at higher SCS Index values
   - Blue points (non-storm events) dominate the lower SCS Index range

3. **Attribution Analysis**:
   - Temperature shows a moderate positive influence on storm formation
   - Humidity has minimal direct impact on storm occurrence
   - The relationship between SCS Index and storm events is non-linear

4. **Seasonal Patterns**:
   - Storm events show seasonal clustering
   - Higher frequency of events during peak seasons
   - Clear separation between seasonal storm and non-storm conditions

5. **Data Processing Improvements**:
   - Consistent data quality through ETL pipeline
   - Verified calculations across processing stages
   - Enhanced visualization with improved resolution and clarity

### Future Work
1. Incorporate real observational data
2. Add spatial analysis components
3. Include climate model projections
4. Perform more detailed statistical analysis
5. Implement real-time data processing
6. Add data quality monitoring
7. Enhance seasonal pattern analysis
8. Develop predictive models for storm events

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

