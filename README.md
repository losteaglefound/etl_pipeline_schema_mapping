# Schema Mapper PRO 🧩

AI-powered automated schema mapping, transformation, and validation tool for environmental and emissions data processing.

## Project Overview

This application automates the complex task of mapping source data into a standardized environmental data schema, with particular focus on emissions and activity data. It uses GPT-4 for intelligent schema mapping and includes comprehensive data validation.

## Core Features

- 🤖 AI-powered schema mapping using Azure OpenAI GPT-4
- 🔄 Support for both expenses-based and consumption-based calculations
- ✨ Automatic dimension table handling and ID management
- 📊 Comprehensive data validation and error reporting
- 🔍 Non-English text detection and handling
- 📝 Detailed logging and debug capabilities

## Project Structure

```
schema_mapper/
├── app.py                  # Streamlit web interface
├── config.py              # Configuration and API settings
├── logger.py              # Logging setup and management
├── data/                  # Reference data files
│   ├── DestinationSchema.xlsx
│   └── DestinationTables.xlsx
├── storage/               # ID management storage
│   └── ids/              # ID mapping files
├── outputs/              # Generated output files
│   ├── mappings.json    # AI-generated schema mappings
│   ├── mappings.csv     # Human-readable mappings
│   ├── *.csv           # Transformed data tables
│   └── UnresolvedData_Report.xlsx  # Validation issues
├── prompts/             # GPT prompt templates
│   └── schema_prompt.py
└── utils/              # Core functionality modules
    ├── file_loader.py  # Data loading utilities
    ├── gpt_mapper.py   # AI mapping integration
    ├── id_manager.py   # ID generation/tracking
    ├── transformer.py  # Data transformation logic
    └── validator.py    # Output validation

```

## Technical Details

### Data Flow
1. User uploads source Excel file via Streamlit interface
2. Source columns analyzed by GPT-4 for intelligent mapping
3. Mappings generated for fact and dimension tables
4. Data transformed according to mappings
5. Comprehensive validation performed
6. Output files generated and bundled

### Key Components

- **ID Management**: Tracks and maintains IDs across sessions
- **Validation Engine**: Checks for:
  - Schema compliance
  - Data type consistency
  - Non-English text
  - Duplicate records
  - Missing required fields
- **Transformation Logic**: Handles:
  - Dimension table relationships
  - Auto-incrementing keys
  - Date normalization
  - Unit conversion

## Setup and Usage

1. **Environment Setup**
```bash
pip install -r requirements.txt
```

2. **Configuration**
- Update `config.py` with Azure OpenAI credentials
- Place required reference data in `data/` directory

3. **Run Application**
```bash
streamlit run app.py
```

4. **Usage Steps**
   - Select calculation method (Expenses/Consumption)
   - Enter company and country
   - Choose activity category and subcategory
   - Upload source Excel file
   - Click "Run Mapping"
   - Download processed outputs

## Input Requirements

### Source Data
- Excel file (.xlsx)
- Contains activity/emissions data
- Supports multiple sheets
- Basic column headers required

### Reference Data
- `DestinationSchema.xlsx`: Schema definitions
- `DestinationTables.xlsx`: Lookup tables and mappings

## Outputs

1. **Mapping Files**
   - `mappings.json`: Complete mapping configuration
   - `mappings.csv`: Human-readable mapping table

2. **Transformed Data**
   - Dimension tables (D_*)
   - Fact tables (F_*)
   - Lookup tables (DE1_*)

3. **Validation Reports**
   - `UnresolvedData_Report.xlsx`: Data quality issues
   - Validation logs in debug mode

## Error Handling

- Comprehensive logging throughout pipeline
- Detailed error messages in UI
- Debug mode for detailed troubleshooting
- Validation report for data quality issues

## Dependencies

- streamlit: Web interface
- pandas: Data processing
- openai: GPT integration
- langdetect: Text validation
- openpyxl: Excel handling

## Contributing

1. Follow existing code structure
2. Maintain comprehensive logging
3. Update documentation as needed
4. Test thoroughly before submitting changes

## License

Proprietary software. All rights reserved.
