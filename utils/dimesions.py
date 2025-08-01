import pandas as pd

def get_next_incremental_id(df: pd.DataFrame, column_name: str):
    """Get next incremental ID for auto-increment columns"""
    if df.empty:
        return 1
    
    if column_name in df.columns:
        max_id = df[column_name].max()
        return max_id + 1 if pd.notna(max_id) else 1
    return None

def transform_dimension(source_value: str, dest_df: pd.DataFrame, 
                       value_column: str, id_column: str) -> pd.DataFrame:

    # Create a copy of destination table
    df = dest_df.copy()
    
    # Check if value exists
    if not df[value_column].str.contains(source_value).any():
        # Generate new ID (max ID + 1)
        new_id = df[id_column].max() + 1 if len(df) > 0 else 1
        # Add new record
        new_record = pd.DataFrame({
            id_column: [new_id],
            value_column: [source_value],
            'created_at': [pd.Timestamp.now()],
            'updated_at': [pd.Timestamp.now()]
        })
        df = pd.concat([df, new_record], ignore_index=True)
    
    return df

def transform_D_Date(mapping, source_df, ReportingYear) -> pd.DataFrame:

    if mapping['DateKey']['source_column'] is not None and mapping['DateKey']['source_column'] in source_df.columns:
        
        # Get the date column
        date_col = source_df[mapping['DateKey']['source_column']]
        
        # Calculate quarter start dates (first day of the quarter)
        quarter_start = date_col.dt.to_period('Q').dt.start_time
        
        # Calculate quarter end dates (last day of the quarter)
        quarter_end = date_col.dt.to_period('Q').dt.end_time
        
        date_df = pd.DataFrame({
            'DateKey': date_col.dt.strftime('%Y%m%d'),
            'StartDate': quarter_start.dt.strftime('%d-%m-%Y'),
            'EndDate': quarter_end.dt.strftime('%d-%m-%Y'),
            'Description': date_col.dt.year.astype(str) + ' Quarter ' + date_col.dt.quarter.astype(str) + ' Report',
            'Year': date_col.dt.year,
            'Quarter': date_col.dt.quarter,
            'Month': date_col.dt.month,
            'Day': date_col.dt.day,
            'created_at': pd.Timestamp.now(),
            'updated_at': pd.Timestamp.now()
        })
   
    else:
        
        date_df = pd.DataFrame({
            'DateKey': [f"{ReportingYear}0101"],
            'StartDate': [f"01-01-{ReportingYear}"],
            'EndDate': [f"31-12-{ReportingYear}"],
            'Description': [f"{ReportingYear} Annual Report"],
            'Year': [ReportingYear],
            'Quarter': [1],
            'Month': [1],
            'Day': [1],
            'created_at': [pd.Timestamp.now()],
            'updated_at': [pd.Timestamp.now()]
        })

    # Ensure DateKey is unique
    date_df = date_df.drop_duplicates(subset='DateKey').reset_index(drop=True)

    return date_df

def relate_country_company(country: str, company: str, company_df: pd.DataFrame, country_df: pd.DataFrame) -> pd.DataFrame:
    """
    Establish relationship between company and country
    """
    if not country_df[country_df['CountryName'] == country].empty:
        country_id = country_df[country_df['CountryName'] == country]['CountryID'].values[0]
        company_df.loc[company_df['CompanyName'] == company, 'CountryID'] = country_id
        company_df.loc[company_df['CompanyName'] == company, 'updated_at'] = pd.Timestamp.now()


    return company_df

def transform_D_Currency(mapping: pd.DataFrame, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:

    if mapping['CurrencyID']['source_column'] is not None and mapping['CurrencyID']['source_column'] in source_df.columns:
        # get unique currencies from source_df
        unique_currencies = source_df[mapping['CurrencyID']['source_column']].unique()
        # Create a copy of destination table
        df = dest_df.copy()
        # Check if each currency exists in the destination table
        for currency in unique_currencies:
            if not df['CurrencyCode'].str.contains(currency).any():
                # Generate new ID (max ID + 1)
                new_id = df['CurrencyID'].max() + 1 if len(df) > 0 else 1
                # Add new record
                new_record = pd.DataFrame({
                    'CurrencyID': [new_id],
                    'CurrencyName': [currency],
                    'created_at': [pd.Timestamp.now()],
                    'updated_at': [pd.Timestamp.now()]
                })
                df = pd.concat([df, new_record], ignore_index=True)
        return df

def transform_emission_source_provider(mapping: dict, source_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Transform emission source provider data"""

    result_df = df.copy()
    
    # Get the source column from mapping
    provider_mapping = next((v for k, v in mapping.items()
                           if k == 'ActivityEmissionSourceProviderID'), None)

    if not provider_mapping or 'source_column' not in provider_mapping:
        return result_df
        
    source_column = provider_mapping['source_column']
    
    if source_column not in source_df.columns:
        return result_df
        
    # Get unique providers from source
    providers = source_df[source_column].dropna().unique()

    for provider in providers:
        # Use regex=False to treat the provider name as a literal string
        if not df['ProviderName'].str.contains(provider, regex=False).any():
            new_row = {
                'ActivityEmissionSourceProviderID': get_next_incremental_id(result_df, 'ActivityEmissionSourceProviderID'),
                'ProviderName': provider
            }
            result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)
    
    return result_df

def transform_unit(mapping: pd.DataFrame, source_df: pd.DataFrame, dest_df: pd.DataFrame, calc_method) -> pd.DataFrame:
    if mapping['UnitID']['source_column'] is not None and mapping['UnitID']['source_column'] in source_df.columns and calc_method=='Consumption-based':
        # get unique units from source_df and handle null values
        unique_units = source_df[mapping['UnitID']['source_column']].dropna().unique()
        # Create a copy of destination table
        df = dest_df.copy()
        # Check if each unit exists in the destination table
        for unit in unique_units:
            # Convert to string and skip empty values
            if isinstance(unit, (list, tuple)):
                unit = str(unit[0])  # Take first element if it's a sequence
            elif unit is not None:
                unit = str(unit)  # Convert to string if it's not None
            else:
                unit = ""  # Default to empty string if None
                
            if not df['UnitName'].str.contains(unit).any():
                # Generate new ID (max ID + 1)
                new_id = df['UnitID'].max() + 1 if len(df) > 0 else 1
                # Add new record
                new_record = pd.DataFrame({
                    'UnitID': [new_id],
                    'UnitName': [unit],
                    'created_at': [pd.Timestamp.now()],
                    'updated_at': [pd.Timestamp.now()]
                })
                df = pd.concat([df, new_record], ignore_index=True)
    else :
        # If calc_method is not 'Consumption-based', return an DataFrame
        df = dest_df.copy()

    return df
