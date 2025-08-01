import pandas as pd
import json
from datetime import datetime, timedelta
import logging
from tqdm.auto import tqdm  # Changed to tqdm.auto for better notebook/UI support
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn
from rich.console import Console
from rich import print as rprint
import streamlit as st
from .progress_state import update_progress

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def find_emission_ids(mappings, activity_subcat, activity_subcat_df, activity_emission_source_df, country_df, country, calc_method):
    """Returns ActivityEmissionSourceID, UnitID, and EmissionFactorID based on mapping conditions."""
    
    # Get basic lookup values
    activity_sub_cat_id = lookup_value(activity_subcat_df, 'ActivitySubcategoryName', activity_subcat, 'ActivitySubcategoryID')
    iso2_code = lookup_value(country_df, 'CountryName', country, 'ISO2Code')
    
    # Define transformation suffixes based on calc_method
    valid_transformations = ['Distance', 'Fuel', 'Electricity', 'Heating', 'Days'] if calc_method == 'Consumption-based' else ['Currency']
    
    # Find the first matching transformation
    transformation = None
    amount_key = 'ConsumptionAmount' 
    for key, mapping_info in mappings.items():
        if amount_key in key:
            trans = mapping_info.get('consumption_type', '').lower()
            print(trans, '=', [x.lower() for x in valid_transformations])
            if trans in [x.lower() for x in valid_transformations]:
                transformation = trans
                break

    if not transformation:
        logging.warning(f"No valid transformation found for {activity_subcat}")
        return None, None, None
    
    # Get emission source ID by suffix
    emission_source_id = get_emission_source_id_by_suffix(
        activity_emission_source_df, activity_sub_cat_id, transformation
    )
    
    if not emission_source_id:
        logging.warning(f"No emission source ID found for {activity_subcat}")
        return None, None, None
    
    # Get unit ID and emission factor ID
    unit_id = lookup_value(activity_emission_source_df, 'ActivityEmissionSourceID', emission_source_id, 'UnitID')
    emission_source_name = lookup_value(activity_emission_source_df, 'ActivityEmissionSourceID', emission_source_id, 'ActivityEmissionSourceName')
    emission_factor_id = f"{iso2_code}_{emission_source_name}"
    
    return emission_source_id, unit_id, emission_factor_id


def get_emission_source_id_by_suffix(df, subcategory_id, suffix):
    """Get emission source ID for sources ending with suffix (case-insensitive) and matching subcategory ID."""
    filtered = df[
        (df['ActivitySubcategoryID'] == subcategory_id) &
        (df['ActivityEmissionSourceName'].str.lower().str.endswith(suffix.lower()))
    ]
    return filtered.iloc[0]['ActivityEmissionSourceID'] if not filtered.empty else None



def lookup_value(df: pd.DataFrame, lookup_column: str, lookup_value: str, return_column: str):
    """Generic lookup function to find values in dimension tables"""
    if df.empty or lookup_value is None:
        return None
    
    # Convert lookup column to string before comparing
    mask = df[lookup_column].astype(str).str.lower() == str(lookup_value).lower()
    result = df.loc[mask, return_column]
    
    if not result.empty:
        return result.iloc[0]
    return None



def get_date_key(date_df: pd.DataFrame, source_column: str, year: int, date_value=None):
    """
    Returns DateKey using the existing lookup_value function
    """
    import pandas as pd
    
    # if source column is None or empty, use reporting year to get DateKey
    if source_column is None or source_column == 'null' or date_value is None:
        # Get DateKey based on the reporting year
        return lookup_value(date_df, 'Year', year, 'DateKey')
    
    # Convert date to datetime
    source_date = pd.to_datetime(date_value, errors='coerce')
    
    if pd.isna(source_date):
        return lookup_value(date_df, 'Year', year, 'DateKey')  # fallback to year
        
    # Convert date to DateKey format (YYYYMMDD)
    date_key = source_date.strftime('%Y%m%d')
    
    # Use the existing lookup function to find the DateKey
    return lookup_value(date_df, 'DateKey', date_key, 'DateKey')
    
def get_next_incremental_id(df: pd.DataFrame, column_name: str):
    """Get next incremental ID for auto-increment columns"""
    if df.empty:
        return 1
    
    if column_name in df.columns:
        max_id = df[column_name].max()
        return max_id + 1 if pd.notna(max_id) else 1
    return None

def generate_fact(mappings: dict, source_df: pd.DataFrame, dest_df: pd.DataFrame,
                  activity_cat_df: pd.DataFrame, activity_subcat_df: pd.DataFrame,
                  scope_df: pd.DataFrame, activity_emission_source_df: pd.DataFrame,
                  activity_emmission_source_provider_df: pd.DataFrame, unit_df: pd.DataFrame,
                  currency_df: pd.DataFrame, date_df: pd.DataFrame, country_df: pd.DataFrame,
                  company_df: pd.DataFrame, company: str, country: str, activity_cat: str, 
                  activity_subcat: str, reporting_year: int, calc_method: str) -> pd.DataFrame:    

    # Record start time
    start_time = datetime.now()

    result_df = dest_df.copy()
    total_records = len(source_df)
    
    # Update progress state
    update_progress(
        table_name=f"{activity_cat} - {activity_subcat}",
        total=total_records,
        processed=0,
        status="Processing"
    )
    
    # Display progress information in Streamlit
    with st.expander("Processing Details", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Company:** " + str(company))
            st.markdown("**Country:** " + str(country))
            st.markdown("**Reporting Year:** " + str(reporting_year))
        with col2:
            st.markdown("**Category:** " + str(activity_cat))
            st.markdown("**Subcategory:** " + str(activity_subcat))
            st.markdown("**Calculation Method:** " + str(calc_method))

    progress_bar = st.progress(0)
    status_text = st.empty()

    # Get initial IDs
    emission_source_id, unit_id, emission_factor_id = find_emission_ids(
        mappings, activity_subcat, activity_subcat_df, activity_emission_source_df, 
        country_df, country, calc_method
    )

    for index, (_, source_row) in enumerate(source_df.iterrows()):
        new_row = {}
        
        # Map the fixed fact columns
        new_row['EmissionActivityID'] = get_next_incremental_id(result_df, 'EmissionActivityID')
        new_row['CompanyID'] = lookup_value(company_df, 'CompanyName', company, 'CompanyID')
        new_row['CountryID'] = lookup_value(country_df, 'CountryName', country, 'CountryID')
        new_row['ActivityCategoryID'] = lookup_value(activity_cat_df, 'ActivityCategory', activity_cat, 'ActivityCategoryID')
        new_row['ActivitySubcategoryID'] = lookup_value(activity_subcat_df, 'ActivitySubcategoryName', activity_subcat, 'ActivitySubcategoryID')
        new_row['ScopeID'] = lookup_value(activity_cat_df, 'ActivityCategory', activity_cat, 'ScopeID')
        new_row['ActivityEmissionSourceID'] = emission_source_id
        new_row['UnitID'] = unit_id
        new_row['EmissionFactorID'] = emission_factor_id

        new_row['DateKey'] = get_date_key(date_df, mappings.get('DateKey', {}).get('source_column'), reporting_year, source_row.get(mappings.get('DateKey', {}).get('source_column')))

        # Map direct values from source data
        for field_name, mapping_config in mappings.items():
            source_column = mapping_config.get("source_column")
            if field_name in ['ConsumptionAmount', 'PaidAmount'] and source_column in source_df.columns:
                new_row[field_name] = source_row[source_column]
            
            # Handle provider and currency if present in mappings
            if field_name == 'ActivityEmissionSourceProviderID' and source_column in source_df.columns:
                provider_name = source_row[source_column]
                new_row[field_name] = lookup_value(activity_emmission_source_provider_df, 
                                                 'ProviderName', provider_name, 'ActivityEmissionSourceProviderID')
            
            if field_name == 'CurrencyID' and source_column in source_df.columns:
                currency_code = source_row[source_column]
                new_row[field_name] = lookup_value(currency_df, 'CurrencyCode', currency_code, 'CurrencyID')
        
        # Update progress
        progress = (index + 1) / total_records
        progress_bar.progress(progress)
        status_text.text(f"Processing record {index + 1} of {total_records}")
        update_progress(processed=index + 1)
        
        # Add the new row to the result DataFrame
        result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)

    # Record end time and calculate duration
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Update final status with timing information
    status_text.text("Processing Complete!")
    update_progress(status="Complete")
    
    # Show summary with timing information
    st.success(f"Processed {len(result_df) - len(dest_df)} records")
    st.write(f"Total Records in Fact Table: {len(result_df)}")
    st.write(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    st.write(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    st.write(f"Duration: {str(duration).split('.')[0]}")  # Format duration without microseconds

    return result_df