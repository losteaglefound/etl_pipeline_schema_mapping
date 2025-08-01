import pandas as pd
import os
from utils.dimesions import transform_dimension, transform_D_Date, transform_D_Currency, \
    transform_emission_source_provider, transform_unit, relate_country_company
from utils.fact import generate_fact

from logger import setup_logger
logger = setup_logger("transformer")

def transform_data(source_df: pd.DataFrame, mapping: pd.DataFrame,
                      company: str, country: str, calc_method: str,
                      activity_cat: str, activity_subcat: str,
                      dest_schema: dict, dest_tables: dict,
                      ReportingYear: int):
        
        # Transform country and company dimensions
        country_df = transform_dimension(country, dest_tables['D_Country'], 'CountryName', 'CountryID')
        company_df = transform_dimension(company, dest_tables['D_Company'], 'CompanyName', 'CompanyID')

        # Establish relationship between company and country
        company_df = relate_country_company(country, company, company_df, country_df)

        # Tranform date dimension
        date_df = transform_D_Date(mapping, source_df, ReportingYear)
        
        # Transform currency dimension
        currency_df = transform_D_Currency(mapping,source_df,dest_tables['D_Currency'])

        # Tranform ActivityEmissionSourceProvider dimension
        activity_emmission_source_provider_df = transform_emission_source_provider(mapping,source_df,dest_tables['DE1_ActivityEmissionSourceProvi'])

        # Transform Unit dimension
        unit_df = transform_unit(mapping, source_df, dest_tables['DE1_Unit'], calc_method)

        # Fixed Destination tables
        activity_cat_df = dest_tables['DE1_ActivityCategory'].copy()
        activity_subcat_df = dest_tables['DE1_ActivitySubcategory'].copy()
        scope_df = dest_tables['DE1_Scopes'].copy()
        activity_emmission_source_df = dest_tables['DE1_ActivityEmissionSource'].copy()
        

        # fact table generation
        emmission_activity_data_df = generate_fact(mapping, source_df, dest_tables['FE1_EmissionActivityData'],
                                                  activity_cat_df, activity_subcat_df, scope_df,
                                                  activity_emmission_source_df, activity_emmission_source_provider_df,
                                                  unit_df, currency_df, date_df, country_df , company_df,
                                                  company, country, activity_cat, activity_subcat,
                                                  ReportingYear, calc_method)



        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.getcwd(), 'outputs')
        os.makedirs(output_dir, exist_ok=True)

        # Write each DataFrame to a separate sheet in an Excel file
        with pd.ExcelWriter(os.path.join(output_dir, 'transformed_data.xlsx')) as writer:
            company_df.to_excel(writer, sheet_name='D_Company', index=False)
            country_df.to_excel(writer, sheet_name='D_Country', index=False)
            activity_cat_df.to_excel(writer, sheet_name='DE1_ActivityCategory', index=False)
            activity_subcat_df.to_excel(writer, sheet_name='DE1_ActivitySubcategory', index=False)
            scope_df.to_excel(writer, sheet_name='DE1_Scopes', index=False)
            activity_emmission_source_df.to_excel(writer, sheet_name='DE1_ActivityEmissionSource', index=False)
            date_df.to_excel(writer, sheet_name='D_Date', index=False)
            unit_df.to_excel(writer, sheet_name='DE1_Unit', index=False)
            currency_df.to_excel(writer, sheet_name='D_Currency', index=False)
            activity_emmission_source_provider_df.to_excel(writer, sheet_name='DE1_ActivityEmissionSourceProvi', index=False)
            emmission_activity_data_df.to_excel(writer, sheet_name='FE1_EmissionActivityData', index=False)

        return {
            "D_Company": company_df,
            "D_Country": country_df,
            "DE1_ActivityCategory": activity_cat_df,
            "DE1_ActivitySubcategory": activity_subcat_df,
            "DE1_Scopes": scope_df,
            "DE1_ActivityEmissionSource": activity_emmission_source_df,
            "D_Date": date_df,
            "DE1_Unit": unit_df,
            "D_Currency": currency_df,
            "DE1_ActivityEmissionSourceProvi": activity_emmission_source_provider_df,
            "FE1_EmissionActivityData": emmission_activity_data_df
        }

        print(f"Transformed data written to {os.path.join(output_dir, 'transformed_data.xlsx')}")


 
