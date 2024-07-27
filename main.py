"""
Vehicle Crash Data Pipeline for Mexico City

This pipeline processes and visualizes vehicle crash records from Mexico's International Data Institute.
It performs the following steps:
1. Downloads data from a specified URL
2. Processes and cleans the data
3. Validates the processed data
4. Performs advanced cleaning and processing
5. Exports the cleaned data to a parquet file
6. Creates visualizations of the crash data

The pipeline uses various custom modules for each step of the process.
"""

from data_loader import get_data
from process_and_validate import process_and_clean_data, validate_data
from cleaning_and_export import clean_and_process_data, export_dataframe
from filter_and_visualize import create_crash_map


def main():
    """
    Main function to execute the vehicle crash data pipeline.

    This function orchestrates the entire process from data download to visualization.
    It prints progress messages at each step for user feedback.
    """

    # Define constants
    URL = "https://i2ds.org/datos-abiertos/"
    BASE_PATH = "./accident_data"
    OUTPUT_FILE = "cleaned_crash_data.parquet"

    # Step 1: Fetch the data
    print("Starting data download...")
    get_data(URL, BASE_PATH)
    print("Data download complete.")

    # Step 2: Initial processing and validation
    print("Starting initial data processing...")
    df_final, all_dfs = process_and_clean_data(BASE_PATH)
    is_valid = validate_data(all_dfs, df_final)
    print(f"Initial data validation {'successful' if is_valid else 'failed'}.")
    print(f"Initial dataframe shape: {df_final.shape}")

    # Step 3: Advanced cleaning and processing
    print("Starting advanced data cleaning and processing...")
    headers = df_final.columns.tolist()
    cleaned_df = clean_and_process_data(df_final, headers)
    print("Advanced data cleaning and processing complete.")
    print(f"Cleaned dataframe shape: {cleaned_df.shape}")

    # Step 4: Export the cleaned data as a parquet file
    print(f"Exporting cleaned data to {OUTPUT_FILE}...")
    export_dataframe(cleaned_df, OUTPUT_FILE, format="parquet")
    print("Data export complete.")

    # Step 5: Visualize the crash data for Mexico City
    print("Creating crash maps...")
    # Create maps for different attributes
    map_attributes = [
        "nivel_dano_vehiculo",
        "total_lesionados",
        "genero_lesionado",
        "punto_impacto",
    ]
    for attribute in map_attributes:
        create_crash_map(cleaned_df, color_column=attribute, min_severity=1)
    print("Map creation complete.")


if __name__ == "__main__":
    main()
