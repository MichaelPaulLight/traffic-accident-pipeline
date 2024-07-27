"""
This script processes and cleans traffic accident data, converting it from a pandas DataFrame
to a Polars DataFrame. It includes functions for data cleaning, type conversion, translation,
and exporting the final dataset.
"""

import pandas as pd
import numpy as np
import polars as pl
import pyarrow
from unidecode import unidecode
from typing import Union, Optional, Dict, List, Any
from pathlib import Path


def clean_headers(headers):
    return [
        unidecode(col.lower().strip().replace(" ", "_").replace("_reporte", ""))
        for col in headers
    ]


def convert_numeric_columns(df, cols_to_convert):
    total_coerced = 0
    for col in cols_to_convert:
        try:
            coerce_mask = (
                pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
            )
            total_coerced += coerce_mask.sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
        except Exception as e:
            print(f"Error converting column {col}: {e}")
    print("Total number of values coerced to NaN:", total_coerced)
    return df


def replace_special_values(df, special_value="\\N"):
    count_before = (df == special_value).sum().sum()
    df = df.replace(special_value, np.nan)
    count_after = df.isnull().sum().sum()
    print(
        f"Total number of '{special_value}' values replaced:",
        count_after - count_before,
    )
    return df


def convert_columns_to_string(df, cols_to_convert):
    for col in cols_to_convert:
        df[col] = df[col].astype("string", errors="ignore")
    return df


def convert_remaining_to_string(df):
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = df[col].astype("string")
            except ValueError:
                pass
    return df


def translate_months(pl_df):
    """
    This function takes a Polars DataFrame, converts a "mes" (month) column from Spanish month names to English,
    and then to numeric values (1-12), returning the modified DataFrame with the updated "mes" column.
    """
    months_spanish = [
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    ]
    months_english = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    numbers = [str(i) for i in range(1, 13)]

    # First, replace Spanish months with English
    pl_df = pl_df.with_columns(
        pl.col("mes")
        .str.to_lowercase()
        .str.replace_many(months_spanish, months_english)
    )

    # Then, replace English months with numbers
    pl_df = pl_df.with_columns(pl.col("mes").str.replace_many(months_english, numbers))

    # Finally, attempt to cast to Int64, replacing failures with null
    pl_df = pl_df.with_columns(pl.col("mes").cast(pl.Int64, strict=False))

    # Print out any values that couldn't be converted
    invalid_months = pl_df.filter(pl.col("mes").is_null()).select("mes").unique()
    if invalid_months.height > 0:
        print("Values in 'mes' that couldn't be converted to integers:")
        print(invalid_months)

    return pl_df


def process_damage_levels(pl_df):
    """
    This function function transforms the "nivel_dano_vehiculo" (vehicle damage level) column
    in a Polars DataFrame by translating Spanish terms to English,
    then converts these to numerical values (1-4), and finally casting the result to 64-bit integers,
    returning the DataFrame with the updated column.

    """
    return pl_df.with_columns(
        pl.col("nivel_dano_vehiculo")
        .str.replace_many(
            ["Bajo", "Alto", "Medio", "Sin "], ["Low", "High", "Medium", "No damage"]
        )
        .str.replace("No damage.*", "No damage")
        .str.replace_many(["Low", "High", "Medium", "No damage"], ["2", "4", "3", "1"])
        .str.replace("1.*", "1")
        .cast(pl.Int64, strict=False)
    )


def filter_data(
    df: pl.DataFrame, state_of_interest: List[str] = ["ciudad", "Ciudad", "CIUDAD"]
) -> pl.DataFrame:
    """
    Returns a dataframe that only includes crashes in the specified state(s) of interest.
    By default, it filters for Mexico City.

    Args:
    df (pl.DataFrame): Input Polars DataFrame
    state_of_interest (List[str]): List of state name variations to filter by

    Returns:
    pl.DataFrame: Filtered DataFrame
    """
    return df.filter(pl.col("estado").str.contains_any(state_of_interest))


def clean_and_process_data(df_final, headers):
    """
    Clean and process the data, including data type conversions and translations.

    Args:
        df_final (pd.DataFrame): The input DataFrame to process.
        headers (list): List of column headers.

    Returns:
        pl.DataFrame: The processed Polars DataFrame.
    """
    # Clean headers
    headers = clean_headers(headers)
    df_final = df_final[headers]

    # Convert numeric columns
    cols_to_convert = pd.concat(
        [
            df_final.loc[:, "siniestro":"codigo_postal"],
            df_final[
                [
                    "modelo",
                    "ano",
                    "dia_numero",
                    "hora",
                    "total_lesionados",
                    "edad_lesionado",
                ]
            ],
            df_final.loc[:, "ambulancia":"animal"],
        ],
        axis=1,
    ).columns
    df_final = convert_numeric_columns(df_final, cols_to_convert)

    # Replace "\N" with NaN
    df_final = replace_special_values(df_final)

    # Convert specific columns to string
    cols_to_convert_to_str = [
        "calle",
        "color",
        "nivel_dano_vehiculo",
        "punto_impacto",
        "mes",
        "dia",
        "estado",
        "ciudad_municipio",
        "rol_lesionado",
        "genero_lesionado",
        "hospitalizado",
        "fallecido",
    ]
    df_final = convert_columns_to_string(df_final, cols_to_convert_to_str)

    # Convert remaining object columns to string
    df_final = convert_remaining_to_string(df_final)

    # Convert to Polars DataFrame
    pl_df = pl.from_pandas(df_final)

    # Translate and process months
    pl_df = translate_months(pl_df)

    # Process damage levels
    pl_df = process_damage_levels(pl_df)

    # Filter data for only Mexico City
    pl_df = filter_data(pl_df)

    return pl_df


def export_dataframe(
    df: pl.DataFrame,
    file_path: Union[str, Path],
    format: str = "csv",
    options: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Exports a Polars DataFrame to a file in a few different formats.

    Args:
        df (pl.DataFrame): The Polars DataFrame to export.
        file_path (str or Path): The path where the file will be saved.
        format (str): The export format. Options: 'csv', 'parquet', 'json'. Default is 'csv'.
        options (dict, optional): Additional options for the export function.

    Returns:
        None

    Raises:
        ValueError: If an unsupported format is specified.
    """
    file_path = Path(file_path)

    # Ensure the directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Default options for each format
    default_options = {
        "csv": {"separator": ",", "include_header": True},
        "parquet": {"compression": "snappy"},
        "json": {"pretty": True},
    }

    # Combine default options with user-provided options
    export_options = default_options.get(format, {})
    if options:
        export_options.update(options)

    try:
        if format == "csv":
            df.write_csv(file_path, **export_options)
        elif format == "parquet":
            df.write_parquet(file_path, **export_options)
        elif format == "json":
            df.write_json(file_path, **export_options)
        else:
            raise ValueError(f"Unsupported format: {format}")

        print(f"DataFrame successfully exported to {file_path}")
    except Exception as e:
        print(f"Error exporting DataFrame: {e}")
