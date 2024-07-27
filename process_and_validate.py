"""
This module processes and cleans traffic accident data from CSV files.
It handles files with and without headers, normalizes column names,
and combines data from multiple years into a single DataFrame.
"""

import pandas as pd
import os
import numpy as np
from unidecode import unidecode
from datetime import datetime


def read_headers(base_dir):
    """
    Read headers from the data dictionary file.

    Args:
        base_dir (str): Base directory containing the data dictionary folder.

    Returns:
        list: List of header names.
    """
    # Find and read the dictionary file
    dict_folder = os.path.join(base_dir, "data-dictionary")
    dict_file = [
        f
        for f in os.listdir(dict_folder)
        if f.startswith("diccionario-percances-viales-axa")
    ][0]
    file_path = os.path.join(dict_folder, dict_file)

    # Extract headers from the Excel file
    headers_df = pd.read_excel(file_path, usecols=[0], skiprows=0)
    headers = headers_df.iloc[:, 0].tolist()

    # Insert 'Día Numero' after 'Mes Reporte'
    headers.insert(headers.index("Mes Reporte") + 1, "Día Numero")
    return headers


def process_files_without_headers(base_dir, years, headers):
    """
    Process CSV files without headers for specified years.

    Args:
        base_dir (str): Base directory containing year folders.
        years (range): Range of years to process.
        headers (list): List of header names to use.

    Returns:
        pandas.DataFrame: Concatenated DataFrame of all processed files.
    """
    dfs = []
    for year in years:
        dir_path = os.path.join(base_dir, str(year))
        if os.path.exists(dir_path):
            csv_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
            for csv_file in csv_files:
                file_path = os.path.join(dir_path, csv_file)
                df = pd.read_csv(
                    file_path,
                    encoding="ISO-8859-1",
                    names=headers,
                    header=None,
                    index_col=False,
                )
                dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def process_files_with_headers(base_dir, years):
    """
    Process CSV files with headers for specified years.

    Args:
        base_dir (str): Base directory containing year folders.
        years (range): Range of years to process.

    Returns:
        list: List of DataFrames for each processed file.
    """
    dfs = []
    for year in years:
        dir_path = os.path.join(base_dir, str(year))
        if os.path.exists(dir_path):
            csv_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
            for csv_file in csv_files:
                file_path = os.path.join(dir_path, csv_file)
                df = pd.read_csv(file_path, encoding="ISO-8859-1")
                dfs.append(df)
    return dfs


def clean_column_names(df):
    df.columns = [unidecode(col.lower().replace(" ", "_")) for col in df.columns]
    df.columns = df.columns.str.replace("_reporte", "")
    column_renames = {
        "daa_numero": "dia_numero",
        "aao": "ano",
        "nivel_daao_vehiculo": "nivel_dano_vehiculo",
        "causa_siniestro": "tipo_de_percance",
        "punto_de_impacto": "punto_impacto",
        "ciudad": "ciudad_municipio",
        "lesionados": "total_lesionados",
        "relacion_lesionados": "rol_lesionado",
        "nivel_lesionados": "nivel_lesion",
        "obra_civil": "dano_obra_civil",
        "fuga": "tercero_fuga",
        "seguro": "aseguradora",
        "taxi": "servicio_taxi",
    }
    df.rename(columns=column_renames, inplace=True)
    return df


def add_missing_columns(df):
    if "rol_lesionado" not in df.columns:
        df = df.assign(rol_lesionado=np.nan)
    if "nivel_lesion" not in df.columns:
        df = df.assign(nivel_lesion=np.nan)
    return df


def get_common_columns(dfs):
    return list(set.intersection(*[set(df.columns) for df in dfs]))


def process_and_clean_data(base_dir):
    headers = read_headers(base_dir)

    current_year = datetime.now().year
    df_without_headers = process_files_without_headers(
        base_dir, range(2020, current_year + 1), headers
    )
    dfs_with_headers = process_files_with_headers(base_dir, range(2015, 2020))

    all_dfs = (
        [df_without_headers] + dfs_with_headers
        if not df_without_headers.empty
        else dfs_with_headers
    )

    all_dfs = [clean_column_names(df) for df in all_dfs]
    all_dfs = [add_missing_columns(df) for df in all_dfs]

    common_columns = get_common_columns(all_dfs)

    df_final = pd.concat([df[common_columns] for df in all_dfs], ignore_index=True)

    return df_final, all_dfs


def validate_data(all_dfs, df_final):
    total_rows = sum(df.shape[0] for df in all_dfs)
    if total_rows == df_final.shape[0]:
        print("No rows were dropped during cleaning")
    else:
        print(
            f"Rows were dropped during cleaning. Original: {total_rows}, Final: {df_final.shape[0]}"
        )
    return total_rows == df_final.shape[0]
