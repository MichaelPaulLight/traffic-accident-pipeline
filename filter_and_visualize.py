import polars as pl
import pandas as pd
import plotly.express as px
from typing import Union, Literal


def filter_crash_severity(
    df: pl.DataFrame, min_severity: Union[int, float] = 0
) -> pl.DataFrame:
    """
    Filters the DataFrame to include only crashes at or above a specified severity level.

    Args:
        df (pl.DataFrame): Input Polars DataFrame containing crash data.
        min_severity (Union[int, float], optional): Minimum crash severity level to include.
                                                    Crashes with severity less than this value
                                                    will be filtered out. Defaults to 0.

    Returns:
        pl.DataFrame: Filtered DataFrame containing only crashes at or above the specified severity level.

    Raises:
        ValueError: If min_severity is less than 0 or greater than 4.
    """
    if min_severity < 0 or min_severity > 4:
        raise ValueError("min_severity must be between 0 and 4 inclusive")

    return df.filter(
        (pl.col("nivel_dano_vehiculo").is_not_null())
        & (pl.col("nivel_dano_vehiculo").is_not_nan())
        & (pl.col("nivel_dano_vehiculo") >= min_severity)
    )


def create_crash_map(
    df: pl.DataFrame,
    color_column: Literal[
        "nivel_dano_vehiculo", "total_lesionados", "genero_lesionado", "punto_impacto"
    ] = "nivel_dano_vehiculo",
    min_severity: Union[int, float] = 0,
) -> None:
    """
    Creates an interactive map of crashes in Mexico City.

    Args:
        df (pl.DataFrame): Input Polars DataFrame containing crash data.
        color_column (str): Column to use for color mapping. Must be one of
                            'nivel_dano_vehiculo', 'total_lesionados', 'genero_lesionado', or 'punto_impacto'.
        min_severity (Union[int, float], optional): Minimum crash severity level to include.
                                                    Crashes with severity less than this value
                                                    will be filtered out. Defaults to 0.

    Returns:
        None: Saves the map as an HTML file.

    Raises:
        ValueError: If color_column is not one of the allowed values.
    """
    allowed_columns = [
        "nivel_dano_vehiculo",
        "total_lesionados",
        "genero_lesionado",
        "punto_impacto",
    ]
    if color_column not in allowed_columns:
        raise ValueError(f"color_column must be one of {allowed_columns}")

    # Filter the data
    filtered_df = filter_crash_severity(df, min_severity)

    # Convert to pandas DataFrame
    pan_df = filtered_df.to_pandas()

    # Filter out rows with NaN values in latitude, longitude, or the color column
    pan_df = pan_df.dropna(subset=["latitud", "longitud", color_column])

    # Define the coordinates for the center of Mexico City
    mexico_city_coords = [19.4326, -99.1332]

    # Create a scatter map using Plotly Express
    fig = px.scatter_mapbox(
        pan_df,
        lat="latitud",
        lon="longitud",
        zoom=10,
        center={"lat": mexico_city_coords[0], "lon": mexico_city_coords[1]},
        title=f"Mexico City - {color_column.replace('_', ' ').title()}",
        height=600,
        color=color_column,
        color_continuous_scale=(
            "Viridis" if color_column != "genero_lesionado" else None
        ),
        labels={color_column: color_column.replace("_", " ").title()},
    )

    # Customize map layout
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})

    # Save the map as an HTML file
    fig.write_html(f"mexico_city_map_{color_column}.html")

    print(f"Map saved as mexico_city_map_{color_column}.html")
