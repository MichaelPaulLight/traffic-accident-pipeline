"""
This module accesses vehicle crash records from Mexico's International Data Institute.

It parses the webpage where the records are available as CSVs contained in zip files
and downloads the records to a specified file path on the user's local computer.
"""

import os
import shutil
from typing import Optional
import requests
from bs4 import BeautifulSoup
import zipfile
import io
from datetime import datetime


def get_data(url: str, base_path: str, button_path: str = "a") -> None:
    """
    Download, extract, and organize vehicle crash records from Mexico's International Data Institute.

    Args:
        url (str): The webpage where the data are accessible.
        base_path (str): The base directory path where the files will be organized.
        button_path (str, optional): The HTML tag used to identify download links. Defaults to "a".

    Raises:
        requests.RequestException: If there's an error downloading the webpage or files.
        zipfile.BadZipFile: If there's an error extracting the ZIP files.
        OSError: If there's an error creating directories or saving files.

    Returns:
        None
    """
    try:
        # Download the webpage
        response = requests.get(url)
        response.raise_for_status()

        # Parse the webpage
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all links on the webpage
        links = soup.find_all(button_path)

        # Create base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Create data dictionary folder
        data_dict_folder = os.path.join(base_path, "data-dictionary")
        os.makedirs(data_dict_folder, exist_ok=True)

        # Create year folders
        current_year = datetime.now().year
        for year in range(2015, current_year + 1):
            os.makedirs(os.path.join(base_path, str(year)), exist_ok=True)

        files_found = False

        for link in links:
            href = link.get("href", "").lower()
            if any(ext in href for ext in [".zip", ".xlsx", ".csv"]):
                files_found = True
                file_url = link["href"]
                file_response = requests.get(file_url)
                file_response.raise_for_status()

                if href.endswith(".zip"):
                    # Handle ZIP files
                    z = zipfile.ZipFile(io.BytesIO(file_response.content))
                    temp_dir = os.path.join(base_path, "temp_extract")
                    z.extractall(path=temp_dir)
                    print(f"Extracted ZIP file from {file_url}")

                    # Process extracted files
                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            process_file(
                                os.path.join(root, file), base_path, data_dict_folder
                            )

                    # Clean up temp directory
                    shutil.rmtree(temp_dir)
                else:
                    # Handle Excel or CSV files
                    file_name = os.path.basename(file_url)
                    process_file(
                        file_name,
                        base_path,
                        data_dict_folder,
                        file_content=file_response.content,
                    )

        if not files_found:
            print("No relevant files found at the given endpoint.")

    except requests.RequestException as e:
        print(f"Error downloading data: {e}")
    except zipfile.BadZipFile as e:
        print(f"Error extracting ZIP file: {e}")
    except OSError as e:
        print(f"Error creating directory or saving files: {e}")


def process_file(
    file_path: str, base_path: str, data_dict_folder: str, file_content=None
):
    """
    Process a single file and move it to the appropriate folder.
    """
    file_name = os.path.basename(file_path)

    if "diccionario-percances-viales-axa-1" in file_name.lower():
        destination = os.path.join(data_dict_folder, file_name)
    else:
        year = extract_year(file_name)
        if year:
            destination = os.path.join(base_path, str(year), file_name)
        else:
            print(f"Could not determine year for file: {file_name}")
            return

    if os.path.exists(destination):
        print(f"File already exists, skipping: {destination}")
    else:
        if file_content:
            with open(destination, "wb") as f:
                f.write(file_content)
        else:
            shutil.move(file_path, destination)
        print(f"Moved file to: {destination}")


def extract_year(file_name: str) -> Optional[int]:
    """
    Extract the year from a filename.
    """
    for year in range(2015, datetime.now().year + 1):
        if str(year) in file_name:
            return year
    return None
