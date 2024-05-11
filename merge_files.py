from src import merge_nc_files
from pathlib import Path

parent = Path.cwd()
downloads_folder = parent / 'assets/downloads'
merge_folder = parent / 'assets/merged_data'

merge_nc_files(downloads_folder=downloads_folder,
               merge_folder=merge_folder)