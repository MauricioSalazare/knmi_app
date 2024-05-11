from src import download_files
from dotenv import dotenv_values
from pathlib import Path
from tqdm import tqdm
import time

config = dotenv_values(".env")
api_key = config["API_KEY"]

parent = Path.cwd()
downloads_folder = parent / 'assets/downloads'

while True:
    download_files(abs_path_download_folder=downloads_folder, api_key=api_key, max_downloads=2)
    for _ in tqdm(range(5)):
        time.sleep(1)





