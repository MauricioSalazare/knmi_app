from pathlib import Path
import pandas as pd

import requests
import warnings
from typing import Union

def filename_to_date(filename: str) -> pd.Timestamp:
    full_date = filename[-12:]
    year = full_date[:4]
    month = full_date[4:6]
    day = full_date[6:8]
    hour = full_date[8:10]
    minute = full_date[10:12]
    second = "00"

    time_stamp = pd.to_datetime(f"{year}-{month}-{day} {hour}:{minute}:{second}")

    return time_stamp

def download_files(abs_path_download_folder: Union[str, Path], api_key:str, max_downloads:int=100) -> int:
    api_url = "https://api.dataplatform.knmi.nl/open-data"
    dataset_name = "Actuele10mindataKNMIstations"
    dataset_version = "2"
    max_keys = "1000"
    file_head = "KMDS__OPER_P___10M_OBS_L2_"
    maximum_quota = max_downloads

    abs_path_download_folder_ = Path(abs_path_download_folder)

    assert abs_path_download_folder_.exists(), f"Folder '{abs_path_download_folder_}' does not exist."

    last_file = sorted(abs_path_download_folder_.glob("*.nc"))[-1].stem
    max_date = filename_to_date(last_file)
    print(f"Max date found in folder: {max_date}")

    start_after_filename = file_head + max_date.strftime('%Y%m%d%H%M') + ".nc"

    list_files_response = requests.get(
        f"{api_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
        headers={"Authorization": api_key},
        params={"maxKeys": max_keys,
                "startAfterFilename": start_after_filename})

    response = list_files_response.json()
    files_list = response.get('files')  # Name of the files available in the server
    data_frame = pd.DataFrame(files_list)

    print(f'Files available in the KNMI Server to update local file: {data_frame.shape[0]}')
    print(data_frame)

    if maximum_quota >= data_frame.shape[0]:
        max_number_files = data_frame.shape[0]
    else:
        max_number_files = maximum_quota

    for file_number in range(max_number_files):
        print(f"Retrieving file {file_number} of {maximum_quota}.")
        get_file_response = requests.get(
            f"{api_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{data_frame.filename[file_number]}/url",
            headers={"Authorization": api_key})

        if get_file_response.status_code == 200:
            download_url = get_file_response.json().get("temporaryDownloadUrl")
            with open(f'{abs_path_download_folder_}/{data_frame.filename[file_number]}', 'wb') as file_write:
                file_write.write(requests.get(download_url).content)
        elif get_file_response.status_code == 403:
            warnings.warn('Download stop!! Maximum requests reached to the KNMI API Server.')
            break

    print("Downloading successfully!")

    return 0
