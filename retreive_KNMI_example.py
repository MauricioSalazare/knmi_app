import logging
import os
import sys
from datetime import datetime
from datetime import timedelta

import requests

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

api_url = "https://api.dataplatform.knmi.nl/open-data"
api_version = "v1"


def main():
    # Parameters
    api_key = "<API_KEY>"
    dataset_name = "Actuele10mindataKNMIstations"
    dataset_version = "2"

    # Use get file to retrieve a file from one hour ago.
    # Filename format for this dataset equals KMDS__OPER_P___10M_OBS_L2_YYYYMMDDHHMM.nc,
    # where the minutes are increased in steps of 10.
    timestamp_now = datetime.utcnow()
    timestamp_one_hour_ago = timestamp_now - timedelta(hours=1) - timedelta(minutes=timestamp_now.minute % 10)
    filename = f"KMDS__OPER_P___10M_OBS_L2_{timestamp_one_hour_ago.strftime('%Y%m%d%H%M')}.nc"

    logger.debug(f"Current time: {timestamp_now}")
    logger.debug(f"One hour ago: {timestamp_one_hour_ago}")
    logger.debug(f"Dataset file to download: {filename}")

    endpoint = f"{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    get_file_response = requests.get(endpoint, headers={"Authorization": api_key})

    if get_file_response.status_code != 200:
        logger.error("Unable to retrieve download url for file")
        logger.error(get_file_response.text)
        sys.exit(1)

    logger.info(f"Successfully retrieved temporary download URL for dataset file {filename}")

    download_url = get_file_response.json().get("temporaryDownloadUrl")
    # Check logging for deprecation
    if "X-KNMI-Deprecation" in get_file_response.headers:
        deprecation_message = get_file_response.headers.get("X-KNMI-Deprecation")
        logger.warning(f"Deprecation message: {deprecation_message}")

    download_file_from_temporary_download_url(download_url, filename)


def download_file_from_temporary_download_url(download_url, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)

    logger.info(f"Successfully downloaded dataset file to {filename}")


if __name__ == "__main__":
    main()
