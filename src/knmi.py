from pathlib import Path
import pandas as pd
import requests
import warnings
from typing import Union
import xarray as xr
from tqdm import tqdm
from glob import glob

class KNMIApp:
    def __init__(self, nc_file_abs_path: Union[Path, str]):
        self.nc_file_abs_path = Path(nc_file_abs_path)
        assert self.nc_file_abs_path.is_file(), "NC file does not exist."
        self.ds = self.load_data(self.nc_file_abs_path)

    @staticmethod
    def load_data(nc_file_abs_path):
        try:
            with xr.open_dataset(nc_file_abs_path) as ds:
                nc_data = ds.copy(deep=True)
            print('Load successful!')
            return nc_data
        except FileNotFoundError:
            print('NC file not found!')
            return None

    def get_station_codes(self, verbose=True):
        self.station_info = pd.DataFrame({'code': self.ds.station.values,
                                          'station_name': self.ds['stationname'].values,
                                          'lat': self.ds['lat'].values,
                                          'lon': self.ds['lon'].values})
        if verbose:
            print(self.station_info)

        return self.station_info

    def get_variables_info(self, verbose=False):
        assert self.ds is not None, 'Load the dataset first.'

        variable_names = list(self.ds._variables.keys())

        var_names = []
        var_units = []
        var_info = []
        for variable in variable_names:
            try:  # Not all variables has units and long name description
                var_units.append(self.ds[variable].units)
                var_info.append(self.ds[variable].attrs['long_name'])
                var_names.append(variable)

            except AttributeError:
                pass

        self.variable_info = pd.DataFrame({'variable': var_names, 'information': var_info, 'units': var_units})

        if verbose: print(self.variable_info)

        return self.variable_info

    def get_time_range_dataset(self, verbose=False):
        minimum_date = pd.to_datetime(self.ds.time.values.min())
        maximum_date = pd.to_datetime(self.ds.time.values.max())

        if verbose:
            print(f'Minimum date-time: {str(minimum_date)}')
            print(f'Maximum date-time: {str(maximum_date)}')

        return [minimum_date, maximum_date]

    def get_data(self, variable:str, station:str='06391', start:str=None, end:str=None, plot=False) -> pd.DataFrame:
        """
        Extract data from the .nc file

        Parameters:
        -----------
        variable: str
        Acronym of the variable to be extracted. Consult the possible variable names in get_variable_names().
        If the variable is 'all', all variables are extracted.

        station: str
        String with the number of the station. Consult the possible station names with get_station_codes()

        start: str
        String with the starting date of the data. Format must be 'YYYY-MM-DD HH:MM:SS'. If not provided, the start
        date will be the first data value of the .nc file. Default is None.

        end: str
        String with the end date of the data. Format must be 'YYYY-MM-DD HH:MM:SS'. If not provided, the end date will
        be the last data value of the .nc file. Default is Nonw

        plot: bool
        It will create a plot to show the requested variable data from the .nc file from the `start` and `end` dates.
        Default is False.


        Returns:
        --------
        data: pandas.DataFrame:
        Data frame with the requested variable data from the .nc file with the requested start/end dates.

        """

        if variable == 'all':
            variable = self.variable_info.variable.to_list()

        if isinstance(variable, list):
            if (len(variable) >= 2) and plot:
                print("You can not plot 2 variables in the same graph... disabling plot...")
                plot = False

        if start is not None and end is not None:
            data = self.ds.sel(station=station, time=slice(start, end))

        else:
            data = self.ds.sel(station=station)

        data_frame = data[variable].to_dataframe()
        data_frame.drop(columns=['station'], inplace=True)

        if plot:
            try:
                import matplotlib.pyplot as plt
                import matplotlib.dates as mdates
            except ImportError:
                print("Please install Matplotlib before.")
                return data_frame

            info = self.get_variables_info(verbose=False)
            idx = info.variable == variable
            hours = mdates.HourLocator(interval=2)  # Every 3 - hour
            days = mdates.DayLocator()  # Every day

            fig = plt.figure(figsize=(10, 4))
            ax = fig.subplots(1, 1)
            fig.subplots_adjust(left=0.1, right=0.97, bottom=0.2, top=0.9, hspace=0.4, wspace=0.35)
            ax.plot(data_frame)
            ax.xaxis.set_major_locator(days)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
            ax.tick_params(which='major', length=12, labelsize=9)

            ax.xaxis.set_minor_locator(hours)
            ax.xaxis.set_minor_formatter(mdates.DateFormatter('%H'))
            ax.tick_params(which='minor', length=2, color='r', labelsize=6)
            ax.set_title(info['information'][idx].values.item())
            ax.set_ylabel(info['units'][idx].values.item())
            ax.set_xlabel('Time')
            ax.grid(which='major', linewidth=1)
            ax.grid(which='minor', linestyle='--', linewidth=0.4, alpha=0.8)
            plt.show()

        return data_frame




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


def merge_nc_files(downloads_folder: Union[str, Path],
                   merge_folder: Union[str, Path]):

    file_merged = "merged_dataset"
    files = sorted(downloads_folder.glob("*.nc"))

    data_sets = []
    merged_batch = []
    for ii, file in enumerate(tqdm(files)):
        with xr.open_dataset(file) as ds:
            data_sets.append(ds.copy(deep=True))

        if (ii + 1) % 500 == 0:
            print(f"Merging a batch of 500 files....")
            merged_batch.append(xr.merge(data_sets))
            data_sets = []

    data_set_merging = xr.merge(data_sets)  # IF less than 1000 or non multiple of 1000 e.g. 1002 -- 2 files are lost
    merged_batch_merging = xr.merge(merged_batch)
    merged_downloaded_files =  xr.merge([merged_batch_merging, data_set_merging])

    if Path(merge_folder / f'{file_merged}.nc').is_file():
        path_local_merged_file = glob(str(merge_folder / 'merged*.nc'))
        merged_local_database_file = []
        with xr.open_dataset(path_local_merged_file[0]) as ds:
            merged_local_database_file.append(ds.copy(deep=True))

        # 3. Merge the downloaded files (one single .nc file) with the local database.
        merge_data_sets = xr.merge([merged_downloaded_files, merged_local_database_file[0]])

        merge_data_sets.to_netcdf(merge_folder/ f'{file_merged}.nc')  # Rewrite the file
    else:
        merge_data_sets = xr.merge([merged_downloaded_files])
        merge_data_sets.to_netcdf(merge_folder/ f'{file_merged}.nc')  # Create new file database

