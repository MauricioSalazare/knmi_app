from src import KNMIApp

from pathlib import Path

parent = Path.cwd()
merge_file = parent / 'assets/merged_data/merged_dataset.nc'

knmi_app = KNMIApp(merge_file)

variables_info = knmi_app.get_variables_info()