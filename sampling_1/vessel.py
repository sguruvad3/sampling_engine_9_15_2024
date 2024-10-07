import boto3
from botocore.exceptions import ClientError
from datetime import date
import pandas as pd
import requests
from typing import List

class vessel():
    '''
    Retrieves vessel type from Global Fishing Watch API
    '''
    def __init__(self):
        '''
        '''
        self.data_dir = Path('data')
        self.config_dir = Path('config')

        self.stage_1_folder = 'stage_1'
        self.log_folder = 'log'
        self.credentials_folder = 'credentials'

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.credentials_dir = self.config_dir / self.credentials_folder
        self.credentials_dir.mkdir(parents=True, exist_ok=True)

        self.stage_1_dir = self.data_dir / self.stage_1_folder
        self.stage_1_dir.mkdir(parents=True, exist_ok=True)

        self.log_dir = self.config_dir / self.log_folder
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_filename = f'{date.today()}.log'
        self.log_path = self.log_dir / log_filename

        self.mmsi_column_name = 'mmsi'

        self.dataframe_stage_1 = None

        logging.basicConfig(filename=str(self.log_path), format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M", level=logging.INFO, filemode='a')

        return

    def get_vessel_type(mmsi_list:List) -> pd.DataFrame:
        '''
        Returns dataframe containing vessel type that corresponds to each mmsi
        '''


        return dataframe_out
