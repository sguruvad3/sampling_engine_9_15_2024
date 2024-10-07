import boto3
from botocore.exceptions import ClientError
from datetime import date
import logging
import pandas as pd
from pathlib import Path
import pprint
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

        self.api_info_filename = 'global_fishing_watch_api_key.csv'
        self.url_column_name = 'url'
        self.api_key_column_name = 'token'

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.credentials_dir = self.config_dir / self.credentials_folder
        self.credentials_dir.mkdir(parents=True, exist_ok=True)

        self.api_info_path = self.credentials_dir / self.api_info_filename

        self.stage_1_dir = self.data_dir / self.stage_1_folder
        self.stage_1_dir.mkdir(parents=True, exist_ok=True)

        self.log_dir = self.config_dir / self.log_folder
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_filename = f'{date.today()}.log'
        self.log_path = self.log_dir / log_filename

        self.mmsi_column_name = 'mmsi'

        self.dataframe_api_info = None
        self.dataframe_stage_1 = None

        self.url = None
        self.api_key = None

        self.load_dataframe_api_info()
        self.clear_dataframe_api_info()

        logging.basicConfig(filename=str(self.log_path), format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M", level=logging.INFO, filemode='a')

        return

    def get_vessel_type_single_mmsi(self, mmsi:str) -> str:
        '''
        Returns dataframe containing vessel type that corresponds to each mmsi
        For single mmsi use
        '''
        headers = {'Authorization':f"Bearer {self.api_key}"}
        url = f'https://gateway.api.globalfishingwatch.org/v3/vessels/search?where=ssvid="{mmsi}"&datasets[0]=public-global-vessel-identity:latest&includes[0]=MATCH_CRITERIA&includes[1]=OWNERSHIP'
        response = requests.get(url,  headers=headers)
        response_dict = response.json()
        vessel_type = response_dict['entries'][0]['combinedSourcesInfo'][0]['shiptypes'][0]['name']
        return vessel_type

    def load_dataframe_api_info(self):
        '''
        Loads API information into self.dataframe_api_info
        '''
        self.dataframe_api_info = pd.read_csv(str(self.api_info_path))
        self.url = self.dataframe_api_info[self.url_column_name][0]
        self.api_key = self.dataframe_api_info[self.api_key_column_name][0]
        return

    def clear_dataframe_api_info(self):
        '''
        Clears self.dataframe_api_info from memory
        '''
        self.dataframe_api_info = None
        return
