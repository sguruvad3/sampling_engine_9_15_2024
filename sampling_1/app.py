# import asyncio
import boto3
from botocore.exceptions import ClientError
import datetime as datetime_object
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra_sigv4.auth import SigV4AuthProvider
import json
import logging
import os
import pandas as pd
from pathlib import Path
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED

def format_time(input_time:str) -> str:
    '''
    Formats timestamp for insertion to keyspace database
    '''
    timestamp_obj = pd.to_datetime(input_time, format='%Y-%m-%d %H:%M:%S.%f %z UTC')
    
    year = timestamp_obj.year
    month = str(timestamp_obj.month).zfill(2)
    day = str(timestamp_obj.day).zfill(2)
    hours = str(timestamp_obj.hour).zfill(2)
    minutes = str(timestamp_obj.minute).zfill(2)
    seconds = str(timestamp_obj.second).zfill(2)
    microseconds = str(timestamp_obj.microsecond)
    milliseconds = microseconds[:-3]
    timezone_info = timestamp_obj.tzinfo

    #parse time zone
    time_object = dt_time(tzinfo=timezone_info)
    timezone_offset = time_object.utcoffset().seconds//3600
    timezone_offset = str(timezone_offset).zfill(4)
    output_time = f'{year}-{month}-{day} {hours}:{minutes}:{seconds}.{milliseconds}+{timezone_offset }'
    
    return output_time

def format_date(timestamp_obj:datetime) -> str:
    '''
    Formats calendar date for query on keyspace database
    '''  
    year = timestamp_obj.year
    month = str(timestamp_obj.month).zfill(2)
    day = str(timestamp_obj.day).zfill(2)
    output_date = f'{year}-{month}-{day}'
    return output_date

class sampling_engine():
    def __init__(self):
        '''
        '''
        self.data_dir = Path('data')
        self.config_dir = Path('config')

        self.raw_data_folder = 'raw'
        self.log_folder = 'log'
        self.credentials_folder = 'credentials'

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.aws_user_credentials_filename = 'ingest-engine-user-1_accessKeys.csv'
        self.iam_role_credentials_filename = 'cassandra_iam_role_credentials.csv'
        self.keyspace_endpoint_filename = 'keyspace_configuration.csv'
        self.cassandra_security_certificate_filename = 'sf-class2-root.crt'

        self.credentials_dir = self.config_dir / self.credentials_folder
        self.credentials_dir.mkdir(parents=True, exist_ok=True)

        self.aws_user_credentials_path = self.credentials_dir / self.aws_user_credentials_filename
        self.iam_role_credentials_path = self.credentials_dir / self.iam_role_credentials_filename
        self.keyspace_endpoint_path = self.credentials_dir / self.keyspace_endpoint_filename
        self.cassandra_security_certificate_path = self.credentials_dir / self.cassandra_security_certificate_filename
        
        self.raw_data_dir = self.data_dir / self.raw_data_folder
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_data_formatted_filename = None

        self.log_dir = self.config_dir / self.log_folder
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_filename = f'{date.today()}.log'
        self.log_path = self.log_dir / log_filename

        self.aws_user_credentials_dataframe = None
        self.iam_role_credentials_dataframe = None
        self.keyspace_endpoint_dataframe = None

        self.aws_user_access_key_id = None
        self.aws_user_secret_access_key = None
        self.iam_role_arn = None

        self.aws_default_region = None

        self.temporary_credentials_access_key_id = None
        self.temporary_credentials_secret_key = None
        self.temporary_credentials_session_token = None
        self.iam_role_timeout = int(datetime_object.timedelta(hours=12).total_seconds()) #hours

        #Parameters for keyspace connection and database instance
        self.ssl_context = None
        self.auth_provider = None
        self.keyspace_endpoint = None
        self.cluster_object = None
        self.keyspace_port = 9142
        self.keyspace_name = None
        self.keyspace_table = None
        # self.fetch_size = 1e2
        self.raw_data_select_timeout_seconds = 60

        #Parameters for keyspace table
        self.id_column_name = 'id'
        self.latitude_column_name = 'lat'
        self.longitude_column_name = 'lon'
        self.mmsi_column_name = 'mmsi'
        self.timestamp_column_name = 'time'
        self.date_column_name = 'calendar_date'

        self.dataframe_raw = None
        self.dataframe_sampled = None

        self.columns_list = [self.latitude_column_name, self.longitude_column_name, self.mmsi_column_name, self.timestamp_column_name]
        self.row_limit = 1e6
        self.row_counter = None
        self.sampled_timestamp_format = '%Y-%m-%d %H:%M:%S'
        self.raw_file_counter = None
        self.raw_filename_timestamp_format = '%Y%m%d%H%M%S'
        
        self.latitude_list = None
        self.longitude_list = None
        self.mmsi_list = None
        self.timestamp_list = None

        #boto3 objects
        self.aws_sts_client = None
        self.aws_keyspaces_session = None

        #timer objects
        self.temporary_credentials_start_time_object = None

        logging.basicConfig(filename=str(self.log_path), format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M", level=logging.INFO, filemode='a')

        return

    def load_aws_user_credentials(self):
        '''
        Loads credentials for AWS user
        '''
        self.aws_user_credentials_dataframe = pd.read_csv(self.aws_user_credentials_path)
        self.aws_user_access_key_id = self.aws_user_credentials_dataframe['Access key ID'][0]
        self.aws_user_secret_access_key = self.aws_user_credentials_dataframe['Secret access key'][0]
        return

    def load_cassandra_iam_role_credentials(self):
        '''
        Loads ARN for IAM role to interact with Cassandra
        '''
        self.iam_role_credentials_dataframe = pd.read_csv(self.iam_role_credentials_path)
        self.iam_role_arn = self.iam_role_credentials_dataframe['ARN'][0]
        return

    def load_keyspace_configuration(self):
        '''
        Loads endpoint for keyspace
        '''
        self.keyspace_endpoint_dataframe = pd.read_csv(self.keyspace_endpoint_path)
        self.aws_default_region = self.keyspace_endpoint_dataframe['region'][0]
        self.keyspace_endpoint = self.keyspace_endpoint_dataframe['endpoint'][0]
        self.keyspace_name = self.keyspace_endpoint_dataframe['keyspace'][0]
        self.keyspace_table = self.keyspace_endpoint_dataframe['table'][0]
        return

    def get_keyspace_credentials(self):
        '''
        Returns temporary credentials to access AWS Keyspace (Cassandra database)
        '''
        self.load_aws_user_credentials()
        self.aws_sts_client = boto3.client('sts', aws_access_key_id=self.aws_user_access_key_id, aws_secret_access_key=self.aws_user_secret_access_key, region_name=self.aws_default_region)
        self.load_cassandra_iam_role_credentials()

        try:
            response = self.aws_sts_client.assume_role(RoleArn=self.iam_role_arn, RoleSessionName='ignored-by-weka-s3', DurationSeconds=self.iam_role_timeout)
            self.temporary_credentials_access_key_id = response['Credentials']['AccessKeyId']
            self.temporary_credentials_secret_key = response['Credentials']['SecretAccessKey']
            self.temporary_credentials_session_token = response['Credentials']['SessionToken']
        except ClientError as e:
            logging.error(e)
        
        self.clear_aws_user_credentials()
        self.close_aws_sts_boto3_session()
        return

    def setup_keyspace_connection(self):
        '''
        Loads connection parameters to interact with cassandra database
        '''
        self.load_keyspace_configuration()
        self.ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        self.ssl_context.load_verify_locations(self.cassandra_security_certificate_path)
        self.ssl_context.verify_mode = CERT_REQUIRED
        self.aws_keyspaces_session = boto3.Session(aws_access_key_id=self.temporary_credentials_access_key_id, aws_secret_access_key=self.temporary_credentials_secret_key, aws_session_token=self.temporary_credentials_session_token, region_name=self.aws_default_region)
        self.auth_provider = SigV4AuthProvider(self.aws_keyspaces_session)

        self.cluster_object = Cluster([self.keyspace_endpoint], ssl_context=self.ssl_context, auth_provider=self.auth_provider, port=self.keyspace_port)
        self.aws_keyspaces_session = self.cluster_object.connect()
        return

    def select_all(self):
        '''
        select all records from keyspace cluster
        '''
        sql_statement = f'SELECT * FROM {self.keyspace_name}.{self.keyspace_table}'
        response_records = self.aws_keyspaces_session.execute(sql_statement)
        for record in response_records:
            print(record.id, record.lat, record.lon, record.mmsi, record.time)
        return

    def select_records(self):
        '''
        Selects records from specified start and end time
        Previous day
        '''
        utc_time_delta = timedelta(hours=0)
        utc_timezone_object = timezone(utc_time_delta)
        
        current_date = date.today()
        start_date = current_date - timedelta(days=1)

        query_start_time = datetime(year=start_date.year, month=start_date.month, day=start_date.day, hour=0, minute=0, second=0, tzinfo=utc_timezone_object)

        query_end_time = datetime(year=start_date.year, month=start_date.month, day=start_date.day, hour=23, minute=59, second=59, tzinfo=utc_timezone_object)

        query_start_time_string = format_date(query_start_time)
        query_end_time_string = format_date(query_end_time)

        sql_statement = SimpleStatement(f"SELECT {self.latitude_column_name}, {self.longitude_column_name}, {self.mmsi_column_name}, {self.timestamp_column_name} FROM {self.keyspace_name}.{self.keyspace_table} WHERE {self.date_column_name}='{query_end_time_string}' ALLOW FILTERING;")

        try:         
            message = f'started select query for {query_start_time_string}'
            logging.info(message)
            records = self.aws_keyspaces_session.execute(statement=sql_statement, timeout=self.raw_data_select_timeout_seconds)
            message = f'completed select query for {query_start_time_string}'
            logging.info(message)
           
            self.reset_raw_data_lists()
            self.reset_dataframe_raw()
            self.reset_row_counter()
            self.reset_raw_file_counter()

            message = 'started raw data file writes'
            logging.info(message)
            
            for record in records:

                lat = record.lat
                lon = record.lon
                mmsi = record.mmsi
                timestamp = record.time
                formatted_timestamp = timestamp.strftime(self.sampled_timestamp_format)
                self.raw_data_formatted_filename = timestamp.strftime(self.raw_filename_timestamp_format)

                self.latitude_list.append(lat)
                self.longitude_list.append(lon)
                self.mmsi_list.append(mmsi)
                self.timestamp_list.append(timestamp)

                if self.row_counter > self.row_limit:

                    self.make_dataframe()
                    self.write_raw_data_file()

                    message = f'completed raw data file {self.raw_data_formatted_filename}'
                    logging.info(message)

                    self.clear_dataframe_raw()
                    self.reset_raw_data_lists()
                    self.reset_row_counter()

                    self.increment_raw_file_counter()

                self.increment_row_counter()
                
        except:
            pass
        return

    def write_raw_data_file(self):
        '''
        Writes raw data to file
        Output: self.raw_data_dir
        '''
        filename = self.raw_data_formatted_filename + 'parquet.gzip'
        out_path = self.raw_data_dir / filename
        if not self.dataframe_raw.empty:
            self.dataframe_raw.to_parquet(str(out_path))
        return

    def make_dataframe(self):
        '''
        Constructs self.dataframe_raw from data contained in self.columns_list
        '''
        self.dataframe_raw = pd.DataFrame(columns=self.columns_list)
        self.dataframe_raw[self.latitude_column_name] = self.latitude_list
        self.dataframe_raw[self.longitude_column_name] = self.longitude_list
        self.dataframe_raw[self.mmsi_column_name] = self.mmsi_list
        self.dataframe_raw[self.timestamp_column_name] = self.timestamp_list
        return

    def reset_raw_data_lists(self):
        '''
        Resets list objects to empty. Used for storing raw data
        '''
        self.latitude_list = []
        self.longitude_list = []
        self.mmsi_list = []
        self.timestamp_list = []
        return

    def reset_row_counter(self):
        '''
        Resets self.row_counter to 1
        '''
        self.row_counter = 1
        return

    def increment_row_counter(self):
        '''
        Increments self.row_counter by 1
        '''
        self.row_counter += 1
        return

    def reset_raw_file_counter(self):
        '''
        Resets counter for files containing raw data
        '''
        self.raw_file_counter = 1
        return

    def increment_raw_file_counter(self):
        '''
        Increments self.raw_file_counter by 1
        '''
        self.raw_file_counter += 1
        return

    def reset_dataframe_raw(self):
        '''
        Instantiates empty dataframe for storing results in self.dataframe_raw
        '''
        self.dataframe_raw = pd.DataFrame(columns=self.columns_list)
        return

    def reset_dataframe_sampled(self):
        '''
        Instantiates empty dataframe for storing results in self.dataframe_sampled
        '''
        self.dataframe_sampled = pd.DataFrame(columns=self.columns_list)
        return

    def clear_dataframe_raw(self):
        '''
        Clears self.dataframe_raw from memory
        '''
        self.dataframe_raw = None
        return

    def clear_dataframe_sampled(self):
        '''
        Clears self.dataframe_sampled from memory
        '''
        self.dataframe_sampled = None
        return

    def clear_aws_user_credentials(self):
        '''
        Clears AWS user credentials from memory
        '''
        self.aws_user_credentials_dataframe = None
        self.aws_user_access_key_ID = None
        self.aws_user_secret_access_key = None
        return

    def close_aws_sts_boto3_client(self):
        '''
        Clears self.aws_sts_client from memory
        '''
        self.aws_sts_client = None
        return

    def close_aws_sts_boto3_session(self):
        '''
        Clears self.aws_keyspaces_session from memory
        '''
        self.aws_keyspaces_session = None
        return

if __name__ == "__main__":
    engine_object = sampling_engine()
    engine_object.get_keyspace_credentials()
    engine_object.setup_keyspace_connection()
    engine_object.select_records()





