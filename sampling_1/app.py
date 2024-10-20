# import asyncio
import boto3
from botocore.exceptions import ClientError
import datetime as datetime_object
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.metadata import ColumnMetadata
from cassandra.query import SimpleStatement
from cassandra_sigv4.auth import SigV4AuthProvider
import json
import logging
import os
import pandas as pd
import pandera as pa
from pandera import Column, Check
from pathlib import Path
import shutil
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from typing import List
from vessel import vessel

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

class model_engine():
    '''
    '''
    
    def __init__(self):
        '''
        '''
        self.data_dir = Path('data')
        self.config_dir = Path('config')

        self.raw_data_folder = 'raw'
        self.stage_1_folder = 'stage_1'
        self.stage_2_folder = 'stage_2'
        self.log_folder = 'log'
        self.credentials_folder = 'credentials'
        self.vessel_info_folder = 'vessel_info'

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.aws_user_credentials_filename = 'ingest-engine-user-1_accessKeys.csv'
        self.iam_keyspace_role_credentials_filename = 'cassandra_iam_role_credentials.csv'
        self.iam_s3_role_credentials_filename = 's3_iam_role_credentials.csv'
        self.keyspace_endpoint_filename = 'keyspace_configuration.csv'
        self.s3_configuration_filename = 's3_configuration.csv'
        self.cassandra_security_certificate_filename = 'sf-class2-root.crt'
        self.vessel_type_info_filename = 'vessel_type_info.parquet.gzip'
        self.unknown_vessel_type_filename = 'unknown_vessel_type.parquet.gzip'

        self.credentials_dir = self.config_dir / self.credentials_folder
        self.credentials_dir.mkdir(parents=True, exist_ok=True)

        self.aws_user_credentials_path = self.credentials_dir / self.aws_user_credentials_filename
        self.iam_keyspace_role_credentials_path = self.credentials_dir / self.iam_keyspace_role_credentials_filename
        self.iam_s3_role_credentials_path = self.credentials_dir / self.iam_s3_role_credentials_filename
        self.keyspace_endpoint_path = self.credentials_dir / self.keyspace_endpoint_filename
        self.s3_configuration_path = self.credentials_dir / self.s3_configuration_filename
        self.cassandra_security_certificate_path = self.credentials_dir / self.cassandra_security_certificate_filename


        self.vessel_type_info_dir = self.config_dir / self.vessel_info_folder
        self.vessel_type_info_dir.mkdir(parents=True, exist_ok=True)
        self.vessel_type_info_path = self.vessel_type_info_dir / self.vessel_type_info_filename
        self.unknown_vessel_type_path = self.vessel_type_info_dir / self.unknown_vessel_type_filename

        self.raw_data_dir = self.data_dir / self.raw_data_folder
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_data_formatted_filename = None

        self.stage_1_dir = self.data_dir / self.stage_1_folder
        self.stage_1_dir.mkdir(parents=True, exist_ok=True)
        self.stage_1_formatted_filename = None

        self.stage_2_dir = self.data_dir / self.stage_2_folder
        self.stage_2_dir.mkdir(parents=True, exist_ok=True)
        self.stage_2_formatted_filename = None

        self.log_dir = self.config_dir / self.log_folder
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_filename = f'{date.today()}.log'
        self.log_path = self.log_dir / log_filename

        self.aws_user_credentials_dataframe = None
        self.iam_keyspace_role_credentials_dataframe = None
        self.iam_s3_role_credentials_dataframe = None
        self.keyspace_endpoint_dataframe = None
        self.s3_configuration_dataframe = None

        self.aws_user_access_key_id = None
        self.aws_user_secret_access_key = None
        self.iam_keyspace_role_arn = None
        self.iam_s3_role_arn = None

        self.aws_default_region = None

        self.temporary_credentials_access_key_id = None
        self.temporary_credentials_secret_key = None
        self.temporary_credentials_session_token = None
        self.iam_keyspace_role_timeout = int(datetime_object.timedelta(hours=12).total_seconds()) #hours

        self.s3_bucket_name = None

        #Parameters for keyspace connection and database instance
        self.ssl_context = None
        self.auth_provider = None
        self.keyspace_endpoint = None
        self.cluster_object = None
        self.keyspace_port = 9142
        self.keyspace_name = None
        self.keyspace_table = None
        self.fetch_size = 1e2
        self.raw_data_select_timeout_seconds = 60

        #Parameters for keyspace table
        self.id_column_name = 'id'
        self.latitude_column_name = 'lat'
        self.longitude_column_name = 'lon'
        self.mmsi_column_name = 'mmsi'
        self.timestamp_column_name = 'time'
        self.year_column_name = 'year'
        self.month_column_name = 'month'
        self.day_column_name = 'day'
        self.hour_column_name = 'hour'

        self.dataframe_raw = None
        self.dataframe_stage_1 = None
        self.dataframe_stage_2 = None
        self.dataframe_mmsi_vessel_type = None
        self.dataframe_unknown_vessel_type = None

        self.dataframe_mmsi_column_name = 'mmsi'
        self.dataframe_vessel_type_column_name = 'vessel_type'
        self.dataframe_unknown_vessel_type_mmsi_column_name = 'mmsi'

        self.vessel_type_write_threshold = 1e2
        self.unknown_vessel_type_logging_threshold = 1e1

        self.columns_list = [self.latitude_column_name, self.longitude_column_name, self.mmsi_column_name, self.timestamp_column_name]
        self.row_limit = 1e6
        self.row_counter = None
        self.total_row_counter = None
        self.total_row_limit = 10560779
        self.sampled_timestamp_format = '%Y-%m-%d %H:%M:%S'
        self.raw_file_counter = None
        self.raw_filename_timestamp_format = '%Y%m%d%H%M%S'
        self.raw_file_limit = 10
        self.sampling_resolution = "5Min"
        
        self.latitude_list = None
        self.longitude_list = None
        self.mmsi_list = None
        self.timestamp_list = None

        #boto3 objects
        self.aws_sts_client = None
        self.aws_keyspaces_session = None
        self.aws_s3_session = None

        #timer objects
        self.temporary_credentials_start_time_object = None

        #pandera parameters
        self.longitude_lower_limit = -180
        self.longitude_upper_limit = 180
        self.latitude_lower_limit = -90
        self.latitude_upper_limit = 90

        self.delta_time_limit = pd.Timedelta(hours=1)
        self.delta_longitude_limit = 1
        self.delta_latitude_limit = 1

        #secondary column names
        self.delta_time_column_name = 'delta_time'
        self.delta_longitude_column_name = 'delta_lon'
        self.delta_latitude_column_name = 'delta_lat'
    

        self.raw_data_schema = pa.DataFrameSchema({ self.timestamp_column_name: Column('datetime64[ns]', nullable=False), 
                                                    self.mmsi_column_name: Column(str, Check.str_length(min_value=9, max_value=9), nullable=False), 
                                                    self.latitude_column_name: Column('float64', Check(lambda s: (s >= self.latitude_lower_limit) & (s <= self.latitude_upper_limit)), nullable=False), 
                                                    self.longitude_column_name: Column('float64', Check(lambda s: (s >= self.longitude_lower_limit) & (s <= self.longitude_upper_limit)), nullable=False),
                                                    self.delta_time_column_name: Column(pd.Timedelta, Check(lambda s: (s <= self.delta_time_limit)), nullable= False),
                                                    self.delta_latitude_column_name: Column('float64', Check(lambda s: (s <= self.delta_latitude_limit))),
                                                    self.delta_longitude_column_name: Column('float64', Check(lambda s: (s <= self.delta_longitude_column_name)))
                                                    }, drop_invalid_rows=True, coerce=True)

        #mmsi retrieval
        self.vessel_type_retrieval_engine = None

        logging.basicConfig(filename=str(self.log_path), format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M", level=logging.INFO, filemode='w')

        return

    def union_mmsi_list(self, mmsi_list_1:List, mmsi_list_2:List) -> List:
        '''
        Returns union of two lists of mmsi numbers. Duplicates removed
        '''
        return list(set(mmsi_list_1) | set(mmsi_list_2))

    def difference_mmsi_list(self, mmsi_list_data_files:List, mmsi_list_stored:List) -> List:
        '''
        Returns difference of two lists of mmsi numbers
        '''
        set_union = set(mmsi_list_data_files) | set(mmsi_list_stored) #removes duplicates
        set_difference = set_union - set(mmsi_list_stored)
        return list(set_difference)

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
        self.iam_keyspace_role_credentials_dataframe = pd.read_csv(self.iam_keyspace_role_credentials_path)
        self.iam_keyspace_role_arn = self.iam_role_credentials_dataframe['ARN'][0]
        return

    def load_s3_iam_role_credentials(self):
        '''
        Loads ARN for IAM role to interact with S3
        '''
        self.iam_s3_role_credentials_dataframe = pd.read_csv(self.iam_s3_role_credentials_path)
        self.iam_s3_role_arn = self.iam_s3_role_credentials_dataframe['ARN'][0]
        return

    def load_keyspace_configuration(self):
        '''
        Loads endpoint for keyspace
        '''
        self.keyspace_endpoint_dataframe = pd.read_csv(str(self.keyspace_endpoint_path))
        self.aws_default_region = self.keyspace_endpoint_dataframe['region'][0]
        self.keyspace_endpoint = self.keyspace_endpoint_dataframe['endpoint'][0]
        self.keyspace_name = self.keyspace_endpoint_dataframe['keyspace'][0]
        self.keyspace_table = self.keyspace_endpoint_dataframe['table'][0]
        return
    
    def load_s3_configuration(self):
        '''
        Loads configuration parameters for uploading files to S3
        '''
        self.s3_configuration_dataframe = pd.read_csv(str(self.s3_configuration_path))
        self.aws_default_region = self.s3_configuration_dataframe['region'][0]
        self.s3_bucket_name = self.s3_configuration_dataframe['bucket'][0]
        return

    def load_vessel_info(self):
        '''
        Loads vessel info to memory
        '''
        if self.vessel_type_info_path.exists():
            self.dataframe_mmsi_vessel_type = pd.read_parquet(str(self.vessel_type_info_path), engine='pyarrow')
            message = 'loaded vessel info to memory'
            logging.info(message)
        return

    def save_vessel_info(self):
        '''
        Saves vessel info to disk
        '''
        self.dataframe_mmsi_vessel_type.to_parquet(str(self.vessel_type_info_path), engine='pyarrow')
        message = 'saved vessel info to disk'
        logging.info(message)
        return

    def load_unknown_vessel_info(self):
        '''
        Loads unknown vessels info to memory
        '''
        if self.unknown_vessel_type_path.exists():
            self.dataframe_unknown_vessel_type = pd.read_parquet(str(self.unknown_vessel_type_path), engine='pyarrow')
            message = 'loaded unknown vessels list to memory'
            logging.info(message)
        return

    def save_unknown_vessel_info(self):
        '''
        Saves unknown vessels info to disk
        '''
        self.dataframe_unknown_vessel_type.to_parquet(str(self.unknown_vessel_type_path), engine='pyarrow')
        message = 'saved unknown vessels info to disk'
        logging.info(message)
        return

    def get_keyspace_credentials(self):
        '''
        Returns temporary credentials to access AWS Keyspace (Cassandra database)
        '''
        self.load_aws_user_credentials()
        self.aws_sts_client = boto3.client('sts', aws_access_key_id=self.aws_user_access_key_id, aws_secret_access_key=self.aws_user_secret_access_key, region_name=self.aws_default_region)
        self.load_cassandra_iam_role_credentials()

        try:
            response = self.aws_sts_client.assume_role(RoleArn=self.iam_keyspace_role_arn, RoleSessionName='ignored-by-weka-s3', DurationSeconds=self.iam_keyspace_role_timeout)
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
        # self.aws_keyspaces_session.default_timeout = self.raw_data_select_timeout_seconds
        return

    def get_s3_credentials(self):
        '''
        Returns temporary credentials to access AWS S3
        '''
        self.load_aws_user_credentials()
        self.aws_sts_client = boto3.client('sts', aws_access_key_id=self.aws_user_access_key_id, aws_secret_access_key=self.aws_user_secret_access_key, region_name=self.aws_default_region)

        try:
            response = self.aws_sts_client.assume_role(RoleArn=self.iam_s3_role_arn, RoleSessionName='ignored-by-weka-s3')
            self.temporary_credentials_access_key_id = response['Credentials']['AccessKeyId']
            self.temporary_credentials_secret_key = response['Credentials']['SecretAccessKey']
            self.temporary_credentials_session_token = response['Credentials']['SessionToken']
        except ClientError as e:
            logging.error(e)
        
        self.clear_aws_user_credentials()
        self.close_aws_sts_boto3_session()

        return

    def setup_s3_connection(self):
        '''
        Loads connection parameters to interact with S3 bucket
        '''
        self.load_s3_configuration()
        self.aws_s3_session = boto3.Session(aws_access_key_id=self.temporary_credentials_access_key_id, aws_secret_access_key=self.temporary_credentials_secret_key, aws_session_token=self.temporary_credentials_session_token, region_name=self.aws_default_region)

        resource_client = self.aws_s3_session.resource(service_name='s3', region_name=self.aws_default_region, aws_access_key_id=self.temporary_credentials_access_key_id, aws_secret_access_key=self.temporary_credentials_secret_key, aws_session_token=self.temporary_credentials_session_token)

        return

    def setup_vessel_type_retrieval_engine(self):
        '''
        Instantiates object for retrieving vessel types bases on mmsi
        '''
        self.vessel_type_retrieval_engine = vessel()
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

        start_year = query_start_time.year
        start_month = query_start_time.month
        start_day = query_start_time.day
        start_hour = query_start_time.hour

        end_year = query_end_time.year
        end_month = query_end_time.month
        end_day = query_end_time.day
        end_hour = query_end_time.hour

        sql_statement = SimpleStatement(f"SELECT {self.latitude_column_name}, {self.longitude_column_name}, {self.mmsi_column_name}, {self.timestamp_column_name} FROM {self.keyspace_name}.{self.keyspace_table} WHERE {self.year_column_name}>={start_year} AND {self.year_column_name}<={end_year} AND {self.month_column_name}>={start_month} AND {self.month_column_name}<={end_month} AND {self.day_column_name}>={start_day} AND {self.day_column_name}<={end_day} AND {self.hour_column_name}>={start_hour} AND {self.hour_column_name}<={end_hour} ALLOW FILTERING;")

        try:         
            message = f'started select query for {query_start_time_string}'
            logging.info(message)
            records = self.aws_keyspaces_session.execute(sql_statement)
            message = f'completed select query for {query_start_time_string}'
            logging.info(message)
            
            self.reset_raw_data_lists()
            self.reset_dataframe_raw()
            self.reset_row_counter()
            self.reset_total_row_counter()
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

                #even chunks
                if  self.row_counter == self.row_limit:
            
                    self.make_raw_dataframe()
                    self.write_raw_data_file()
                    message = f'completed raw data file {self.raw_data_formatted_filename}'
                    logging.info(message)

                    self.clear_dataframe_raw()
                    self.reset_raw_data_lists()
                    self.reset_row_counter()

                    self.increment_raw_file_counter()
                
                #last chunk           
                if self.raw_file_counter == self.raw_file_limit and self.total_row_counter>=self.total_row_limit:

                    self.make_raw_dataframe()
                    self.write_raw_data_file()

                    message = f'completed last raw data file {self.raw_data_formatted_filename}'
                    logging.info(message)

                    self.clear_dataframe_raw()
                    self.reset_raw_data_lists()
                    self.reset_row_counter()

                self.increment_row_counter()
                self.increment_total_row_counter()
        
        except:
            pass
        return

    def write_raw_data_file(self):
        '''
        Writes raw data to file
        Output: self.raw_data_dir
        '''
        filename = self.raw_data_formatted_filename + '.parquet.gzip'
        out_path = self.raw_data_dir / filename
        if not self.dataframe_raw.empty:
            self.dataframe_raw.to_parquet(str(out_path), engine='pyarrow', compression='gzip')
        return

    def make_raw_dataframe(self):
        '''
        Constructs self.dataframe_raw from data contained in self.columns_list
        '''
        self.dataframe_raw = pd.DataFrame(columns=self.columns_list)
        self.dataframe_raw[self.latitude_column_name] = self.latitude_list
        self.dataframe_raw[self.longitude_column_name] = self.longitude_list
        self.dataframe_raw[self.mmsi_column_name] = self.mmsi_list
        self.dataframe_raw[self.timestamp_column_name] = self.timestamp_list
        return

    def write_stage_1_data_file(self):
        '''
        Writes stage 1 data to file
        Output: self.stage_1_dir
        '''
        filename = self.stage_1_formatted_filename + '.parquet.gzip'
        out_path = self.stage_1_dir / filename
        if not self.dataframe_stage_1.empty:
            self.dataframe_stage_1.to_parquet(str(out_path), engine='pyarrow', compression='gzip')
        return

    def write_stage_2_data_file(self):
        '''
        Writes stage 2 data to file
        Output: self.stage_2_dir
        '''
        filename = self.stage_2_formatted_filename + '.parquet.gzip'
        out_path = self.stage_2_dir / filename
        if not self.dataframe_stage_2.empty:
            self.dataframe_stage_2.to_parquet(str(out_path), engine='pyarrow', compression='gzip')
        return
    
    def ETL_stage_1(self):
        '''
        Coerce data to self.raw_data_schema
        Sample raw data at rate 
        Source: self.raw_data_dir
        Destination: self.stage_1_dir
        '''
        message = 'begin ETL stage 1 on all data files'
        logging.info(message)
        files_list = list(self.raw_data_dir.glob('*'))        
        for file_path in files_list:
            message = f'begin stage 1 of file {file_path.name}'
            logging.info(message)
            self.dataframe_raw = pd.read_parquet(str(file_path), engine='pyarrow')
            self.add_secondary_columns()
            self.stage_1_formatted_filename = file_path.stem.split('.')[0]
            try:
                self.dataframe_stage_1 = self.raw_data_schema.validate(self.dataframe_raw, lazy=True)
            except pa.errors.SchemaError as exc:
                logging.info(exc.message)
            # mmsi_list = self.dataframe_raw[self.mmsi_column_name].unique().tolist()
            # self.dataframe_raw = self.dataframe_raw.sort_values(by=[self.mmsi_column_name, self.timestamp_column_name])
            # self.dataframe_raw = self.dataframe_raw.reset_index(drop=True)
            # self.reset_dataframe_sampled()
            # mmsi_counter = 1
            # for mmsi in mmsi_list:
            #     condition = self.dataframe_raw[self.mmsi_column_name] == mmsi
            #     index_selection = self.dataframe_raw.index[condition]
            #     dataframe_raw_data_selection = self.dataframe_raw.iloc[index_selection]
            #     dataframe_raw_data_resampled = self.dataframe_raw.resample(rule=self.sampling_resolution, on=self.timestamp_column_name).last()
            #     if not dataframe_raw_data_resampled.empty:
            #         self.dataframe_sampled = pd.concat([self.dataframe_sampled, dataframe_raw_data_resampled], ignore_index=True)
                
            #     if mmsi_counter % 1e2 ==0 and mmsi_counter >=1e2:
            #         message = f'mmsi processed: {mmsi_counter}'
            #         logging.info(message)
            #     mmsi_counter += 1     
            self.write_stage_1_data_file()
            message = f'end stage 1 of file {file_path.name}'
            logging.info(message)
        message = 'end ETL stage 1 on all files'
        logging.info(message)
        self.clear_dataframe_raw()
        self.clear_dataframe_stage_1()
        return

    def get_vessel_type_all_files(self):
        '''
        Requests vessel types and performs join on each data file
        Input: self.stage_1_dir
        Output: self.vessel_type_info_path
        '''
        message = 'begin retrieval of vessel type on all data files'
        logging.info(message)
        files_list = list(self.stage_1_dir.glob('*'))      
        vessel_types_list = []
        total_mmsi_list = []  
        #retrieve mmsi from data files
        for file_path in files_list:
            message = f'begin mmsi retrieval on file {file_path.name}'
            logging.info(message)
            self.dataframe_stage_1 = pd.read_parquet(str(file_path), engine='pyarrow')
            mmsi_list_temp = self.dataframe_stage_1[self.mmsi_column_name].unique().tolist()
            total_mmsi_list.extend(mmsi_list_temp)
            message = f'end mmsi retrieval on file {file_path.name}'
            logging.info(message)
        total_mmsi_dict = {'mmsi':total_mmsi_list}
        dataframe_total_mmsi = pd.DataFrame.from_dict(total_mmsi_dict)
        dataframe_total_mmsi = dataframe_total_mmsi.drop_duplicates(subset='mmsi')
        dataframe_total_mmsi = dataframe_total_mmsi.reset_index(drop=True)
        total_mmsi_list = dataframe_total_mmsi['mmsi'].tolist()

        self.load_vessel_info()
        self.load_unknown_vessel_info()
        self.setup_vessel_type_retrieval_engine()
        #correlate against stored mmsi list
        if self.dataframe_mmsi_vessel_type is not None and self.dataframe_unknown_vessel_type is not None:
            stored_mmsi_list = self.dataframe_mmsi_vessel_type[self.dataframe_mmsi_column_name].tolist()
            vessel_types_list = self.dataframe_mmsi_vessel_type[self.dataframe_vessel_type_column_name].tolist()
            unknown_vessel_type_mmsi_list = self.dataframe_unknown_vessel_type[self.dataframe_unknown_vessel_type_mmsi_column_name].tolist()
            filtered_mmsi_list = self.difference_mmsi_list(mmsi_list_data_files=total_mmsi_list, mmsi_list_stored=stored_mmsi_list)
            filtered_mmsi_list = self.difference_mmsi_list(mmsi_list_data_files=filtered_mmsi_list, mmsi_list_stored=unknown_vessel_type_mmsi_list)
            message = f'{len(filtered_mmsi_list)} mmsi numbers remaining'
            logging.info(message)
            for index, mmsi in enumerate(filtered_mmsi_list):
                #retrieve vessel types from API
                vessel_type = self.vessel_type_retrieval_engine.get_vessel_type_single_mmsi(mmsi)
                if vessel_type == 'Unknown':
                    unknown_vessel_type_mmsi_list.append(mmsi)
                    dict_output = {self.dataframe_unknown_vessel_type_mmsi_column_name:unknown_vessel_type_mmsi_list}
                    self.dataframe_unknown_vessel_type = pd.DataFrame.from_dict(dict_output)
                    self.dataframe_unknown_vessel_type = self.dataframe_unknown_vessel_type.drop_duplicates(subset=[self.dataframe_unknown_vessel_type_mmsi_column_name], ignore_index=True)
                    self.save_unknown_vessel_info()
                    if len(unknown_vessel_type_mmsi_list) % self.uknown_vessel_type_logging_threshold == 0 and len(unknown_vessel_type_mmsi_list) >= self.uknown_vessel_type_logging_threshold:
                        message = f'retrieved {len(unknown_vessel_type_mmsi_list)} unknown vessels'
                        logging.info(message)
                else:
                    vessel_types_list.append(vessel_type)
                    stored_mmsi_list.append(mmsi)
                    if (index+1) % self.vessel_type_write_threshold ==0 and (index+1) >= self.vessel_type_write_threshold:
                        dict_output = {self.dataframe_mmsi_column_name:stored_mmsi_list ,self.dataframe_vessel_type_column_name:vessel_types_list}
                        self.dataframe_mmsi_vessel_type = pd.DataFrame.from_dict(dict_output)
                        self.dataframe_mmsi_vessel_type = self.dataframe_mmsi_vessel_type.drop_duplicates(subset=[self.dataframe_mmsi_column_name], ignore_index=True)
                        self.save_vessel_info()
                        message = f'retrieved {(index+1)} vessel types'
                        logging.info(message)
                    #last chunk
                    elif (index+1) >= (len(filtered_mmsi_list) - self.vessel_type_write_threshold):
                        dict_output = {self.dataframe_mmsi_column_name:stored_mmsi_list, self.dataframe_vessel_type_column_name:vessel_types_list}
                        self.dataframe_mmsi_vessel_type = self.dataframe_mmsi_vessel_type.drop_duplicates(subset=[self.dataframe_mmsi_column_name], ignore_index=True)
                        self.dataframe_mmsi_vessel_type = pd.DataFrame.from_dict(dict_output)
                        self.save_vessel_info()
                        message = f'retrieved {(index+1)} vessel types'
                        logging.info(message)
            message = f'retrieved {len(total_mmsi_list)} vessel types'
            logging.info(message)     
        #start from empty list of vessel types
        else:
            message = f'{len(total_mmsi_list)} mmsi numbers remaining'
            logging.info(message)
            save_mmsi_list = []
            vessel_types_list = []
            unknown_vessel_type_mmsi_list = []
            for index, mmsi in enumerate(total_mmsi_list):
                #retrieve vessel types from API
                vessel_type = self.vessel_type_retrieval_engine.get_vessel_type_single_mmsi(mmsi)
                if vessel_type == 'Unknown':
                    unknown_vessel_type_mmsi_list.append(mmsi)
                    dict_output = {self.dataframe_unknown_vessel_type_mmsi_column_name:unknown_vessel_type_mmsi_list}   
                    self.dataframe_unknown_vessel_type = pd.DataFrame.from_dict(dict_output)
                    self.dataframe_unknown_vessel_type = self.dataframe_unknown_vessel_type.drop_duplicates(subset=[self.dataframe_unknown_vessel_type_mmsi_column_name], ignore_index=True)
                    self.save_unknown_vessel_info()
                    if len(unknown_vessel_type_mmsi_list) % self.uknown_vessel_type_logging_threshold == 0 and len(unknown_vessel_type_mmsi_list) >= self.uknown_vessel_type_logging_threshold:
                        message = f'retrieved {len(unknown_vessel_type_mmsi_list)} unknown vessels'
                        logging.info(message)
                else: 
                    vessel_types_list.append(vessel_type)
                    save_mmsi_list.append(mmsi)
                    if (index+1) % self.vessel_type_write_threshold ==0 and (index+1) >= self.vessel_type_write_threshold:
                        dict_output = {self.dataframe_mmsi_column_name:save_mmsi_list ,self.dataframe_vessel_type_column_name:vessel_types_list}
                        self.dataframe_mmsi_vessel_type = pd.DataFrame.from_dict(dict_output)
                        self.dataframe_mmsi_vessel_type = self.dataframe_mmsi_vessel_type.drop_duplicates(subset=[self.dataframe_mmsi_column_name], ignore_index=True)
                        self.save_vessel_info()
                        message = f'retrieved {(index+1)} vessel types'
                        logging.info(message)
                    #last chunk
                    elif (index+1) >= (len(total_mmsi_list) - self.vessel_type_write_threshold):
                        dict_output = {self.dataframe_mmsi_column_name:save_mmsi_list ,self.dataframe_vessel_type_column_name:vessel_types_list}
                        self.dataframe_mmsi_vessel_type = pd.DataFrame.from_dict(dict_output)
                        self.dataframe_mmsi_vessel_type = self.dataframe_mmsi_vessel_type.drop_duplicates(subset=[self.dataframe_mmsi_column_name], ignore_index=True)
                        self.save_vessel_info()
                        message = f'retrieved {(index+1)} vessel types'
                        logging.info(message)
        message = 'end retrieval of vessel type on all data files'
        logging.info(message)
        self.clear_dataframe_stage_1()
        # self.clear_dataframe_stage_2()
        self.clear_vessel_type_retrieval_engine()
        return

    def ETL_stage_2(self):
        '''
        Perform join operation on AIS data with vesse types
        Source: self.stage_1_dir
        Destination: self.stage_2_dir
        '''
        message = 'begin ETL stage 2 on all data files'
        logging.info(message)
        files_list = list(self.stage_1_dir.glob('*')) 
        for file_path in files_list[0:1]:
            message = f'begin stage 2 of file {file_path.name}'
            logging.info(message)
            self.dataframe_stage_1 = pd.read_parquet(str(file_path), engine='pyarrow')
            sample_timestamp = self.dataframe_stage_1[self.timestamp_column_name].iloc[0]
            year = sample_timestamp.year
            month = sample_timestamp.month
            day = sample_timestamp.day

            self.stage_2_formatted_filename = f''

            # sample_timestamp_dt = datetime.strptime(sample_timestamp, self.sampled_timestamp_format)
            print(self.stage_2_formatted_filename)


            message = f'end stage 2 of file {file_path.name}'
            logging.info(message)
        
        message = 'end ETL stage 2 on all files'
        logging.info(message)
        self.clear_dataframe_stage_1()

        return

    def add_secondary_columns(self):
        '''
        Adds columns for dropping rows exceeding telemetry thresholds
        '''
        #sort by time column
        self.dataframe_raw = self.dataframe_raw.sort_values(by=[self.timestamp_column_name])

        #copy columns
        self.dataframe_raw['time_copy'] = self.dataframe_raw[self.timestamp_column_name]
        self.dataframe_raw['latitude_copy'] = self.dataframe_raw[self.latitude_column_name]
        self.dataframe_raw['longitude_copy'] = self.dataframe_raw[self.longitude_column_name]

        #shift copied columns
        self.dataframe_raw['time_copy'] = self.dataframe_raw['time_copy'].shift(1)
        self.dataframe_raw['latitude_copy'] = self.dataframe_raw['latitude_copy'].shift(1)
        self.dataframe_raw['longitude_copy'] = self.dataframe_raw['longitude_copy'].shift(1)

        #drop NaN from copied columns
        self.dataframe_raw = self.dataframe_raw.dropna(subset=['time_copy', 'latitude_copy', 'longitude_copy'])

        #compute delta columns
        self.dataframe_raw[self.delta_time_column_name] = self.dataframe_raw[self.timestamp_column_name] - self.dataframe_raw['time_copy']
        self.dataframe_raw[self.delta_longitude_column_name] = (self.dataframe_raw['latitude_copy'] - self.dataframe_raw[self.latitude_column_name]).abs()
        self.dataframe_raw[self.delta_latitude_column_name] = (self.dataframe_raw['longitude_copy'] - self.dataframe_raw[self.longitude_column_name]).abs()

        #drop copied columns
        self.dataframe_raw = self.dataframe_raw.drop(columns=['time_copy', 'latitude_copy', 'longitude_copy'])
        self.dataframe_raw = self.dataframe_raw.reset_index(drop=True)
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

    def reset_total_row_counter(self):
        '''
        Resets self.total_row_counter to 1
        '''
        self.total_row_counter = 1
        return

    def increment_total_row_counter(self):
        '''
        Increments self.total_row_counter by 1
        '''
        self.total_row_counter += 1
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

    def clear_dataframe_stage_1(self):
        '''
        Clears self.dataframe_stage_1 from memory
        '''
        self.dataframe_stage_1 = None
        return

    def clear_dataframe_stage_2(self):
        '''
        Clears self.dataframe_stage_2 from memory
        '''
        self.dataframe_stage_2 = None
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

    def clear_vessel_type_retrieval_engine(self):
        '''
        Clears self.vessel_type_retrieval_engine from memory
        '''
        self.vessel_type_retrieval_engine = None
        return

    def delete_raw_data_files(self):
        '''
        Deletes raw data files from disk
        '''
        files_list = list(self.raw_data_dir.glob('*'))
        for file in files_list:
            file.unlink()
        message = 'deleted raw data files'
        logging.info(message)
        return

    def delete_stage_1_data_files(self):
        '''
        Deletes stage 1 data files from disk
        '''
        files_list = list(self.stage_1_dir.glob('*'))
        for file in files_list:
            file.unlink()
        message = 'deleted stage 1 data files'
        logging.info(message)
        return

    def delete_stage_2_data_files(self):
        '''
        Deletes stage 2 data files from disk
        '''
        files_list = list(self.stage_2_dir.glob('*'))
        for file in files_list:
            file.unlink()
        message = 'deleted stage 2 data files'
        logging.info(message)
        return


if __name__ == "__main__":
    engine_object = model_engine()
    # engine_object.get_keyspace_credentials()
    # engine_object.setup_keyspace_connection()
    # engine_object.select_records()

    # engine_object.ETL_stage_1()
    # engine_object.get_vessel_type_all_files()

    # engine_object.load_s3_iam_role_credentials()
    # engine_object.get_s3_credentials()
    # engine_object.setup_s3_connection()

    engine_object.ETL_stage_2()


    # engine_object.delete_raw_data_files()
    # engine_object.delete_stage_1_data_files()
    # engine_object.delete_stage_2_data_files()
    





