import boto3
from botocore.exceptions import ClientError
from datetime import date
import joblib
import logging
import pandas as pd
from pathlib import Path
from sklearn import linear_model


class model_engine():
    '''
    '''
    def __init__(self, data_dir:Path, config_dir:Path):
        '''
        '''
        self.data_dir = Path('data')
        self.config_dir = Path('config')

        self.stage_2_folder = 'stage_2'
        self.log_folder = 'log'
        self.credentials_folder = 'credentials'
        self.model_folder = 'model'

        self.aws_user_credentials_filename = 'ingest-engine-user-1_accessKeys.csv'
        self.iam_s3_role_credentials_filename = 's3_iam_role_credentials.csv'
        self.s3_configuration_filename = 's3_configuration.csv'
        self.model_filename = 'model.joblib'


        self.credentials_dir = self.config_dir / self.credentials_folder
        self.credentials_dir.mkdir(parents=True, exist_ok=True)

        self.aws_user_credentials_path = self.credentials_dir / self.aws_user_credentials_filename
        self.iam_s3_role_credentials_path = self.credentials_dir / self.iam_s3_role_credentials_filename
        self.s3_configuration_path = self.credentials_dir / self.s3_configuration_filename

        self.model_dir = self.config_dir / self.model_folder
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.model_path = self.model_dir / self.model_filename
        
        self.log_dir = self.config_dir / self.log_folder
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_filename = f'{date.today()}.log'
        self.log_path = self.log_dir / log_filename



        self.aws_user_credentials_dataframe = None
        self.iam_s3_role_credentials_dataframe = None
        self.s3_configuration_dataframe = None

        self.aws_user_access_key_id = None
        self.aws_user_secret_access_key = None
        self.iam_s3_role_arn = None

        self.aws_default_region = None
        self.temporary_credentials_access_key_id = None
        self.temporary_credentials_secret_key = None
        self.temporary_credentials_session_token = None
        
        self.s3_bucket_name = None
        self.dataframe_stage_2 = None

        self.dataframe_mmsi_column_name = 'mmsi'
        self.dataframe_vessel_type_column_name = 'vessel_type'

        self.columns_list = [self.latitude_column_name, self.longitude_column_name, self.mmsi_column_name, self.timestamp_column_name]
        self.chunksize = 1e6
        self.sampled_timestamp_format = '%Y-%m-%d %H:%M:%S'

        #boto3 objects
        self.aws_sts_client = None
        self.aws_s3_session = None
        self.s3_resource_client = None

        #S3 file parameters
        self.key_data_folder = 'data'
        self.key_prefix_year = None
        self.key_prefix_month = None
        self.key_prefix_day = None

        #model parameters
        self.model_object = None
        self.model_s3_key = None

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

    def load_s3_iam_role_credentials(self):
        '''
        Loads ARN for IAM role to interact with S3
        '''
        self.iam_s3_role_credentials_dataframe = pd.read_csv(self.iam_s3_role_credentials_path)
        self.iam_s3_role_arn = self.iam_s3_role_credentials_dataframe['ARN'][0]
        return

    def load_s3_configuration(self):
        '''
        Loads configuration parameters for uploading files to S3
        '''
        self.s3_configuration_dataframe = pd.read_csv(str(self.s3_configuration_path))
        self.aws_default_region = self.s3_configuration_dataframe['region'][0]
        self.s3_bucket_name = self.s3_configuration_dataframe['bucket'][0]
        self.model_s3_key = self.s3_configuration_dataframe['model_s3_key'][0]
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
        self.s3_resource_client = self.aws_s3_session.client(service_name='s3', region_name=self.aws_default_region, aws_access_key_id=self.temporary_credentials_access_key_id, aws_secret_access_key=self.temporary_credentials_secret_key, aws_session_token=self.temporary_credentials_session_token)

        return

    def load_model(self):
        '''
        Loads model from disk
        Input: self.model_path
        Output: self.model_object
        '''
        if self.model_path.exists():
            self.model_object = joblib.load(self.model_path)
            message = 'model loaded from disk'
            logging.info(message)
        else:
            message = 'model failed to load from disk'
            logging.info(message)
        return
    
    def save_model(self):
        '''
        Saves self.model_object to disk
        Input: self.model_object
        Output: self.model_path
        '''
        if self.model_object is not None:
            joblib.dump(self.model_object, self.model_path)
            message = 'model saved to disk'
            logging.info(message)
        else:
            message = 'model failed to save to disk'
            logging.info(message)
        return

    def initialize_model(self):
        '''
        Instantiates self.model_object with new classifier
        '''
        self.model_object = linear_model.SGDClassifier()
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

    
    