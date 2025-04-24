import pandas as pd
agencyMaster_df = pd.read_csv(r"AgencyMaster.csv")
empMaster_df = pd.read_csv(r"EmpMaster.csv")
nycpayroll_2020_df = pd.read_csv(r"nycpayroll_2020.csv")
nycpayroll_2021_df = pd.read_csv(r"nycpayroll_2021.csv")
titlemaster_df = pd.read_csv(r"TitleMaster.csv")
titlemaster_df = titlemaster_df.dropna()
titlemaster_df.index.name = 'TitlemasterID'
titlemaster_df = titlemaster_df.reset_index()
date_2020_dim = nycpayroll_2020_df[['FiscalYear', 'AgencyStartDate']].copy().drop_duplicates().reset_index(drop=True)
date_2020_dim.index.name = 'nycpayrollID'
date_2020_dim = date_2020_dim.reset_index()
date_2020_dim['AgencyStartDate'] = pd.to_datetime(date_2020_dim['AgencyStartDate'], errors='coerce')
nycpayroll_2020_df['AgencyStartDate'] = pd.to_datetime(nycpayroll_2020_df['AgencyStartDate'], errors='coerce')
date_dim_2021 = nycpayroll_2021_df[['FiscalYear', 'AgencyStartDate']].copy().drop_duplicates().reset_index(drop=True)
date_dim_2021.index.name = 'nycpayrollID'
date_dim_2021 = date_dim_2021.reset_index()
date_dim_2021['AgencyStartDate'] = pd.to_datetime(date_dim_2021['AgencyStartDate'], errors='coerce')
nypayroll_fact = nycpayroll_2020_df.merge(agencyMaster_df, on = ['AgencyID', 'AgencyName'], how = 'left')\
    .merge(empMaster_df, on = ['EmployeeID', 'LastName', 'FirstName'], how = 'left')\
    .merge(titlemaster_df, on = ['TitleCode', 'TitleDescription'], how = 'left')\
    .merge(date_dim_2021, on = ['FiscalYear', 'AgencyStartDate'], how = 'left')\
    .merge(date_2020_dim, on = ['FiscalYear', 'AgencyStartDate'], how = 'left')

# Now add your custom ID column if needed
nypayroll_fact['nycpayrollID'] = range(1, len(nypayroll_fact) + 1)

# Then filter your columns
nypayroll_fact = nypayroll_fact[['nycpayrollID', 'AgencyID', 'EmployeeID', 'TitlemasterID', 'PayrollNumber',
                                 'WorkLocationBorough', 'LeaveStatusasofJune30', 'BaseSalary', 'PayBasis',
                                 'RegularHours', 'RegularGrossPaid', 'OTHours', 'TotalOTPaid', 'TotalOtherPay']]
nypayroll_fact.to_csv(r"C:\Users\USER\OneDrive\Desktop\10Alytics\Capstone\nypayroll_fact.csv")

print('files have been loaded temporarily into local machine')

import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient 
from dotenv import load_dotenv

load_dotenv()
connection_str = os.getenv('CONNECTION_STR')
blob_service_client = BlobServiceClient.from_connection_string(connection_str)

container_name= os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)

def upload_df_to_blob_as_parquent(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type='BlockBlob', overwrite=True)
    print (f'{blob_name} upload to Blob storage successfully')

upload_df_to_blob_as_parquent(agencyMaster_df, container_client, 'rawdata/agencymaster.parquent')
upload_df_to_blob_as_parquent(empMaster_df, container_client, 'rawdata/empMaster_df.parquent')
upload_df_to_blob_as_parquent(nycpayroll_2020_df, container_client, 'rawdata/nycpayroll_2020_df.parquent')
upload_df_to_blob_as_parquent(nycpayroll_2021_df, container_client, 'rawdata/nycpayroll_2021_df.parquent')
upload_df_to_blob_as_parquent(titlemaster_df, container_client, 'rawdata/titlemaster_df.parquent')
upload_df_to_blob_as_parquent(nypayroll_fact, container_client, 'rawdata/nypayroll_fact.parquent')