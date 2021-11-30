import requests
import zipfile
import os
from azure.identity import ClientSecretCredential
from azurecloudhandler.datalake_gen2 import ADSL2DataLoad

credential = ClientSecretCredential(
    client_id = os.getenv("AZURE_CLIENT_ID"),
    client_secret = os.getenv("AZURE_CLIENT_SECRET"),
    tenant_id = os.getenv("AZURE_TENANT_ID")
)

class Ingestion:

    def __init__(self, source_url, local_file_path, remote_file_path, inside_zip_path, file_name, storage_account_name, file_system_name):
        self._source_url = source_url
        self._local_file_path = local_file_path
        self._remote_file_path = remote_file_path
        self._inside_zip_path = inside_zip_path
        self._file_name = file_name
        self._dload = ADSL2DataLoad(
            storage_account_name = storage_account_name,
            file_system_name = file_system_name,
            credential = credential
            )
    
    def _extract(self):
        os.makedirs("./tmp", exist_ok=True)
        os.makedirs("./data",exist_ok=True)

        # download data
        print("Downloading the data...")
        source_url = self._source_url
        session = requests.Session()
        r = session.get(source_url, stream=True)
        if r.status_code == requests.codes.OK:
            with open("./tmp/temp_file.zip", "wb") as new_file:
                for part in r.iter_content(chunk_size=1024*1024*5):
                    new_file.write(part)
        else:
            r.raise_for_status()
        
        # unzip data
        print("Unzip the data...")
        with zipfile.ZipFile("./tmp/temp_file.zip", 'r') as zip_data:
            zip_data.extract(self._inside_zip_path, "./data")
        
    def _load(self):
        
        # upload file
        print(f"Upload {self._file_name} to Lake")
        self._dload.upload_file(
            self._local_file_path+self._file_name,
            self._remote_file_path+self._file_name
            )

    def start(self):
        self._extract()
        self._load()
        print("Ingestion completed")

if __name__ == "__main__":

    source_url = 'https://download.inep.gov.br/microdados/Enade_Microdados/microdados_Enade_2017_portal_2018.10.09.zip'
    local_file_path = "./data/3.DADOS/"
    remote_file_path = "bronze/enade/"
    inside_zip_path = "3.DADOS/MICRODADOS_ENADE_2017.txt"
    file_name = "MICRODADOS_ENADE_2017.txt"
    storage_account_name = os.getenv("STG_ACC_NAME")
    file_system_name = os.getenv("LAKE_NAME")

    ings = Ingestion(
        source_url,
        local_file_path,
        remote_file_path,
        inside_zip_path,
        file_name,
        storage_account_name,
        file_system_name
        )
    
    ings.start()
