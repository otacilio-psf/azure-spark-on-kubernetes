import requests
import zipfile
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential

class Ingestion:

    def __init__(self, connection, container):
        credential = ClientSecretCredential(connection['tenant_id'],
                                            connection['client_id'],
                                            connection['client_secret'])
        account_url = f"https://{connection['storage_account_name']}.dfs.core.windows.net"
        self._adls = DataLakeServiceClient(account_url=account_url, credential=credential)
        
        self._fs = self._adls.get_file_system_client(file_system=container)
    
    def _upload_file(self, local_file_path, remote_file_path):
        file_client = self._fs.get_file_client(remote_file_path)
        with open(local_file_path,'r') as local_file:
            file_contents = local_file.read()
            file_client.upload_data(file_contents, overwrite=True)
    
    def _extract(self):
        os.makedirs("./tmp", exist_ok=True)
        os.makedirs("./data",exist_ok=True)

        # download data
        print("Downloading the data...")
        source_url = 'https://download.inep.gov.br/microdados/Enade_Microdados/microdados_Enade_2017_portal_2018.10.09.zip'
        r = requests.get(source_url, stream=True)
        if r.status_code == requests.codes.OK:
            with open("./tmp/microdados_Enade_2017.zip", "wb") as new_file:
                for part in r.iter_content(chunk_size=1024):
                    new_file.write(part)
        else:
            r.raise_for_status()
        
        # unzip data
        print("Unzip the data...")
        with zipfile.ZipFile("./tmp/microdados_Enade_2017.zip", 'r') as zip_data:
            zip_data.extract("3.DADOS/MICRODADOS_ENADE_2017.txt", "./data")
        
    def _load(self):
        file_path = "./data/3.DADOS/"
        file_name = "MICRODADOS_ENADE_2017.txt"

        # upload file
        print(f"Upload {file_name} to Lake")
        self._upload_file(file_path+file_name, 'bronze/enade/MICRODADOS_ENADE_2017.txt')

    def start(self):
        self._extract()
        self._load()
        print("Ingestion completed")

if __name__ == "__main__":

    connection = {
        'tenant_id': os.getenv('AZURE_TENANT_ID'),
        'client_id': os.getenv('AZURE_CLIENT_ID'),
        'client_secret': os.getenv('AZURE_CLIENT_SECRET'),
        'storage_account_name': 'datalakeigtibootcamp'
    }

    container = 'datalake'

    Ingestion(connection, container).start()
