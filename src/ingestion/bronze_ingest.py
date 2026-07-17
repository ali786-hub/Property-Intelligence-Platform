import polars as pl 
import os
from src.helper_files.lineage import LineageTracker 
import hashlib
from dotenv import load_dotenv
import glob
load_dotenv()

LANDING_ZONE = os.getenv("LANDING_ZONE")
BRONZE_ZONE = os.getenv("BRONZE_ZONE")

def calculate_file_hash(file_path:str) ->str:
    sha256 = hashlib.sha256()
    with open(file_path,"rb") as file:
        while True:
            chunk= file.read(8192) 
            if (not chunk):
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def update_lineage_table(batch_limit:int = 0):
    with LineageTracker('BRONZE') as tracker:
        
        file_list = [file for file in glob.glob(LANDING_ZONE + "*.csv")]
        
        for file in file_list:
            if (not tracker.is_file_processed(file_hash)
                hashed=calculate_hash(os.path.join(LANDING_ZONE,file))
                hashed_list=[]
                hashed_list.append(hashed)
                

                
        


    

