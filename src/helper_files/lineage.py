# this is the lineage tracker that will help us to track the lineage of the files that are processed by the pipeline
# the database file contains essentials for db connections which includes connection pool and other things
import os
import sys

# Dynamically add root project directory so we can import 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.helper_files.database import DBConnection
# First we define the constructor to create instances of this helper funciton LIneage tracker
class LineageTracker:

    def __init__ (self, layer:str, airflow_run_id:str=None,flush_threshold:int=100):

        self.layer = layer.upper()
        self.airflow_run_id = airflow_run_id
        self.flush_threshold = flush_threshold
        self.processed_hashes = set()
        self.buffer = []
# this helper will help us to enter connection to db and fetch all the files that have been processed successfully 
    def __enter__ (self):
        with DBConnection() as conn:
            with conn.cursor() as cur:
                cur.execute( "SELECT file_hash FROM file_lineage WHERE status='SUCCESS' AND layer=%s",(self.layer,))
                self.processed_hashes= {row[0] for row in cur.fetchall()}
        return self
        # a quick checker for individual files to see whether file is processed or not 
    def is_file_processed(self,file_hash: str) ->bool:
        return file_hash in self.processed_hashes
    # a function that will exit the connection to db and return back all the buffered results 
    def __exit__ (self,exc_type, exc_val, exc_tb):
        if exc_val is None:
            self.flush()
        return False

    # current
    # a function that will log the result to the buffer and flush it to the database if the buffer is full 
    def log_result(self, file_hash, file_name, status, row_count=None, file_size_bytes=None, error_message=None):
        record = (file_hash, self.layer, self.airflow_run_id, file_name, status, row_count, file_size_bytes, error_message ,)
        self.buffer.append(record)
        if (len(self.buffer) >=self.flush_threshold):
            self.flush()
    # this will flush the buffered results to the database 
    def flush(self):
        if not self.buffer:
            return

        with DBConnection() as conn:
            with conn.cursor() as cur:
                cur.executemany("""INSERT INTO file_lineage(file_hash, layer, airflow_run_id,file_name, status, row_count, file_size_bytes, error_message) VALUES (%s, %s, %s, %s, %s, %s, %s,%s) ON CONFLICT (file_hash,layer) DO UPDATE SET file_name = EXCLUDED.file_name, airflow_run_id =EXCLUDED.airflow_run_id,status =EXCLUDED.status, row_count= EXCLUDED.row_count, file_size_bytes=EXCLUDED.file_size_bytes, error_message=EXCLUDED.error_message, retry_count= CASE WHEN EXCLUDED.status= 'FAILED' THEN file_lineage.retry_count + 1 ELSE file_lineage.retry_count END """,self.buffer)
        
        self.buffer.clear()