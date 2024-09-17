import s3fs
import pickle
import json
import logging

fs = s3fs.S3FileSystem(anon=False)
s3_results_root = "aind-behavior-data/foraging_nwb_bonsai_processed/v2"
local_results_root = "/root/capsule/results"

logger = logging.getLogger(__name__)

def upload_s3_fig(fig, job_hash, filename, if_save_local=True):
    with fs.open(f"{s3_results_root}/{job_hash}/{filename}", "wb") as f:
        fig.savefig(f)
        logger.info(f"Uploaded {filename} to S3")
    
    if if_save_local:
        fig.savefig(f"{local_results_root}/{job_hash}/{filename}")
        logger.info(f"Saved {filename} locally")

def upload_s3_pkl(obj, job_hash, filename, if_save_local=True):
    with fs.open(f"{s3_results_root}/{job_hash}/{filename}", "wb") as f:
        pickle.dump(obj, f)
        logger.info(f"Uploaded {filename} to S3")
        
    if if_save_local:
        with open(f"{local_results_root}/{job_hash}/{filename}", "wb") as f:
            pickle.dump(obj, f)
            logger.info(f"Saved {filename} locally")

def upload_s3_json(obj, job_hash, filename, if_save_local=True):
    with fs.open(f"{s3_results_root}/{job_hash}/{filename}", "w") as f:
        json.dump(obj, f, indent=4)
        logger.info(f"Uploaded {filename} to S3")
        
    if if_save_local:
        with open(f"{local_results_root}/{job_hash}/{filename}", "w") as f:
            json.dump(obj, f, indent=4)
            logger.info(f"Saved {filename} locally")