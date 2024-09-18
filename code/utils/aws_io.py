import s3fs
import pickle
import json
import logging

from run_capsule import S3_RESULTS_ROOT, LOCAL_RESULTS_ROOT

fs = s3fs.S3FileSystem(anon=False)

logger = logging.getLogger(__name__)

def upload_s3_fig(job_hash, filename, fig, if_save_local=True):
    with fs.open(f"{S3_RESULTS_ROOT}/{job_hash}/{filename}", "wb") as f:
        fig.savefig(f)
        logger.info(f"Uploaded {filename} to S3")
    
    if if_save_local:
        fig.savefig(f"{LOCAL_RESULTS_ROOT}/{job_hash}/{filename}")
        logger.info(f"Saved {filename} locally")

def upload_s3_pkl(job_hash, filename, obj, if_save_local=True):
    with fs.open(f"{S3_RESULTS_ROOT}/{job_hash}/{filename}", "wb") as f:
        pickle.dump(obj, f)
        logger.info(f"Uploaded {filename} to S3")
        
    if if_save_local:
        with open(f"{LOCAL_RESULTS_ROOT}/{job_hash}/{filename}", "wb") as f:
            pickle.dump(obj, f)
            logger.info(f"Saved {filename} locally")

def upload_s3_json(job_hash, filename, obj, if_save_local=True):
    with fs.open(f"{S3_RESULTS_ROOT}/{job_hash}/{filename}", "w") as f:
        json.dump(obj, f, indent=4)
        logger.info(f"Uploaded {filename} to S3")
        
    if if_save_local:
        with open(f"{LOCAL_RESULTS_ROOT}/{job_hash}/{filename}", "w") as f:
            json.dump(obj, f, indent=4)
            logger.info(f"Saved {filename} locally")