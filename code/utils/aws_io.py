import s3fs
import pickle

fs = s3fs.S3FileSystem(anon=False)
s3_results_root = "aind-behavior-data/foraging_nwb_bonsai_processed/v2"
local_results_root = "/root/capsule/results"

def upload_s3_fig(fig, job_hash, filename, if_save_local=True):
    with fs.open(f"{s3_results_root}/{job_hash}/{filename}", "wb") as f:
        fig.savefig(f)
    
    if if_save_local:
        fig.savefig(f"{local_results_root}/{job_hash}/{filename}")

def upload_s3_pkl(obj, job_hash, filename, if_save_local=True):
    with fs.open(f"{s3_results_root}/{job_hash}/{filename}", "wb") as f:
        pickle.dump(obj, f)
        
    if if_save_local:
        with open(f"{local_results_root}/{job_hash}/{filename}", "wb") as f:
            pickle.dump(obj, f)
