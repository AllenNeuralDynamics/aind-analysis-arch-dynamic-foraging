""" top level run script """

import json
import glob
import logging
import importlib
import traceback
import os

import multiprocessing as mp

from utils.capture_logs import capture_logs
from utils.docDB_io import update_job_manager, insert_result_to_docDB_ssh
from utils.aws_io import (
    upload_s3_fig,
    upload_s3_pkl,
    upload_s3_json,
    S3_RESULTS_ROOT,
    LOCAL_RESULTS_ROOT,
)

# Get script directory
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f'{SCRIPT_DIR}/../results/run.log'),
                              logging.StreamHandler()])
logger = logging.getLogger()  # Use root logger to capture all logs (including logs from imported modules)

ANALYSIS_MAPPER = {
    # Mapping of analysis name to package name under analysis_wrappers
    "MLE fitting": "mle_fitting",
}


def upload_results(job_hash, results):
    """
    Upload results to S3 and docDB

    Parameters
    ----------
    job_hash : _type_
        _description_
    results : dict
        Dictionary containing the result of the analysis.
        "status": str, "success" or others
        "upload_figs_s3": dict, figures to upload to s3, {"file_name": fig object}
        "upload_pkls_s3": dict, pkl files to upload to s3, {"pkl_name": pkl object}
        "upload_record_docDB": dict, bson-compatible record to upload to docDB

    """
    if "skipped" in results["status"]:
        return {
            "docDB_id": None,
            "collection_name": None,
            "s3_location": None,
        }

    # Upload figures
    for fig_name, fig in results.get("upload_figs_s3", {}).items():
        upload_s3_fig(job_hash, fig_name, fig, if_save_local=True)

    # Upload pkl files
    for pkl_name, pkl in results.get("upload_pkls_s3", {}).items():
        upload_s3_pkl(job_hash, pkl_name, pkl, if_save_local=True)

    upload_status = {"s3_location": f"s3://{S3_RESULTS_ROOT}/{job_hash}"}

    # Upload record to docDB
    upload_record_docDB = results.get("upload_record_docDB", {})
    upload_status.update(
        insert_result_to_docDB_ssh(
            result_dict=upload_record_docDB, 
            collection_name="mle_fitting"
        )
    )  # Note that this will add _id automatically to upload_record_docDB
    # Save a copy of docDB record to s3 and local
    upload_s3_json(
        job_hash=job_hash,
        filename="docDB_record.json",
        dict=upload_record_docDB,
        if_save_local=True,
    )
    return upload_status

def _run_one_job(job_file, parallel_inside_job):
    with open(job_file) as f:
        job_dict = json.load(f)

    job_hash = job_dict["job_hash"]

    # Update status to "running" in job manager DB
    update_job_manager(job_hash=job_hash, update_dict={"status": "running"})

    # Get analysis function
    package_name = ANALYSIS_MAPPER[job_dict["analysis_spec"]["analysis_name"]]
    analysis_fun = importlib.import_module(f"analysis_wrappers.{package_name}").wrapper_main

    try:
        # -- Trigger analysis --
        logger.info("")
        logger.info(f"Running {job_dict['analysis_spec']['analysis_name']} for {job_dict['nwb_name']}")
        logger.info(f"Job hash: {job_hash}")
        
        analysis_results = capture_logs(logger)(analysis_fun)(job_dict, parallel_inside_job)
        results, log = analysis_results["result"], analysis_results["logs"]
        logger.info(
            f"Job {job_hash} completed with status: {results['status']}"
        )
        print(f"Job {job_hash} completed with status: {results['status']}")  # Print to console of CO pipeline run

        # -- Upload results --
        upload_response = capture_logs(logger)(upload_results)(job_hash, results)
        upload_status, upload_log = upload_response["result"], upload_response["logs"]
        log += upload_log  # Also add log during upload
        
        # -- Update job manager DB with log and status --
        update_job_manager(
            job_hash,
            update_dict={
                "status": results["status"],
                "docDB_id": upload_status["docDB_id"],
                "collection_name": upload_status["collection_name"],
                "s3_location": upload_status["s3_location"],
                "log": log,
            },
        )
    except Exception as e:  # Unhandled exception
        logger.error(f"Job {job_hash} failed with unhandled exception: {e}")
        logger.error(traceback.format_exc())  # Logs the full traceback
        update_job_manager(
            job_hash,
            update_dict={
                "status": "failed due to unhandled exception",
                "docDB_id": None,
                "collection_name": None,
                "log": log,
            },
        )


def run(parallel_on_jobs=False, debug_mode=True):
    """
    Parameters
    -----
    parallel_on_jobs, boolean, Optional (by default, True)
        if true, will call multiprocessing on the level of job
        else, process each job sequentially, but go parallel inside each job (e.g., DE workers)
    """
    # Discover all job json in /root/capsule/data
    job_files = glob.glob(f"{SCRIPT_DIR}/../data/jobs/**/*.json", recursive=True)

    if debug_mode:
        job_files = job_files[:1]

    # For each job json, run the corresponding job using multiprocessing
    if parallel_on_jobs:
        logger.info(f"\n\nRunning {len(job_files)} jobs, parallel on jobs...")
        pool = mp.Pool(mp.cpu_count())
        results = [pool.apply_async(_run_one_job, args=(job_file, False)) for job_file in job_files]
        _ = [r.get() for r in results]
        pool.close()
        pool.join()
    else:
        logger.info(f"\n\nRunning {len(job_files)} jobs, serial on jobs...")
        [_run_one_job(job_file, parallel_inside_job=True) for job_file in job_files]
    logger.info(f"All done!")

if __name__ == "__main__": 

    import argparse

    # create a parser object
    parser = argparse.ArgumentParser()
    
    # add the corresponding parameters
    parser.add_argument('--parallel_on_jobs', dest='parallel_on_jobs')
    parser.add_argument('--debug_mode', dest='debug_mode')
    
    # return the data in the object and save in args
    args = parser.parse_args()

    # retrive the arguments
    parallel_on_jobs = bool(int(args.parallel_on_jobs or "0"))  # Default 0
    debug_mode = bool(int(args.debug_mode or "1"))  # Default 1

    run(parallel_on_jobs=parallel_on_jobs, debug_mode=debug_mode)
