""" top level run script """

import json
import glob
import logging
import importlib

import multiprocessing as mp

from utils.capture_logs import capture_logs

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('run.log')])
logger = logging.getLogger()  # Use root logger to capture all logs (including logs from imported modules)

ANALYSIS_MAPPER = {
    # Mapping of analysis name to package name under analysis_wrappers
    "MLE fitting": "mle_fitting",
}

def _run_one_job(job_file, parallel_inside_job):
    with open(job_file) as f:
        job_dict = json.load(f)
    
    # Get analysis function
    package_name = ANALYSIS_MAPPER[job_dict["analysis_spec"]["analysis_name"]]
    analysis_fun = importlib.import_module(f"analysis_wrappers.{package_name}").wrapper_main
    
    # Trigger analysis
    logger.info("")
    logger.info(f"Running {job_dict['analysis_spec']['analysis_name']} for {job_dict['nwb_name']}")
    logger.info(f"Job hash: {job_dict['job_hash']}")
    try:
        status = capture_logs(logger)(analysis_fun)(job_dict, parallel_inside_job)
        logger.info(f"Job {job_dict['job_hash']} completed with status: {status}")
    except Exception as e:
        logger.error(f"Job {job_dict['job_hash']} failed with error: {e}")


def run(parallel_on_jobs=False):
    """
    Parameters
    -----
    parallel_on_jobs, boolean, Optional (by default, True)
        if true, will call multiprocessing on the level of job
        else, process each job sequentially, but go parallel inside each job (e.g., DE workers)
    """
    # Discover all job json in /root/capsule/data
    job_files = glob.glob("/root/capsule/data/*.json")

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

if __name__ == "__main__": 

    import argparse

    # create a parser object
    parser = argparse.ArgumentParser()
    
    # add the corresponding parameters
    parser.add_argument('--parallel_on_jobs', dest='parallel_on_jobs')
    
    # return the data in the object and save in args
    args = parser.parse_args()

    # retrive the arguments
    parallel_on_jobs = bool(int(args.parallel_on_jobs or "0"))  # Default 0
     
    run(parallel_on_jobs=parallel_on_jobs)
