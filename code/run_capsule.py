""" top level run script """

import json
import glob
import logging

import multiprocessing as mp

from analysis import (
    fit_mle_one_session,
)

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('run.log')])
logger = logging.getLogger(__name__)

JOB_MAPPER = {
    "MLE fitting": fit_mle_one_session,
}

def _run_one_job(job_file, parallel_inside_job):
    with open(job_file) as f:
        job_dict = json.load(f)
    
    # Trigger job
    JOB_MAPPER[job_dict["job_spec"]["analysis_name"]](job_dict, parallel_inside_job)


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
        logger.info(f"Running {len(job_files)} jobs, parallel on jobs...")
        pool = mp.Pool(mp.cpu_count())
        results = [pool.apply_async(_run_one_job, args=(job_file, False)) for job_file in job_files]
        _ = [r.get() for r in results]
        pool.close()
        pool.join()
    else:
        logger.info(f"Running {len(job_files)} jobs, serial on jobs...")
        [_run_one_job(job_file, parallel_inside_job=True) for job_file in job_files]

if __name__ == "__main__": 

    import argparse

    # create a parser object
    parser = argparse.ArgumentParser()
    
    # add the corresponding parameters
    parser.add_argument('--parallel_on_jobs', dest='parallel_on_jobs')
    
    # return the data in the object and save in args
    args = parser.parse_args()
    print(args)

    # retrive the arguments
    parallel_on_jobs = bool(int(args.parallel_on_jobs or "0"))  # Default 0
     
    run(parallel_on_jobs=parallel_on_jobs)
