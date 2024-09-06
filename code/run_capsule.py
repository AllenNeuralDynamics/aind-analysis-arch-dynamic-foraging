""" top level run script """

import os
import json
import glob
import logging

import multiprocessing as mp

from analysis import (
    fit_mle_one_session,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


JOB_MAPPER = {
    "MLE fitting": fit_mle_one_session,
}

def _run_one_job(job_file):
    with open(job_file) as f:
        job_dict = json.load(f)
    
    # Trigger job
    JOB_MAPPER[job_dict["job_spec"]["analysis_name"]](job_dict)


def run():
    # Discover all job json in /root/capsule/data
    job_files = glob.glob("/root/capsule/data/*.json")

    # For each job json, run the corresponding job using multiprocessing
    logger.info(f"Running {len(job_files)} jobs")
    pool = mp.Pool(mp.cpu_count())
    results = [pool.apply_async(_run_one_job, args=(job_file,)) for job_file in job_files]
    _ = [r.get() for r in results]
    pool.close()
    pool.join()
    
    # [_run_one_job(job_file) for job_file in job_files]

if __name__ == "__main__": run()