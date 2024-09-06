""" top level run script """

import os
import json
import glob
import logging

import multiprocessing as mp
import numpy as np

from nwb_io import get_history_from_nwb
from aind_dynamic_foraging_models.generative_model import ForagerCollection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def fit_mle_one_session(job_dict):
    job_hash = job_dict["job_hash"]
    nwb_name = job_dict["nwb_name"]
    analysis_args = job_dict["job_spec"]["analysis_args"]
    
    logger.info(f"MLE fitting for {nwb_name} with {analysis_args['agent_class']}")
    
    # Load data
    session_id = job_dict["nwb_name"].replace(".nwb", "")

    (
        baiting,
        choice_history,
        reward_history,
        p_reward,
        autowater_offered,
        random_number,
    ) = get_history_from_nwb(f"/root/capsule/data/{session_id}.nwb")

    # Remove NaNs
    # TODO: handle in model fitting
    ignored = np.isnan(choice_history)
    choice_history = choice_history[~ignored]
    reward_history = reward_history[~ignored].to_numpy()
    p_reward = [p[~ignored] for p in p_reward]
    
    # Initialize model
    forager = ForagerCollection().get_forager(
       agent_class_name=analysis_args["agent_class"],
       agent_kwargs=analysis_args["agent_kwargs"],
    )
    forager.fit(
       choice_history,
       reward_history,
       **analysis_args["fit_kwargs"],
    )
    
    # Saving results
    os.makedirs(f"/root/capsule/results/{job_hash}", exist_ok=True)
    
    fig_fitting, axes = forager.plot_fitted_session(if_plot_latent=True)
    fig_fitting.savefig(f"/root/capsule/results/fitted.png")


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