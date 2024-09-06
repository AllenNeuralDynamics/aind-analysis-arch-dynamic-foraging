import logging
import os

import numpy as np
import multiprocessing as mp

from nwb_io import get_history_from_nwb
from aind_dynamic_foraging_models.generative_model import ForagerCollection

logger = logging.getLogger(__name__)

def fit_mle_one_session(job_dict, parallel_inside_job=False):
    job_hash = job_dict["job_hash"]
    nwb_name = job_dict["nwb_name"]
    analysis_args = job_dict["job_spec"]["analysis_args"]

    # Overwrite DE_workers
    if parallel_inside_job:
        analysis_args["fit_kwargs"]["DE_kwargs"]["workers"] = mp.cpu_count()
    else:
        analysis_args["fit_kwargs"]["DE_kwargs"]["workers"] = 1
    
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
    result_dir = f"/root/capsule/results/{job_hash}"
    os.makedirs(result_dir, exist_ok=True)
    
    fig_fitting, axes = forager.plot_fitted_session(if_plot_latent=True)
    fig_fitting.savefig(f"{result_dir}/fitted.png")