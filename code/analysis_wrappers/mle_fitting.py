import logging
import os
import time
import pkg_resources
from datetime import datetime

import numpy as np
import multiprocessing as mp

from utils.nwb_io import get_history_from_nwb
from utils.docDB_io import insert_docDB_ssh
from utils.aws_io import upload_s3_fig, upload_s3_pkl, upload_s3_json
from aind_dynamic_foraging_models.generative_model import ForagerCollection

logger = logging.getLogger(__name__)

def wrapper_main(job_dict, parallel_inside_job=False):
    """Main entrance of this analysis"""
    start_time = time.time()

    job_hash = job_dict["job_hash"]
    nwb_name = job_dict["nwb_name"]
    analysis_args = job_dict["analysis_spec"]["analysis_args"]

    # Overwrite DE_workers
    if parallel_inside_job:
        analysis_args["fit_kwargs"]["DE_kwargs"]["workers"] = mp.cpu_count()
    else:
        analysis_args["fit_kwargs"]["DE_kwargs"]["workers"] = 1

    logger.info(f"MLE fitting for {nwb_name} with {analysis_args['agent_class']}")

    # -- Load data --
    session_id = job_dict["nwb_name"].replace(".nwb", "")

    (
        baiting,
        choice_history,
        reward_history,
        _,
        autowater_offered,
        random_number,
    ) = get_history_from_nwb(f"/root/capsule/data/foraging_nwb_bonsai/{session_id}.nwb")

    # Remove NaNs
    ignored = np.isnan(choice_history)
    choice_history = choice_history[~ignored]
    reward_history = reward_history[~ignored].to_numpy()

    # -- Initialize model --
    forager = ForagerCollection().get_forager(
       agent_class_name=analysis_args["agent_class"],
       agent_kwargs=analysis_args["agent_kwargs"],
    )
    forager.fit(
       choice_history,
       reward_history,
       **analysis_args["fit_kwargs"],
    )

    # -- Saving results --
    # 1. Figure
    result_dir = f"/root/capsule/results/{job_hash}"
    os.makedirs(result_dir, exist_ok=True)

    fig_fitting, _ = forager.plot_fitted_session(if_plot_latent=True)
    upload_s3_fig(fig_fitting, job_hash, "fitted_session.png", if_save_local=True)
 
    # 2. Fit results object
    # Have to flatten pydantic models in forager for pickle to work
    forager.ParamModel = forager.ParamModel.model_json_schema()
    forager.ParamFitBoundModel = forager.ParamFitBoundModel.schema_json()
    forager.params = forager.params.model_dump()
    upload_s3_pkl(forager, job_hash, "forager.pkl", if_save_local=True)

    """
    # -- Reload from pickle --
    with fs.open(f"{s3_results_root}/{job_hash}/forager.pkl", "rb") as f:
        forager_reloaded = pickle.load(f)
        
    # Recover pydantic models if needed
    forager_tmp = forager_reloaded.__class__(**forager_reloaded.agent_kwargs)
    forager_reloaded.ParamModel = forager_tmp.ParamModel
    forager_reloaded.ParamFitBoundModel = forager_tmp.ParamFitBoundModel
    forager.params = forager_reloaded.ParamModel(**forager.params)
    
    # Test
    forager.plot_fitted_session(if_plot_latent=True)
    """

    # -- Prepare database record --
    analysis_results = forager.get_fitting_result_dict()

    analysis_libs_to_track_ver = {
        lib: pkg_resources.get_distribution(lib).version
        for lib in job_dict["analysis_spec"]["analysis_libs_to_track_ver"]
    }

    result_dict = {
        **job_dict,
        "analysis_datetime": datetime.now().isoformat(),
        "analysis_time_spent_in_sec": time.time() - start_time, 
        "analysis_libs_to_track_ver": analysis_libs_to_track_ver,
        "analysis_results": analysis_results,
    }

    # -- Insert to s3 --
    uploaded = upload_s3_json(result_dict, job_hash, "results.json", if_save_local=True)
    
    # -- Insert to docDB via ssh --
    status = insert_docDB_ssh(result_dict, "mle_fitting")


    return "success"