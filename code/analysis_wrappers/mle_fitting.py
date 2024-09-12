import logging
import os
import time

import numpy as np
import multiprocessing as mp

from utils.nwb_io import get_history_from_nwb
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
    ) = get_history_from_nwb(f"/root/capsule/data/{session_id}.nwb")
    
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

    # Saving results
    result_dir = f"/root/capsule/results/{job_hash}"
    os.makedirs(result_dir, exist_ok=True)

    fig_fitting, axes = forager.plot_fitted_session(if_plot_latent=True)
    fig_fitting.savefig(f"{result_dir}/fitted.png")

    # %%
    import s3fs
    import pickle
    from datetime import datetime

    fs = s3fs.S3FileSystem(anon=False)
    s3_results_root = "aind-behavior-data/foraging_nwb_bonsai_processed/v2"
    with fs.open(f"{s3_results_root}/{job_hash}/fitted.png", "wb") as f:
        fig_fitting.savefig(f)

    # Save to pickle on s3
    # Have to flatten pydantic models in forager for pickle to work
    forager.ParamModel = forager.ParamModel.model_json_schema()
    forager.ParamFitBoundModel = forager.ParamFitBoundModel.schema_json()
    forager.params = forager.params.model_dump()

    with fs.open(f"{s3_results_root}/{job_hash}/forager.pkl", "wb") as f:
        pickle.dump(forager, f)

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

    # -- Insert key numbers to docDB --
    # Prepare json

    import pkg_resources
    analysis_libs = {lib: pkg_resources.get_distribution(lib).version
                     for lib in job_dict["analysis_spec"]["analysis_libs"]}
        
    analysis_results = forager.get_fitting_result_dict()

    result_dict = {
        **job_dict,
        "analysis_datetime": datetime.now().isoformat(),
        "analysis_time_spent_in_sec": time.time() - start_time, 
        "analysis_libs": analysis_libs,
        "analysis_results": analysis_results,
    }
    
    # -- Insert to docDB via ssh --
    from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

    credentials = DocumentDbSSHCredentials()
    credentials.database = "behavior_analysis"
    credentials.collection = "model_fitting"

    with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
        response = doc_db_client.collection.insert_one(result_dict)
        print(response.inserted_id)
    