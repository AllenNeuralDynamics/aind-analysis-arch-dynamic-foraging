""" top level run script """

import multiprocessing as mp
import numpy as np

from nwb_io import get_history_from_nwb
from aind_dynamic_foraging_models.generative_model import ForagerCollection

def run():
    # Load data
    session_id = "703548_2024-03-01_08-51-32"
    (
        baiting,
        choice_history,
        reward_history,
        p_reward,
        autowater_offered,
        random_number,
    ) = get_history_from_nwb(f"/root/capsule/data/foraging_nwb_bonsai/{session_id}.nwb")

    # Remove NaNs
    # TODO: handle in model fitting
    ignored = np.isnan(choice_history)
    choice_history = choice_history[~ignored]
    reward_history = reward_history[~ignored].to_numpy()
    p_reward = [p[~ignored] for p in p_reward]
    
    # Initialize model
    forager = ForagerCollection().get_preset_forager("Hattori2019", seed=42)
    forager.fit(
        choice_history,
        reward_history,
        DE_kwargs=dict(workers=1, disp=False, seed=np.random.default_rng(42)),
        k_fold_cross_validation=0,
    )
    fig_fitting, axes = forager.plot_fitted_session(if_plot_latent=True)
    
    fig_fitting.savefig(f"results/{session_id}_Hattori_fitted.png")
    

if __name__ == "__main__": run()