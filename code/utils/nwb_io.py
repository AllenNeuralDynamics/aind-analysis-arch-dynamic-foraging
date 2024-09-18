"""Util function for reading NWB files. (for dev only)"""

import numpy as np
from pynwb import NWBHDF5IO

def get_nwb(session_id):
    """Get NWB file from session_id.

    Overwrite this function to get NWB file from other places.

    Parameters
    ----------
    session_id : _type_
        _description_
    """
    io = NWBHDF5IO(f"/root/capsule/data/foraging_nwb_bonsai/{session_id}.nwb", mode="r")
    nwb = io.read()
    return nwb


def get_history_from_nwb(nwb):
    """Get choice and reward history from nwb file
    
    #TODO move this to aind-behavior-nwb-util
    """

    df_trial = nwb.trials.to_dataframe()

    autowater_offered = (df_trial.auto_waterL == 1) | (df_trial.auto_waterR == 1)
    choice_history = df_trial.animal_response.map({0: 0, 1: 1, 2: np.nan}).values
    reward_history = df_trial.rewarded_historyL | df_trial.rewarded_historyR
    p_reward = [
        df_trial.reward_probabilityL.values,
        df_trial.reward_probabilityR.values,
    ]
    random_number = [
        df_trial.reward_random_number_left.values,
        df_trial.reward_random_number_right.values,
    ]

    baiting = False if "without baiting" in nwb.protocol.lower() else True

    return (
        baiting,
        choice_history,
        reward_history,
        p_reward,
        autowater_offered,
        random_number,
    )
