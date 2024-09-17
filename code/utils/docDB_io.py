import json
from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"
local_results_root = "/root/capsule/results"

def insert_docDB_ssh(result_dict, collection_name, if_save_local=True) -> int:
    """_summary_

    Parameters
    ----------
    result_dict : dict
        bson-compatible result dictionary to be inserted
    collection_name : _type_
        name of the collection to insert into (such as "mle_fitting")
    if_save_local : _type_, Optional
        whether to save the result locally, by default True

    Returns
    -------
    integer
        inserted_id
    """
    credentials.collection = collection_name
    
    if if_save_local:
        # Save json locally
        result_json = json.dumps(result_dict, indent=4)
        with open(f"{local_results_root}/{result_dict['job_hash']}/result.json", "wb") as f:
            json.dump(result_json, f)

    with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
        response = doc_db_client.collection.insert_one(result_dict)
        
    return response.inserted_id