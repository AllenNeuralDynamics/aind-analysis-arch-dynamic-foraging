import logging
from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"

logger = logging.getLogger(__name__)

def insert_result_to_docDB_ssh(result_dict, collection_name) -> int:
    """_summary_

    Parameters
    ----------
    result_dict : dict
        bson-compatible result dictionary to be inserted
    collection_name : _type_
        name of the collection to insert into (such as "mle_fitting")

    Returns
    -------
    integer
        inserted_id
    """
    credentials.collection = collection_name
    
    with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
        # Check if job hash already exists, if yes, log warning, but still insert
        if doc_db_client.collection.find_one({"job_hash": result_dict["job_hash"]}):
            logger.warning(f"Job hash {result_dict['job_hash']} already exists in {collection_name} in docDB")
        # Insert
        response = doc_db_client.collection.insert_one(result_dict)
        
    if response.acknowledged is False:
        logger.error(f"Failed to insert {result_dict['job_hash']} to {collection_name} in docDB")
        return {"status": "docDB insertion failed", "docDB_id": None}
    else:
        logger.info(f"Inserted {response.inserted_id} to {collection_name} in docDB")
        return {"status": "success", "docDB_id": response.inserted_id}