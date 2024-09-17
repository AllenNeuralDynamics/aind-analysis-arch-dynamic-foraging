from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"

def insert_docDB_ssh(result_dict, collection_name) -> int:
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
        response = doc_db_client.collection.insert_one(result_dict)
        
    return response.inserted_id