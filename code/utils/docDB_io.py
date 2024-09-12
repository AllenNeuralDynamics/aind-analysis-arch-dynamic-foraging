from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"

def insert_docDB_ssh(result_dict, collection_name):
    credentials.collection = collection_name

    with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
        response = doc_db_client.collection.insert_one(result_dict)
        
    return response.inserted_id