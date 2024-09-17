# %%
# Via REST API
import json
import requests

URL = "https://api.allenneuraldynamics-test.org/v1/behavior_analysis/test/"
filter = {"subject_id": "test"}
limit = 500
response = requests.get(URL, params={"filter": json.dumps(filter), "limit": limit})
print(response.json())


# %%
# Via aind_data_access
# pip install aind-data-access-api[docdb]
from aind_data_access_api.document_db import MetadataDbClient

API_GATEWAY_HOST = "api.allenneuraldynamics-test.org"
DATABASE = "behavior_analysis"
COLLECTION = "test"

docdb_api_client = MetadataDbClient(
    host=API_GATEWAY_HOST,
    database=DATABASE,
    collection=COLLECTION,
)

filter = {"subject_id": "test"}
response = docdb_api_client.retrieve_docdb_records(
    filter_query=filter,
)
print(response)

# %%
docdb_api_client.upsert_one_docdb_record(
    {"_id": "", "subject_id": "test_3"}
)


#%%
# Via ssh
from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

credentials = DocumentDbSSHCredentials()
credentials.database = "behavior_analysis"
credentials.collection = "test"

with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
   # To get a list of filtered records:
   filter = {}

   count = doc_db_client.collection.count_documents(filter)
   response = list(doc_db_client.collection.find(filter=filter, projection={"_id": 0}))
   print(len(response))
   
   # Upsert one record!
   doc_db_client.collection.insert_one({"subject_id": "test_4"})