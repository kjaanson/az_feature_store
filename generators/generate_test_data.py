
import azure.cosmos.documents as documents
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey
import azure.cosmos.errors as errors

from . import config

if __file__ == "__main__":
    
    COSMOSDB_HOST = config["COSMOSDB_HOST"]
    COSMOSDB_KEY = config["COSMOSDB_KEY"]

    DATABASE_ID = 'main'
    COLLECTION_ID = 'transactions'

    database_link = 'dbs/' + DATABASE_ID
    collection_link = database_link + '/colls/' + COLLECTION_ID

    client = CosmosClient(COSMOS_HOST, {'masterKey': MASTER_KEY})
    database = client.get_database_client(DATABASE_ID)
    container = database.get_container_client(COLLECTION_ID)

    # siia tuleb mõelda mingi data genereerimise asi
    container.create_item(body={'id':'test1','partition':'part1','prop1':'prop1','prop2'})



