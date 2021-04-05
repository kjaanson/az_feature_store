import pytest
from azure.cosmos import CosmosClient, PartitionKey

import config


@pytest.fixture(scope='function')
def payments_container():

    endpoint = config.config['cosmosdb_config']['COSMOSDB_HOST']
    key = config.config['cosmosdb_config']['COSMOSDB_KEY']
    database_name = "rawdata"
    container_name = "payments"

    client = CosmosClient(endpoint, key)

    database = client.create_database_if_not_exists(id=database_name)

    containers = database.list_containers()
    if (any(container['id'] == container_name for container in containers)):
        database.delete_container(container_name)
        print('Container dropped')

    container = database.create_container_if_not_exists(
        id=container_name, 
        partition_key=PartitionKey(path="/step"),
        offer_throughput=400
    )

    print(f"Container {container} created ")

    yield container

    containers = database.list_containers()
    if (any(container['id'] == container_name for container in containers)):
        database.delete_container(container_name)
        print('Container dropped')