Feature store
------------------

Proovin luua prototype feature store-i mille backend üles ehitatud CosmosDB-l 
ja frontend kuni feature gruppide access-ini pythonis. Datasetide ja ML
treeningu osa jaoks tahaks kasutada hiljem AzureML-i.

Dataset prototüüpimise jaoks
  * https://www.kaggle.com/ntnu-testimon/paysim1
  *

Dev setup
---------
```
conda create --name feature-store python pandas notebook scikit-learn azure-keyvault pytest tqdm pyspark=2.4.0
conda activate feature-store
pip install azure-cosmos

conda env export --no-builds > conda_env.yml
```



Development
-----------
  * Create `local.settings.json`:
```json
{
    "cosmosdb_config":{
        "COSMOSDB_HOST":"",
        "COSMOSDB_KEY":""
    }
}
```
  * Download sim data to `data` folder from https://www.kaggle.com/ntnu-testimon/paysim1


TODO
----
  - [ ] Create aggregation framework with Spark - per step total payments aggregation
  - [ ] Create aggregation framework with Azure Functions - per step total payments aggregation
  - [ ] Automate Azure CosmosDB setup with ARM template
  - [ ] Create generator for payments to help with testing
  - [ ] Create sample job with data loading from analytical container via Spark

  

