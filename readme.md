Feature store
------------------

Proovin luua prototype feature store-i mille backend üles ehitatud CosmosDB-l 
ja frontend kuni feature gruppide access-ini pythonis. Datasetide ja ML
treeningu osa jaoks tahaks kasutada hiljem AzureML-i.

Dataset prototüüpimise jaoks
  * https://www.kaggle.com/ntnu-testimon/paysim1
  *


Development
-----------
  * Create `local.settings.json`:
```json
{
    "config":{
        "COSMOSDB_HOST":"",
        "COSMOSDB_KEY":""
    }
}
```
  * Download sim data to `data` folder from https://www.kaggle.com/ntnu-testimon/paysim1
  

