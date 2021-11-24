az storage account create --name $STG_ACC_NAME --resource-group $RESOURCE_GROUP --location $LOCATION --sku Standard_RAGRS --kind StorageV2 --enable-hierarchical-namespace true

az storage fs create --name $LAKE_NAME --account-name $STG_ACC_NAME