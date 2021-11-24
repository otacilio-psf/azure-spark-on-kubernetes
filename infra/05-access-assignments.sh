# Service Principal
az ad sp create-for-rbac --name "igti-service-account" --role Contributor | python3 -c "import sys, json;j = json.load(sys.stdin);print('AZURE_CLIENT_ID=',j['appId'], sep='');print('AZURE_CLIENT_SECRET=',j['password'], sep='');print('AZURE_TENANT_ID=',j['tenant'], sep='')" > .env
az account list |  python3 -c "import sys, json; print('AZURE_SUBSCRIPTION_ID=',json.load(sys.stdin)[0]['id'], sep='')" >> .env

source .env

az role assignment create --assignee $AZURE_CLIENT_ID --role "Storage Blob Data Contributor" \
--scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STG_ACC_NAME}"

# Myself
USER_ID=$(az ad signed-in-user show | python3 -c "import sys, json;j = json.load(sys.stdin);print(j['objectId'])")
az role assignment create --assignee $USER_ID --role "Storage Blob Data Contributor" \
--scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STG_ACC_NAME}"

# Synapse Workspace
SYNAPSE_ID= $(az synapse workspace show --name $SYNAPSE_NAME --resource-group $RESOURCE_GROUP | python3 -c "import sys, json;j = json.load(sys.stdin);print(j['identity']['principalId'])")
az role assignment create --assignee $SYNAPSE_ID --role "Storage Blob Data Contributor" \
--scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STG_ACC_NAME}"
