WORKSPACEWEB=$(az synapse workspace show --name $SYNAPSE_NAME --resource-group $RESOURCE_GROUP | python3 -c "import sys, json;j = json.load(sys.stdin);print(j['connectivityEndpoints']['web'])")

echo "Open your Azure Synapse Workspace Web URL in the browser: $WORKSPACEWEB"