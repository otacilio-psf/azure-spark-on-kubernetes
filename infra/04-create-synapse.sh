az synapse workspace create \
  --name $SYNAPSE_NAME \
  --resource-group $RESOURCE_GROUP \
  --storage-account $STG_ACC_NAME \
  --file-system $LAKE_NAME \
  --sql-admin-login-user $SYNAPSE_USER \
  --sql-admin-login-password $SYNAPSE_PASSWORD \
  --location $LOCATION


WORKSPACEWEB=$(az synapse workspace show --name $SYNAPSE_NAME --resource-group $RESOURCE_GROUP | python3 -c "import sys, json;j = json.load(sys.stdin);print(j['connectivityEndpoints']['web'])")

WORKSPACEDEV=$(az synapse workspace show --name $SYNAPSE_NAME --resource-group $RESOURCE_GROUP | python3 -c "import sys, json;j = json.load(sys.stdin);print(j['connectivityEndpoints']['dev'])")

CLIENTIP=$(curl -sb -H "Accept: application/json" "$WORKSPACEDEV" | python3 -c "import sys, json;j = json.load(sys.stdin);print(j['message'])")
CLIENTIP=${CLIENTIP##'Client Ip address : '}
echo "Creating a firewall rule to enable access for IP address: $CLIENTIP"

az synapse workspace firewall-rule create --end-ip-address $CLIENTIP --start-ip-address $CLIENTIP --name "Allow Client IP" --resource-group $RESOURCE_GROUP --workspace-name $SYNAPSE_NAME

echo "Open your Azure Synapse Workspace Web URL in the browser: $WORKSPACEWEB"