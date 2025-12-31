# Importing hvac library which is the official python client for Vault
import hvac
 
from airflow.models import Variable
 
# This function when called, connects to Vault and fetches snowflake credentials
def get_snowflake_creds_from_vault():
        vault_addr = Variable.get("VAULT_ADDR", default_var="http://vault:8200") # url points to the Vault server adderss ie port 8200
        token = Variable.get("VAULT_TOKEN")  # set via env/secret manager. Vault token to authorize
        client = hvac.Client(url=vault_addr, token=token)
 
       
        secret = client.secrets.kv.v2.read_secret_version(
                mount_point="secret",
                path="snowflake"
            )
 
 
        data = secret["data"]["data"] # We're building a dictionary of credentials
 
        return {
            "account": data["account"],
            "user": data["user"],
            "password": data["password"],
            "warehouse": data["warehouse"],
            "database": data["database"],
            "schema": data["schema"],
            "role": data["role"]
        } # returns the connection fields expected by the snowflake.connector.connect() function