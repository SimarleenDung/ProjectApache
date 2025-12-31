data "vault_kv_secret_v2" "snowflake" {
  mount = "secret"
  name  = "snowflake"
}
 
provider "snowflake" {
  account_name = data.vault_kv_secret_v2.snowflake.data["account_name"]
  user= data.vault_kv_secret_v2.snowflake.data["user"]
  password = data.vault_kv_secret_v2.snowflake.data["password"]
  role     = data.vault_kv_secret_v2.snowflake.data["role"]
  warehouse = data.vault_kv_secret_v2.snowflake.data["warehouse"]
  organization_name = data.vault_kv_secret_v2.snowflake.data["organization"]
  preview_features_enabled = ["snowflake_table_resource"]
}
 
resource "snowflake_database" "example_db" {
  name = "EXAMPLE_DB"
}