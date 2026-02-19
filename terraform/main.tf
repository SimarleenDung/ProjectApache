data "vault_kv_secret_v2" "snowflake" {
  mount = "snowflake-secrets"
  name  = "snowflake-secrets"
}
 
provider "snowflake" {
  organization_name = data.vault_kv_secret_v2.snowflake.data["organization_name"]
  account_name      = data.vault_kv_secret_v2.snowflake.data["account_name"]
  user              = data.vault_kv_secret_v2.snowflake.data["user"]
  password          = data.vault_kv_secret_v2.snowflake.data["password"]
  role              = data.vault_kv_secret_v2.snowflake.data["role"]
  preview_features_enabled  = ["snowflake_table_resource"]
}
 
resource "snowflake_database" "example_db" {
  name = "EXAMPLE_DB"
}