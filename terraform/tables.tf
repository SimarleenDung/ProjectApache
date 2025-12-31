resource "snowflake_table" "nation" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "NATION"

column {
  name = "N_NATIONKEY"
  type = "VARCHAR(16777216)"
}

column {
  name = "N_NAME"
  type = "VARCHAR(16777216)"
}

column {
  name = "N_REGIONKEY"
  type = "VARCHAR(16777216)"
}

column {
  name = "N_COMMENT"
  type = "VARCHAR(16777216)"
}

column {
  name = "DAG_ID"
  type = "VARCHAR(16777216)"
}

column {
  name = "LOAD_TIME"
  type = "VARCHAR(16777216)"
}

}
