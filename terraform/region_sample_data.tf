resource "snowflake_table" "region" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "REGION"

column {
  name = "R_REGIONKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "R_NAME"
  type = "VARCHAR(25)"
}

column {
  name = "R_COMMENT"
  type = "VARCHAR(152)"
}

column {
  name = "DAG_ID"
  type = "VARCHAR(25)"
}

column {
  name = "LOAD_TIME"
  type = "TIMESTAMP_NTZ"
}

}
