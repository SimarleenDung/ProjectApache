resource "snowflake_table" "supplier" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "SUPPLIER"

column {
  name = "S_SUPPKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "S_NAME"
  type = "VARCHAR(25)"
}

column {
  name = "S_ADDRESS"
  type = "VARCHAR(40)"
}

column {
  name = "S_NATIONKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "S_PHONE"
  type = "VARCHAR(15)"
}

column {
  name = "S_ACCTBAL"
  type = "NUMBER(12,2)"
}

column {
  name = "S_COMMENT"
  type = "VARCHAR(101)"
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
