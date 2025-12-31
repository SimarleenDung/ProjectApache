resource "snowflake_table" "customer" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "CUSTOMER"

column {
  name = "C_CUSTKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "C_NAME"
  type = "VARCHAR(25)"
}

column {
  name = "C_ADDRESS"
  type = "VARCHAR(40)"
}

column {
  name = "C_NATIONKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "C_PHONE"
  type = "VARCHAR(15)"
}

column {
  name = "C_ACCTBAL"
  type = "NUMBER(12,2)"
}

column {
  name = "C_MKTSEGMENT"
  type = "VARCHAR(10)"
}

column {
  name = "C_COMMENT"
  type = "VARCHAR(117)"
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
