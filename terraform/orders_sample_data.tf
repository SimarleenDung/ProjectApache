resource "snowflake_table" "orders" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "ORDERS"

column {
  name = "O_ORDERKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "O_CUSTKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "O_ORDERSTATUS"
  type = "VARCHAR(1)"
}

column {
  name = "O_TOTALPRICE"
  type = "NUMBER(12,2)"
}

column {
  name = "O_ORDERDATE"
  type = "DATE"
}

column {
  name = "O_ORDERPRIORITY"
  type = "VARCHAR(15)"
}

column {
  name = "O_CLERK"
  type = "VARCHAR(15)"
}

column {
  name = "O_SHIPPRIORITY"
  type = "NUMBER(38,0)"
}

column {
  name = "O_COMMENT"
  type = "VARCHAR(79)"
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
