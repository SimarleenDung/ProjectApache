resource "snowflake_table" "part" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "PART"

column {
  name = "P_PARTKEY"
  type = "NUMBER(38,0)"
}

column {
  name = "P_NAME"
  type = "VARCHAR(55)"
}

column {
  name = "P_MFGR"
  type = "VARCHAR(25)"
}

column {
  name = "P_BRAND"
  type = "VARCHAR(10)"
}

column {
  name = "P_TYPE"
  type = "VARCHAR(25)"
}

column {
  name = "P_SIZE"
  type = "NUMBER(38,0)"
}

column {
  name = "P_CONTAINER"
  type = "VARCHAR(10)"
}

column {
  name = "P_RETAILPRICE"
  type = "NUMBER(12,2)"
}

column {
  name = "P_COMMENT"
  type = "VARCHAR(23)"
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
