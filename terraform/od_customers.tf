resource "snowflake_table" "OD_CUSTOMER" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "OD_CUSTOMER"

  column {
    name = "CUSTOMER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "CUSTOMER_UNIQUE_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "CUSTOMER_ZIP_CODE_PREFIX"
    type = "NUMBER(5,0)"
  }

  column {
    name = "CUSTOMER_CITY"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "CUSTOMER_STATE"
    type = "VARCHAR(16777216)"
  }

}
