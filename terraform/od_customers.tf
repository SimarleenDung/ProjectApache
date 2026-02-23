resource "snowflake_table" "CUSTOMER_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "CUSTOMER_OD"

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
