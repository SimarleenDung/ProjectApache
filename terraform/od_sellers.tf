resource "snowflake_table" "OD_SELLERS" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "OD_SELLERS"

  column {
    name = "SELLER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "SELLER_ZIP_CODE_PREFIX"
    type = "NUMBER(5,0)"
  }

  column {
    name = "SELLER_CITY"
    type = "VARCHAR(255)"
  }

  column {
    name = "SELLER_STATE"
    type = "VARCHAR(255)"
  }

  column {
    name = "ELT_BY"
    type = "VARCHAR(255)"
  }

  column {
    name = "ELT_TS"
    type = "TIMESTAMP_NTZ(3)"
  }

  column {
    name = "FILE_NAME"
    type = "VARCHAR(255)"
  }


}
