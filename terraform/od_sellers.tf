resource "snowflake_table" "SELLERS_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "SELLERS_OD"

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

}
