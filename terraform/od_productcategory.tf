resource "snowflake_table" "PRODUCT_CATEGORY_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "PRODUCT_CATEGORY_OD"

  column {
    name = "PRODUCT_CATEGORY_NAME"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "PRODUCT_CATEGORY_NAME_ENGLISH"
    type = "NUMBER(5,0)"
  }

}
