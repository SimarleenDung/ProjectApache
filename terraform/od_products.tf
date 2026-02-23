resource "snowflake_table" "PRODUCTS_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "PRODUCTS_OD"

  column {
    name = "PRODUCT_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "PRODUCT_CATEGORY_NAME"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "PRODUCT_NAME_LENGHT"
    type = "NUMBER(5,0)"
  }

  column {
    name = "PRODUCT_DESCRIPTION_LENGHT"
    type = "NUMBER(6,0)"
  }

  column {
    name = "PRODUCT_PHOTOS_QTY"
    type = "NUMBER(3,0)"
  }

  column {
    name = "PRODUCT_WEIGHT_G"
    type = "NUMBER(8,0)"
  }

 column {
    name = "PRODUCT_LENGTH_CM"
    type = "NUMBER(6,2)"
  }

   column {
    name = "PRODUCT_HEIGHT_CM"
    type = "NUMBER(6,2)"
  }
   column {
    name = "PRODUCT_WIDTH_CM"
    type = "NUMBER(6,2)"
  }



}