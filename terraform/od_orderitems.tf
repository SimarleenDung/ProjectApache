resource "snowflake_table" "OD_ORDERITEM" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "OD_ORDERITEM"

  column {
    name = "ORDER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "ORDER_ITEM_ID"
    type = "NUMBER(5,0)"
  }

  column {
    name = "PRODUCT_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "SELLER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "SHIPPING_LIMIT_DATE"
    type = "TIMESTAMP_NTZ(3)"
  }
 
 column {
    name = "PRICE"
    type = "NUMBER(10,2)"
  }

  column {
    name = "FREIGHT_VALUE"
    type = "NUMBER(10,2)"
  }


}
