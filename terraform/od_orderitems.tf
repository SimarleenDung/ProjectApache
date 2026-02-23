resource "snowflake_table" "ORDERITEM_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "ORDERITEM_OD"

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
