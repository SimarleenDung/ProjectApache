resource "snowflake_table" "ORDERS_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "ORDERS_OD"

  column {
    name = "ORDER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "CUSTOMER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "ORDER_STATUS"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "ORDER_PURCHASE_TIMESTAMP"
    type = "TIMESTAMP_NTZ(3)"
  }

  column {
    name = "ORDER_APPROVED_AT"
    type = "TIMESTAMP_NTZ(3)"
  }

 column {
    name = "ORDER_DELIVERED_CARRIER_DATE"
    type = "TIMESTAMP_NTZ(3)"
  }

   column {
    name = "ORDER_DELIVERED_CUSTOMER_DATE"
    type = "TIMESTAMP_NTZ(3)"
  }
   column {
    name = "ORDER_ESTIMATED_DELIVERY_DATE"
    type = "DATE"
  }


}
