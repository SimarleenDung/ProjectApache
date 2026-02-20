resource "snowflake_table" "ORDERPAYMENT_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "ORDERPAYMENT_OD"

  column {
    name = "ORDER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "PAYMENT_SEQUENTIAL"
    type = "NUMBER(3,0)"
  }

  column {
    name = "PAYMENT_TYPE"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "PAYMENT_INSTALLMENTS"
    type = "NUMBER(3,0)"
  }

  column {
    name = "PAYMENT_VALUE"
    type = "NUMBER(10,2)"
  }


}
