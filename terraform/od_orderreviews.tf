resource "snowflake_table" "ORDERREVIEWS_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "ORDERREVIEWS_OD"

  column {
    name = "REVIEW_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "ORDER_ID"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "REVIEW_SCORE"
    type = "NUMBER(1,0)"
  }

  column {
    name = "REVIEW_COMMENT_TITLE"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "REVIEW_COMMENT_MESSAGE"
    type = "VARCHAR(16777216)"
  }

 column {
    name = "REVIEW_CREATION_DATE"
    type = "DATE"
  }

   column {
    name = "REVIEW_ANSWER_TIMESTAMP"
    type = "TIMESTAMP_NTZ(3)"
  }


}
