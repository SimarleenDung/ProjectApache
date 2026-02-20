resource "snowflake_table" "GEOLOCATION_OD" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "GEOLOCATION_OD"

  column {
    name = "GEOLOCATION_ZIP_CODE_PREFIX"
    type = "NUMBER(5,0)"
  }

  column {
    name = "GEOLOCATION_LAT"
    type = "FLOAT"
  }

  column {
    name = "GEOLOCATION_LNG"
    type = "FLOAT"
  }

  column {
    name = "GEOLOCATION_CITY"
    type = "VARCHAR(16777216)"
  }

  column {
    name = "GEOLOCATION_STATE"
    type = "VARCHAR(255)"
  }

}
