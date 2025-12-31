variable "snowflake_role" {
    default = "ACCOUNTADMIN"
}

variable "snowflake_database"{
    default = "AIRFLOW_DB"
    type = string
}

variable "snowflake_schema"{
    default = "STAGE"
    type = string
}

variable "snowflake_warehouse"{
    default = "COMPUTE_WH"
    type = string
}