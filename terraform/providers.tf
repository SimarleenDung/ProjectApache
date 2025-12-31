terraform {
  required_providers {
    vault = {
      source  = "hashicorp/vault"
    }
    snowflake = {
      source  = "snowflake-labs/snowflake"
    }
  }
}
 
variable "vault_address" {
  type = string
}
 
variable "vault_token" {
  type      = string
  sensitive = true
}
 
provider "vault" {
  address = var.vault_address
  token   = var.vault_token
}