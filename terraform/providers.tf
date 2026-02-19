terraform {
  required_providers {
    vault = {
      source  = "hashicorp/vault"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "snowflake-labs/snowflake"
      version = "~> 1.0"
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