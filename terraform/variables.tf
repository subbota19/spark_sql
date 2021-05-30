variable "prefix" {
  description = "This prefix will be included in the name of most resources."
  default = "subota"
}

variable "region" {
  type = string
  default = "westeurope"
}

variable "node_type_id" {
  type = string
  default = "Standard_DS3_v2"
}

variable "spark_version" {
  type = string
  default = "7.3.x-scala2.12"
}

variable "upload_path" {
  type = string
  default = "abfs://data@subota19.dfs.core.windows.net/sql/"
}

variable "client_secret" {
  type = string
  default = "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT"
}

variable "provider_type" {
  type = string
  default = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
}

variable "client_endpoint" {
  type = string
  default = "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token"
}

variable "account_key" {
  type = string
  default = "K7vavU+ZjtY0iD4C3guRgaxsSsiXzXNyQHzvEqGgVhhghgmNyE+vYdWEzyqnOVtRihqy+0RVl469yB2J1dSKRQ=="
}

variable "client_id" {
  type = string
  default = "f3905ff9-16d4-43ac-9011-842b661d556d"
}

variable "expedia_path" {
  type = string
  default = "abfs://m06sparksql@bd201stacc.dfs.core.windows.net/expedia/"
}

variable "auth_type" {
  type = string
  default = "OAuth"
}

variable "account_load_name" {
  type = string
  default = "bd201stacc"
}

variable "account_upload_name" {
  type = string
  default = "subota19"
}

variable "core_info" {
  type = string
  default = "local[*]"
}

variable "hotel_weather_path" {
  type = string
  default = "abfs://m06sparksql@bd201stacc.dfs.core.windows.net/hotel-weather/"
}
