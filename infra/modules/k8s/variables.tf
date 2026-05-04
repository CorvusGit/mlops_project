variable "instance_name" {
  description = "Name of the compute instance"
  type        = string
}

variable "network_id" {
  description = "ID of the network"
  type        = string
}

variable "subnet_id" {
  description = "ID of the subnet"
  type        = string
}

variable "provider_config" {
  description = "Yandex Cloud configuration"
  type = object({
    zone      = string
    folder_id = string
    token     = string
    cloud_id  = string
  })
}

variable "service_account_id" {
  description = "ID of the service account"
  type        = string
}

variable "registry_name" {
  description = "registry_name"
  type        = string
}