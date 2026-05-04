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

variable "for_ml_fraud_topic" {
  description = "for_ml_fraud_topic"
  type        = string
}

variable "prediction_topic" {
  description = "prediction_topic"
  type        = string
}

variable "user_name" {
  description = "user name"
  type        = string
}

variable "password" {
  description = "password for user"
  type        = string
}