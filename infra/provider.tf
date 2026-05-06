# Объявление провайдера
terraform {
  # Версия самого Terraform (оставляем вашу 1.5.7)
  required_version = ">= 1.00" 

  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
    # Версия ПРОВАЙДЕРА Helm (вот здесь нужна 2.x)
    helm = {
      source  = "hashicorp/helm"
      version = "2.10.1" 
    }
  }
}

provider "yandex" {
  zone      = var.yc_config.zone
  folder_id = var.yc_config.folder_id
  token     = var.yc_config.token
  cloud_id  = var.yc_config.cloud_id
}

provider "helm" {
  kubernetes {
    host                   = module.k8s.k8s_cluster_external_endpoint
    cluster_ca_certificate = module.k8s.k8_cluster_ca_certificate
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "yc"
      args        = ["k8s", "create-token"]
    }
  }
}

provider "kubernetes" {
  host                   = module.k8s.k8s_cluster_external_endpoint
  cluster_ca_certificate = module.k8s.k8_cluster_ca_certificate
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "yc"
    args        = ["k8s", "create-token"]
  }
}