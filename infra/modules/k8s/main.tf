# modules/k8s/main.tf

resource "yandex_kubernetes_cluster" "kub_cluster" {
  name       = var.instance_name
  network_id = var.network_id

  master {
    version = "1.31"
    zonal {
      zone      = var.provider_config.zone
      subnet_id = var.subnet_id
    }

    maintenance_policy {
      auto_upgrade = true
    }

    # ЗАКРЫВАЕМ API ОТ ИНТЕРНЕТА
    public_ip = true 
  }

  
  service_account_id      = var.service_account_id
  node_service_account_id = var.service_account_id
}

resource "yandex_kubernetes_node_group" "kub_nodes" {
  cluster_id = yandex_kubernetes_cluster.kub_cluster.id
  name       = "minimal-node-group"

  instance_template {
    platform_id = "standard-v1"

    resources {
      memory = 16  # GB
      cores  = 4
    }

    boot_disk {
      type = "network-ssd"
      size = 30 # GB
    }

    network_interface {
      subnet_ids = [var.subnet_id]
      nat        = false
    }

    scheduling_policy {
      preemptible = true  # дешевле
    }
  }

  scale_policy {
    fixed_scale {
      size = 2
    }
  }

  allocation_policy {
    location {
      zone = var.provider_config.zone
    }
  }
}

# Реестр для образов
resource "yandex_container_registry" "registry" {
  name      = var.registry_name
  folder_id = var.provider_config.folder_id
}


# Создаем ServiceAccount внутри K8s

resource "kubernetes_namespace_v1" "spark_ns" {
  metadata {
    name = "spark"
  }
}

resource "kubernetes_service_account_v1" "spark_sa" {
  metadata {
    name      = "spark"
    namespace = "spark"
  }
  depends_on = [kubernetes_namespace_v1.spark_ns]
}

resource "kubernetes_role_binding_v1" "spark_role_binding" {
  metadata {
    name      = "spark-role-binding"
    namespace = "spark"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "edit"
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.spark_sa.metadata[0].name
    namespace = "spark"
  }
}