output "k8s_cluster_internal_fqdn" {
  value = yandex_kubernetes_cluster.kub_cluster.master[0].internal_v4_endpoint
}

output "k8_cluster_id" {
  value = yandex_kubernetes_cluster.kub_cluster.id
}

output "registry_id" {
  value = yandex_container_registry.registry.id
}

output "k8_cluster_ca_certificate" {
  description = "Root CA certificate for the Kubernetes cluster"
  # Мы обращаемся к первому элементу списка master
  value       = yandex_kubernetes_cluster.kub_cluster.master[0].cluster_ca_certificate
}

output "k8s_cluster_external_endpoint" {
  description = "Внешний IP API-сервера для доступа через интернет"
  value       = yandex_kubernetes_cluster.kub_cluster.master[0].external_v4_endpoint
}