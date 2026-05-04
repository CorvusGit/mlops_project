output "mlflow_vm_public_ip" {
  description = "Публичный IP VM из модуля mlflow_vm"
  value       = module.mlflow-vm.mlflow_public_ip
}

output "new_bucket_name" {
  value = module.storage.bucket
  description = "Name of the created bucket"
}

output "kafka_host" {
  value = module.kafka.kafka_host_fqdn
  description = "kafka_host_fqdn"
}


output "k8s_cluster_internal_fqdn" {
  value = module.k8s.k8s_cluster_internal_fqdn
}

output "k8_cluster_id" {
  value = module.k8s.k8_cluster_id
}

output "registry_id" {
  value = module.k8s.registry_id
}
