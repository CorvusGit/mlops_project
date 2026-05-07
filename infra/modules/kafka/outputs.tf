output "kafka_host_fqdn" {
  value = tolist(yandex_mdb_kafka_cluster.kafka_cluster.host)[0].name
}

# output "kafka_ip" {
#   value = tolist(yandex_mdb_kafka_cluster.kafka_cluster.host)[0].network_interface[0].ip_address
# }