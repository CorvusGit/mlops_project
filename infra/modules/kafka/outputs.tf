output "kafka_host_fqdn" {
  value = tolist(yandex_mdb_kafka_cluster.kafka_cluster.host)[0].name
}