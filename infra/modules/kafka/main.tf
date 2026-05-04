# modules/kafka/main.tf

resource "yandex_mdb_kafka_cluster" "kafka_cluster" {
  name        = var.instance_name
  environment = "PRESTABLE"
  network_id  = var.network_id
  subnet_ids  = [var.subnet_id]

  config {
    version          = "3.9"
    brokers_count    = 1
    zones            = [var.provider_config.zone]
    assign_public_ip = false
    schema_registry  = false
    rest_api {
      enabled = true
    }
    kafka_ui {
      enabled = true
    }
    kafka {
      resources {
        resource_preset_id = "s2.micro"
        disk_type_id       = "network-ssd"
        disk_size          = 32
      }
      kafka_config {
        compression_type                = "COMPRESSION_TYPE_ZSTD"
        log_flush_interval_messages     = 1024
        log_flush_interval_ms           = 1000
        log_flush_scheduler_interval_ms = 1000
        log_retention_bytes             = 1073741824
        log_retention_hours             = 168
        log_retention_minutes           = 10080
        log_retention_ms                = 86400000
        log_segment_bytes               = 134217728
        num_partitions                  = 10
        default_replication_factor      = 1
        message_max_bytes               = 1048588
        replica_fetch_max_bytes         = 1048576
        ssl_cipher_suites               = ["TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"]
        offsets_retention_minutes       = 10080
        sasl_enabled_mechanisms         = ["SASL_MECHANISM_SCRAM_SHA_256", "SASL_MECHANISM_SCRAM_SHA_512"]
      }
    }
  }
}


resource "yandex_mdb_kafka_topic" "for_ml_fraud_data" {
  cluster_id         = yandex_mdb_kafka_cluster.kafka_cluster.id
  name               = var.for_ml_fraud_topic
  partitions         = 3
  replication_factor = 1
  topic_config {
    cleanup_policy        = "CLEANUP_POLICY_DELETE"
    compression_type      = "COMPRESSION_TYPE_PRODUCER"
    delete_retention_ms   = 86400000
    file_delete_delay_ms  = 60000
    flush_messages        = 128
    flush_ms              = 1000
    min_compaction_lag_ms = 0
    retention_bytes       = 10737418240
    retention_ms          = 604800000
    max_message_bytes     = 1048588
    min_insync_replicas   = 1
    segment_bytes         = 268435456
  }
}


resource "yandex_mdb_kafka_topic" "prediction_data" {
  cluster_id         = yandex_mdb_kafka_cluster.kafka_cluster.id
  name               = var.prediction_topic
  partitions         = 3
  replication_factor = 1
  topic_config {
    cleanup_policy        = "CLEANUP_POLICY_DELETE"
    compression_type      = "COMPRESSION_TYPE_PRODUCER"
    delete_retention_ms   = 86400000
    file_delete_delay_ms  = 60000
    flush_messages        = 128
    flush_ms              = 1000
    min_compaction_lag_ms = 0
    retention_bytes       = 10737418240
    retention_ms          = 604800000
    max_message_bytes     = 1048588
    min_insync_replicas   = 1
    segment_bytes         = 268435456
  }
}

resource "yandex_mdb_kafka_user" "user_kafka" {
  cluster_id = yandex_mdb_kafka_cluster.kafka_cluster.id
  name       = var.user_name
  password   = var.password
  
  # Разрешения для первого топика (ML Fraud)
  permission {
    topic_name = var.for_ml_fraud_topic
    role       = "ACCESS_ROLE_CONSUMER"
  }
  permission {
    topic_name = var.for_ml_fraud_topic
    role       = "ACCESS_ROLE_PRODUCER"
  }

  # Разрешения для второго топика (Prediction)
  permission {
    topic_name = var.prediction_topic
    role       = "ACCESS_ROLE_CONSUMER"
  }
  permission {
    topic_name = var.prediction_topic
    role       = "ACCESS_ROLE_PRODUCER"
  }
}