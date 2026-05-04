# main.tf

module "iam" {
  source          = "./modules/iam"
  name            = var.yc_service_account_name
  provider_config = var.yc_config
}

module "network" {
  source          = "./modules/network"
  network_name    = var.yc_network_name
  subnet_name     = var.yc_subnet_name
  provider_config = var.yc_config
}

module "storage" {
  source          = "./modules/storage"
  name            = var.yc_bucket_name
  provider_config = var.yc_config
  access_key      = module.iam.access_key
  secret_key      = module.iam.secret_key
}

module "airflow-cluster" {
  source             = "./modules/airflow-cluster"
  instance_name      = var.yc_instance_name
  subnet_id          = module.network.subnet_id
  service_account_id = module.iam.service_account_id
  admin_password     = var.admin_password
  bucket_name        = module.storage.bucket
  provider_config    = var.yc_config
}

module "mlflow-vm" {
  source             = "./modules/mlflow-vm"
  
  image_id = var.mlfloe_image_id
  instance_name         = var.mlflow_instance_name
  service_account_id = var.for_mlflow_service_account_id
  subnet_id = module.network.subnet_id
  ip_address = var.mlflow_ip_address

  access_key = var.mlflow_access_key
  secret_key = var.mlflow_secret_key
  bucket_name = var.mlflow_buket_name
  
  pg_db_name = var.pg_db_name
  pg_user  = var.pg_user
  pg_password = var.pg_password
  backup_interval = var.mlflow_backup_interval
  
  public_key_path = var.mlflow_public_key_path
  provider_config = var.yc_config
}

module "kafka" {
  source             = "./modules/kafka"

  instance_name = var.kafka_instance_name
  network_id = module.network.network_id
  subnet_id = module.network.subnet_id
  provider_config = var.yc_config
  
  for_ml_fraud_topic = var.kafka_for_ml_fraud_topic
  prediction_topic = var.kafka_prediction_topic

  user_name = var.kafka_user
  password = var.kafka_pwd
}

module "k8s" {
  source             = "./modules/k8s"

  instance_name = var.k8s_instance_name
  network_id = module.network.network_id
  subnet_id = module.network.subnet_id
  provider_config = var.yc_config
  service_account_id = module.iam.service_account_id
  registry_name = var.registry_name
}


# Storage ресурсы
# устраняем ошибку apply
resource "time_sleep" "wait_for_s3_key" {
  depends_on = [
    module.iam
  ]
  create_duration = "60s"
}

# Обновление файла .env на основе данных из инфраструктуры
data "local_file" "existing_env" {
  filename = "../src/.env"
}

# 2. Перезаписываем его (старое содержимое + новые строки)
resource "local_file" "env_config" {
  filename = "../.env"
  
#AIRFLOW_URL=https://c-${module.airflow-cluster.airflow_id}.airflow.yandexcloud.net
#AIRFLOW_ADMIN_PASSWORD=${var.admin_password}
  content = <<EOT
${data.local_file.existing_env.content}
# Добавлено Terraform:
S3_ENDPOINT_URL=${var.yc_storage_endpoint_url}
S3_BUCKET_NAME=${module.storage.bucket}
S3_ACCESS_KEY=${module.iam.access_key}
S3_SECRET_KEY=${module.iam.secret_key}
YC_FOLDER_ID=${var.yc_config.folder_id}
YC_CLOUD_ID=${var.yc_config.cloud_id}
MLFLOW_IP=${var.mlflow_ip_address}
MLFLOW_PORT=${var.mlflow_port}
PERSIST_BUCKET_NAME=${var.mlflow_buket_name}
PERSIST_BUCKET_KEY=${var.mlflow_access_key}
PERSIST_BUCKET_SECRET=${var.mlflow_secret_key}
YC_REGISTRY_ID = ${module.k8s.registry_id}
YC_SA_JSON_KEY           = ${jsonencode({
      id                 = module.iam.auth_key_id
      service_account_id = module.iam.service_account_id
      created_at         = module.iam.auth_key_created_at
      public_key         = module.iam.public_key
      private_key        = module.iam.private_key
    })}
KAFKA_FQDN = module.kafka.kafka_host_fqdn
KAFKA_PORT = var.kafka_port
KAFKA_USER = var.kafka_user
KAFKA_PASS = var.kafka_pwd
KUBER_ID = module.k8s.k8_cluster_id

EOT

  depends_on = [
    module.iam,
    module.storage
  ]
}


locals {
  # Определяем только имена 
  var_names = [
    "YC_ZONE", "YC_FOLDER_ID", "YC_SUBNET_ID", "YC_SSH_PUBLIC_KEY",
    "S3_ENDPOINT_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET_NAME",
    "DP_SECURITY_GROUP_ID", "DP_SA_AUTH_KEY_PUBLIC_KEY",
    "DP_SA_ID", "DP_SA_JSON","MLFLOW_IP", "MLFLOW_PORT",
    "LEARNING_SAMPLE_FRAQ",
    "PERSIST_BUCKET_NAME", "PERSIST_BUCKET_KEY","PERSIST_BUCKET_SECRET",
    "KAFKA_USER", "KAFKA_PWD","KAFKA_FOR_ML_FRAUD_TOPIC","KAFKA_PREDICTION_TOPIC",
    "KAFKA_FQDN"
  ]

  # Формируем мапу значений
  all_values = {
    YC_ZONE              = var.yc_config.zone
    YC_FOLDER_ID         = var.yc_config.folder_id
    YC_SUBNET_ID         = module.network.subnet_id
    YC_SSH_PUBLIC_KEY    = trimspace(file(var.public_key_path))
    S3_ENDPOINT_URL      = var.yc_storage_endpoint_url
    S3_ACCESS_KEY        = module.iam.access_key
    S3_SECRET_KEY        = module.iam.secret_key
    S3_BUCKET_NAME       = module.storage.bucket
    DP_SECURITY_GROUP_ID = module.network.security_group_id
    DP_SA_AUTH_KEY_PUBLIC_KEY = module.iam.public_key
    DP_SA_ID             = module.iam.service_account_id
    MLFLOW_IP            = var.mlflow_ip_address
    MLFLOW_PORT          = var.mlflow_port
    LEARNING_SAMPLE_FRAQ = var.learning_sample_fraq
    PERSIST_BUCKET_NAME = var.mlflow_buket_name
    PERSIST_BUCKET_KEY =  var.mlflow_access_key
    PERSIST_BUCKET_SECRET = var.mlflow_secret_key
    KAFKA_USER = var.kafka_user
    KAFKA_PWD = var.kafka_pwd
    KAFKA_FOR_ML_FRAUD_TOPIC = var.kafka_for_ml_fraud_topic
    KAFKA_PREDICTION_TOPIC = var.kafka_prediction_topic
    KAFKA_FQDN =  module.kafka.kafka_host_fqdn
    YC_REGISTRY_ID = module.k8s.registry_id
    DP_SA_JSON           = jsonencode({
      id                 = module.iam.auth_key_id
      service_account_id = module.iam.service_account_id
      created_at         = module.iam.auth_key_created_at
      public_key         = module.iam.public_key
      private_key        = module.iam.private_key
    })
  }
  
  depends_on = [
    module.iam,
    module.storage,
    module.network,
    module.kafka
  ]
}


# Создаем отдельные секреты с именами как в Airflow
resource "yandex_lockbox_secret" "airflow_vars" {
  for_each = toset(local.var_names)

  name      = "airflow/variables/${each.value}"
  folder_id = var.yc_config.folder_id
}

# Создаем версии. Внутри каждой версии ключ 'value'
resource "yandex_lockbox_secret_version" "airflow_vars" {
  for_each = toset(local.var_names)

  secret_id = yandex_lockbox_secret.airflow_vars[each.value].id

  entries {
    key        = "value"
    text_value = tostring(local.all_values[each.value])
  }
}
