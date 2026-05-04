#!/bin/bash
set -e

CONDA_BIN="/opt/conda/bin/conda"
PIP_BIN="/opt/conda/bin/pip"

$PIP_BIN install --upgrade pip
$PIP_BIN install mlflow

curl -o YandexCA.pem https://storage.yandexcloud.net/cloud-certs/CA.pem
cp YandexCA.pem /etc/ssl/certs/yandex_ca.pem
chmod 644 /etc/ssl/certs/yandex_ca.pem
