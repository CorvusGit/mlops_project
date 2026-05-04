resource "helm_release" "prometheus" {
  name       = "prometheus"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"

  namespace        = "monitoring"
  create_namespace = true

  values = [
    file("${path.module}/manifests/prometheus-values.yaml")
  ]

  set {
    name  = "grafana.adminPassword"
    value = var.grafana_admin_password
  }
}