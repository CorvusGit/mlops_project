resource "kubernetes_namespace" "spark_operator" {
  metadata {
    name = "spark-operator"
  }
}

resource "helm_release" "spark_operator" {
  name       = "spark-operator"
  repository = "https://kubeflow.github.io/spark-operator"
  chart      = "spark-operator"

  namespace = kubernetes_namespace.spark_operator.metadata[0].name

  depends_on = [
    kubernetes_namespace.spark_operator
  ]

  set {
    name  = "installCRDs"
    value = "true"
  }

  set {
    name  = "spark.jobNamespaces[0]"
    value = "spark"
  }

  set {
    name  = "metrics.enable"
    value = "true"
  }

  set {
    name  = "webhook.enable"
    value = "false"
  }
}