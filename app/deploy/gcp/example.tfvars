region                        = "us-central1"
worker_image                  = "metapage/metaframe-docker-worker:0.54.87-standalone"
opentelemetry_collector_image = "docker.io/otel/opentelemetry-collector-contrib:0.113.0"

worker_groups = {
  a = {
    # Queue IDs should later be supplied/retrieved secretly, not stored in code
    queue_id      = "81e93644-4af2-11ef-a58f-676a7833797e"
    instance_type = "n1-standard-1"
    cpus          = 1
  }
  b = {
    queue_id      = "652ab68e-5f7b-11ef-b136-6f3c51289ae7"
    instance_type = "n1-standard-2"
    min_workers   = 0
    max_workers   = 6
    cpus          = 2
  }
  # A worker group of basic N1 series VMs with attached GPUs
  c = {
    queue_id      = "4195a148-baac-11ef-b438-a71e6014b28e"
    instance_type = "n1-standard-1"
    cpus          = 1
    gpus = {
      type  = "nvidia-tesla-t4"
      count = 1
    }
    min_workers = 0
    max_workers = 3
  }
  # A worker group of accelerator-optimized VMs which come with their own GPUs
  # d = {
  #   queue_id      = "652ab68e-5f7b-11ef-b136-6f3c51289ae7"
  #   instance_type = "g2-standard-4"
  #   cpus          = 4
  # }
}
