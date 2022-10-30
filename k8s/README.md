# K8S deploy tutorial

1) Setup secrets in `secrets.yml` in base64 encoding, apply it with `kubectl apply -f secrets.yml`
2) Setup schedule interval in cron-job MANIFEST
3) Run cron-job on cluster with `kubectl apply -f`
