# Flight Delay ML Prediction System

The repository contains a complete MlOps pipeline for implementing a flight delay prediction system, namely:
* Airflow DAGs with feeature preparation, model fitting and evaluation and features monitoring.
* MlFlow code and systemd example.
* Kuberntes cron-job for seving ML model and predict fresh data.
* ~~Grafana with some dashboards and alerts.~~

## Usage

```
1. Setup Airflow on any machine.
2. Clone this repo to Airflow server and change DAG folder in airflow.conf to {REPO}/dags
3. On Airflow node also run MlFlow daemon (see template)
4. Setup all S3 ENV variables (see k8s/secrets)
5. Connect node to Hadoop cluster (see https://github.com/slavkostrov/flight_delays_ml_system/blob/dev/bash/setup_hadoop.sh)
6. Change Config in config.py and Run DAGS
7. Deploy model on K8S via MANIFESTs from /k8s.
```

## Tests

TODO


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
