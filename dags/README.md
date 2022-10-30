# DAGs

1) Features preparation DAG
![изображение](https://user-images.githubusercontent.com/64536258/198902312-d7a28fdc-d0ae-472d-9d5f-79f3929a074b.png)

* Download raw data
* Clean raw data from outliers etc
* Build features from clean data
* Save features and trigger next two dags

2) Model learning DAG
![изображение](https://user-images.githubusercontent.com/64536258/198902327-96d31225-25ed-4c5d-a02d-169b4cf560a5.png)
* Fit new model on new features
* Compare quality with actual production (mlflow stage) model
* If new model is better, set it to production

3) Monitoring DAG
![изображение](https://user-images.githubusercontent.com/64536258/198902338-e22f9e90-481d-4cd8-afa5-e50a3f51c12e.png)
* Creating SQL tables (by default with PostgresOperator)
* Collect feature stats and place it to SQL tables
* Collect last run metrics from MlFlow and place it to SQL tables
