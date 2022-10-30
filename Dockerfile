FROM openjdk:slim
COPY --from=python:3.8 / /
WORKDIR /code

COPY ./models/home/ubuntu/final_project/models/ /code/models/
COPY ./load_models.py /code/load_models.py

RUN pip install --no-cache-dir --upgrade mlflow pyspark==3.3.1 scipy==1.9.3
RUN pip install boto3
CMD ["python", "/code/load_models.py"]