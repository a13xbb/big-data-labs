FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y python3-pip

RUN pip3 install pandas scikit-learn psutil tqdm

RUN pip3 install matplotlib

USER 1001

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]