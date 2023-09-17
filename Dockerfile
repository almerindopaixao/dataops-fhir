FROM datamechanics/spark:3.2.1-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
ENV PATH=$PATH:$SPARK_HOME/bin

USER 0

WORKDIR /opt/application/

COPY requirements.txt ./

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

COPY .docker/conf/log4j.properties /opt/spark/conf/

COPY . .

CMD ["spark-submit", "app.py"]