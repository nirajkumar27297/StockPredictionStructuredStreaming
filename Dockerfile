FROM bitnami/spark:3.0.0
USER root

RUN apt-get update && apt install gnupg -y && \
        echo "deb https://dl.bintray.com/sbt/debian /" |  tee -a /etc/apt/sources.list.d/sbt.list && \
        apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 && \
        apt-get update && \
        apt-get install sbt -y && \
        apt-get install python3 -y \
        && pip3 install --upgrade pip



WORKDIR /app

COPY . /app/
COPY requirements.txt .
COPY DockerScript.sh .



RUN chmod +x /app/DockerScript.sh
RUN pip3 install -r requirements.txt
ENTRYPOINT ["/app/DockerScript.sh"]
