# Dockerfile for ride matching consumer


FROM python:3.7
RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev

RUN pip3 install pika

WORKDIR /app

COPY . /app

ENTRYPOINT [ "python3" ]

CMD [ "ride_matching_consumer.py" ]
