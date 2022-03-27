# Dockerfile for ride matching consumer


FROM python:3.7
RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev

RUN pip3 install pika

WORKDIR /ride_matching_consumer

COPY . /ride_matching_consumer


ENTRYPOINT [ "python3" ]

CMD [ "ride_matching_consumer.py" ]
