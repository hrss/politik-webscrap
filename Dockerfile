FROM python:3
ADD . /home/webscrap
RUN pip install celery
RUN pip install pika
RUN pip install unidecode
RUN pip install requests
WORKDIR /home/webscrap

ENTRYPOINT celery -A tasks worker -B -f celery.log
