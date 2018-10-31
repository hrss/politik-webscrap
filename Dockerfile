FROM python:3
ADD . /
RUN pip install celery
RUN pip install pika
RUN pip install unidecode
CMD [ "celery", "-A proj worker -B" ]
