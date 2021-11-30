FROM python:3.8.12-slim

USER root:root

RUN mkdir /work-dir

WORKDIR /work-dir

RUN pip3 install azure-storage-file-datalake azure-identity requests

COPY azurecloudhandler /work-dir/azurecloudhandler

COPY app.py /work-dir

CMD ["python3", "app.py"]