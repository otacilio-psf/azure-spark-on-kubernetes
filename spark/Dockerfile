FROM otaciliopsf/spark-py:v3.1.1-hadoop3.2-abfss-delta

USER root:root

WORKDIR /opt/spark/work-dir

COPY app.py /opt/spark/work-dir

COPY schema.py /opt/spark/work-dir

COPY dimensao_municipio.csv /opt/spark/work-dir

COPY dimensao_cursos.csv /opt/spark/work-dir

USER root