#FROM apache/airflow:2.2.3
FROM denv77/airflow:2.2.3.4
#USER root
#RUN apt-get update && apt-get install -y python3-opencv && apt-get install libmagic1
USER airflow
#RUN pip install --no-cache-dir ngt opencv-python opencv-contrib-python python-magic PyMuPDF Pillow
RUN pip install --no-cache-dir airflow-code-editor black
