FROM apache/airflow:2.2.3
USER root
RUN apt-get update && apt-get install -y python3-opencv && apt-get install libmagic1
USER airflow
RUN pip install --no-cache-dir ngt opencv-python opencv-contrib-python python-magic PyMuPDF Pillow
