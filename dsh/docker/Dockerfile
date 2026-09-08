FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm@sha256:b726eb94f42fcddb10056835f2c474c9f9e12e717ba2b2d2f9a8b1d78feeb68b
USER root
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MPLCONFIGDIR=/tmp/matplotlib
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update && apt-get install -y --no-install-recommends gdal-bin python3-gdal libgomp1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /opt/geosentinel/requirements.txt
RUN pip install --no-cache-dir -r /opt/geosentinel/requirements.txt \
    && ln -s /usr/lib/python3/dist-packages/osgeo /usr/local/lib/python3.11/site-packages/osgeo \
    && useradd --uid 10001 --create-home worker
COPY worker.py /opt/geosentinel/worker.py
USER 10001:10001
WORKDIR /workspace
ENTRYPOINT ["python", "/opt/geosentinel/worker.py"]
