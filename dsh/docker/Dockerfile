FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm@sha256:b726eb94f42fcddb10056835f2c474c9f9e12e717ba2b2d2f9a8b1d78feeb68b
USER root
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MPLCONFIGDIR=/tmp/matplotlib
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update && apt-get install -y --no-install-recommends gdal-bin python3-gdal libgomp1 ca-certificates fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*
# ESRI Personal Geodatabases (the `.mdb` data packs) are read through GDAL's
# PGeo driver, which needs the MDB Tools ODBC driver. The CLI package alone is
# not enough (verified: without this, PGeo reports "MDB Tools driver not found"),
# and the driver entry is kept in two places: the system config for tooling, and
# /opt/geosentinel/odbcinst.ini, which the worker copies into a writable tmpfs at
# startup because the sandbox root filesystem is read-only.
RUN apt-get update && apt-get install -y --no-install-recommends mdbtools odbc-mdbtools odbcinst unixodbc \
    && driver="$(find /usr/lib -name 'libmdbodbc*.so*' | head -1)" \
    && test -n "$driver" \
    && mkdir -p /opt/geosentinel \
    && printf '[MDBTools]\nDescription=MDB Tools ODBC\nDriver=%s\n' "$driver" | tee /etc/odbcinst.ini /opt/geosentinel/odbcinst.ini > /dev/null \
    && odbcinst -q -d | grep -q MDBTools \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /opt/geosentinel/requirements.txt
RUN pip install --no-cache-dir -r /opt/geosentinel/requirements.txt \
    && ln -s /usr/lib/python3/dist-packages/osgeo /usr/local/lib/python3.11/site-packages/osgeo \
    && useradd --uid 10001 --create-home worker \
    && mkdir -p /tmp/matplotlib && chmod 1777 /tmp/matplotlib \
    && python -c "import matplotlib.font_manager as fm; fm._load_fontmanager(try_read_cache=False)" \
    && chmod -R a+rX /tmp/matplotlib
COPY worker.py /opt/geosentinel/worker.py
COPY boundaries.py gis_dispatch.py /opt/geosentinel/
USER 10001:10001
WORKDIR /workspace
ENTRYPOINT ["python", "/opt/geosentinel/worker.py"]
