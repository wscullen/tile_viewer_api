FROM python:3.8-slim-buster AS compile-image

ARG GITHUB_PAT

RUN apt-get update && apt-get install -y --no-install-recommends build-essential git gcc g++ nginx gdal-bin python3-gdal python-gdal libgdal-dev

RUN python3.8 -m pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`

RUN python3.8 -m venv /opt/venv

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install psycopg2-binary
RUN pip install git+https://sscullen:$GITHUB_PAT@github.com/sscullen/landsat_downloader.git@v0.0.4#egg=landsat_downloader
RUN pip install git+https://sscullen:$GITHUB_PAT@github.com/sscullen/sentinel_downloader.git@v1.0.1#egg=sentinel_downloader
RUN pip install git+https://sscullen:$GITHUB_PAT@github.com/sscullen/spatial_ops.git@v0.0.2#egg=spatial_ops

COPY . /app

FROM python:3.8-slim-buster AS build-image

RUN apt-get update && apt-get install -y --no-install-recommends nginx gdal-bin python3-gdal

COPY --from=compile-image /opt/venv /opt/venv

COPY --from=compile-image /app /app

WORKDIR /app

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY --from=compile-image /app/common/grid_files/ /usr/local/lib/python3.8/dist-packages/spatial_ops
COPY --from=compile-image /app/common/data/ /usr/local/lib/python3.8/dist-packages/spatial_ops

RUN python /app/manage.py collectstatic --no-input

COPY nginx_site.txt /etc/nginx/sites-available/jobmanager

RUN ln -s /etc/nginx/sites-available/jobmanager /etc/nginx/sites-enabled/jobmanager

EXPOSE 80

EXPOSE 443

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--log-level=debug", "jobmanager.wsgi"]