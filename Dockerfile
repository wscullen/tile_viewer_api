FROM python:3.8-slim-buster AS compile-image

ARG GITHUB_PAT

RUN apt-get update && apt-get install -y --no-install-recommends build-essential git gcc g++ nginx gdal-bin python3-gdal libgdal-dev

RUN python3.8 -m venv /opt/venv

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN CPLUS_INCLUDE_PATH=/usr/include/gdal && C_INCLUDE_PATH=/usr/include/gdal && pip install GDAL==2.4.0
RUN pip install psycopg2-binary==2.8.6
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install git+https://sscullen:$GITHUB_PAT@github.com/sscullen/landsat_downloader.git@v0.0.4#egg=landsat_downloader
RUN pip install git+https://sscullen:$GITHUB_PAT@github.com/sscullen/sentinel_downloader.git@v1.0.1#egg=sentinel_downloader
RUN pip install git+https://sscullen:$GITHUB_PAT@github.com/sscullen/spatial_ops.git@v0.0.2#egg=spatial_ops

COPY . /app

WORKDIR /app

COPY ./common/grid_files/ /opt/venv/lib/python3.8/site-packages/spatial_ops/grid_files
COPY ./common/data/ /opt/venv/lib/python3.8/site-packages/spatial_ops/data

RUN python /app/manage.py collectstatic --no-input

COPY nginx_site.txt /etc/nginx/sites-available/jobmanager

RUN ln -s /etc/nginx/sites-available/jobmanager /etc/nginx/sites-enabled/jobmanager

EXPOSE 80

EXPOSE 443

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--log-level=debug", "jobmanager.wsgi"]