## Local Docker Development

Create a local docker image mounted from the current code from your repo dir. Changes you make on your local dir will be reflected inside the container.

```
docker run -td -p 5001:5000 -e DEVELOPMENT=True -v /home/common/Development/job_manager/:/code -v /mnt/drobos/zeus/:/mnt/zeusdrobo zeus684440.agr.gc.ca/jobmanager-api:v0.0.9 bash
```

Find your local docker container:

`docker ps`

Connect to the docker container:

`docker exec -it container_shortname bash`

Remove container after use:

`docker kill container_shortname`

## Build and Push Latest Docker Image

When you are satisfied with the latest build, you can build a new image and push it to the private registry. Then you can update the image that your kube deployment uses to complete the upgrade.

Build a new image:
`docker build -f Dockerfile.ubuntu -t ubuntu_base:ver8 . --squash`

When you need to include ENV args for secrets:

`docker build --build-arg GITHUB_PAT=${GITHUB_PAT} -f Dockerfile.ubuntu1804 -t jobmanager-api:ver1 . --squash`

List images (find the image ID for your latest image):
`docker images`

Tag the image with a full repository name so it can be pushed to your private repo:
`docker tag DOCKERIMAGEID zeus684440.agr.gc.ca/ubuntu_base:ver8`

Push the tagged image to the registry:
`docker push zeus684440.agr.gc.ca/ubuntu_base:ver8`

### External Data Files Required by Spatial Ops Module

If you look at the `Dockerfile.django` file, you can see that it copies two folders of external data to locations where the `spatial_ops` modules can access. More information can be found on the Spatial Ops Github page https://github.com/sscullen/spatial_ops.

Follow the link on the ReadMe to download the data. Place the `grid_files` and `data` directories in the common folder in the root of the project.

### Freezing Your Requirements

Pipenv is used to manage packages and python dependencies. The Dockerfile.django file uses pip to install the dependencies so you need to freeze your dependencies and create a new `requirements.txt` before creating a new Docker image. Use the command:

```
pipenv lock --requirements > requirements.txt
```

This will create a fresh `requirements.txt` based on the latest changes to your Pipfile. Make sure to remove any references to your modules that were installed using the -e flag. This will make sure that a fresh copy of your module will be pulled from git during the build process.

WARNING: This is critical because what can happen is your local pipenv env will become out of sync with your docker requirements.txt file (which can happen if you haven't updated the requirements file in a while by freezing.) What results is a Django database created with a pipenv (run django manage.py migrate from your pipenv), and your out of date docker images no longer are compatible with the database schema. To avoid this, make sure to keep your requirements.txt up to date (especially if you are doing a lot of dev from your pipenv environent and run database sync commands from the pipenv environment), or make sure to run all django manage.py database commands from inside your docker containers, which should all be using the same requirements.txt file.

## ConfigMap Inside Kubernetes

A ConfigMap was created using the file `config.yaml` with the following command:
s

```
kubectl create configmap jobmanager-config --from-file=config.yaml
```

Inside the `deployment.yaml`, this ConfigMap is mapped to environment vars inside the container, these env vars should be used in place of a `.env` file or `config.json`. Passwords are now stored in individual Secret objects, one for USGS EarthExplorer and one for the Minio S3 Private key.

## Start the Django app

The exact command used to start the django app will vary depending on if we are in a local development environment or on the cluster.

`python3.7 manage.py runserver 0.0.0.0:5000`

The port will vary depending on the networking setup (ie which ports are exposed and accessible from outside the cluster)

When the job manager is running on kubernetes, there will be a nodeport enabled so that you can access the job manager through the port on the node it is running on.

## Start Celery Workers

### Sen2Agri Worker

The specific celery command to start a worker is like so:

`celery -A jobmanager --loglevel=INFO --concurrency=2 -n sen2agri_worker1@%h -Q sen2agri`

But we need the django settings context enabled, so we use a BASH script to start the worker for us:

`./common/celery_worker.sh`

### Agricarta Worker

There is a different queue for agricarta, to handle those jobs we start the worker differently:

`celery -A jobmanager worker --loglevel=INFO --concurrency=1 -n agricarta_worker1@%h -Q agricarta`

### Celery Beat for periodic tasks

We need to start an instance of celery beat to run the periodic tasks, such as L8Batch Fetch and Submit.

`celery -A jobmanager beat -l debug --scheduler django_celery_beat.schedulers:DatabaseScheduler`

## Add a Job

To submit a new job, you navigate to the URL defined by where your django app is running.

## Update the configmap

When the config.yaml file is updated you need to update the configmap in the cluster like so:

`kubectl create configmap foo --from-file foo.properties -o yaml --dry-run | kubectl replace -f -`

Check the status of the config map:

`kubectl get configmap jobmanager-config -o=yaml`

## Minikube Related (for local kubernetres testing)

Start minikube (with the insecure registry option):

`minikube start --insecure-registry=”zeus684440.agr.gc.ca”`

Check the minikube cluster status:

`minikube status`

List available contexts (prod cluster, local cluster, etc):

`kubectl config get-contexts`

Change contexts from production cluster to minikube cluster. This will allow you to change the cluster that kubectl references:

`kubectl config use-context minikube`

And back to the main cluster:

`kubectl config use-context kubernetes-admin@kubernetes`

Get the minikube node IP address (for node port access):

`kubectl get nodes -o wide`

## Misc Kubernetes Resources

kubectl cheat sheet:

https://kubernetes.io/docs/reference/kubectl/cheatsheet/

## Testing Outside of Kubernetes

5 major steps, all in separate terminals:

### 1. Start the django app

```
pipenv shell

DEVELOPMENT=True python manage.py runserver 0.0.0.0:5000
```

### 2. Start the celery beat process

```
pipenv shell
DEVELOPMENT=True ./common/start_celery_beat.sh
```

### 3. Start the periodic celery worker

```
pipenv shell
DEVELOPMENT=True ./common/celery_worker_periodic.sh
```

### 4. Start the downloader celery worker

```
pipenv shell
DEVELOPMENT=True ./common/celery_worker_downloader.sh
```

### 5. Start the sen2agri celery worker (inside the jobmanager-api-centos7 docker container)

```
 docker run -td -p 5001:5000 -e DEVELOPMENT=True -v /home/common/Development/job_manager/:/code -v /mnt/drobos/zeus/:/mnt/zeusdrobo zeus684440.agr.gc.ca/jobmanager-api-centos7:v0.0.13 bash

 docker ps

 docker exec -it CONTAINER_NAME_FROM_ABOVE bash

 Docker container terminal# DEVELOPMENT=True ./common/celery_worker_sen2agri.sh
```

NOTE: Steps 1-4 could also be executed from inside the jobmanager-api docker container. This would more closely resemble the environment that exists in the kubernetes cluster, but is mostly unnecessary since our expected development environment (Ubuntu 18.04.3 with Python3.7) closely resembles the jobmanager-api container (or should, see the note above about requirements.txt)
