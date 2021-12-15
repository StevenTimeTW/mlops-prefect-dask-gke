from prefect import task, Flow
from prefect.executors import DaskExecutor
from prefect.storage import Docker

import random
from time import sleep

import yaml
with open('../config/secrets.yml', 'r') as file:
    secrets = yaml.safe_load(file)

REGISTRY_URL = secrets['development']['REGISTRY_URL']
PROJECT_NAME = secrets['development']['PROJECT_NAME']
DASK_ADDRESS = secrets['development']['DASK_ADDRESS']

@task
def inc(x):
    sleep(random.random() / 10)
    return x + 1

@task
def dec(x):
    sleep(random.random() / 10)
    return x - 1


@task
def add(x, y):
    sleep(random.random() / 10)
    return x + y


@task(log_stdout=True)
def list_sum(arr):
    return sum(arr)

with Flow("dask-k8") as flow:
    random.seed(123)
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)


if __name__ == '__main__':
    flow.storage = Docker(registry_url=REGISTRY_URL, image_name="dask-k8", image_tag="latest")
    flow.executor = DaskExecutor(DASK_ADDRESS)
    flow.register(project_name=PROJECT_NAME)
