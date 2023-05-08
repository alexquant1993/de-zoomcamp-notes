## Setup Python environment
- Create a new conda environment: `conda create -n de-zoomcamp python=3.9`
- Activate the environment: `conda activate de-zoomcamp`
- Install python requirements: `pip install -r requirements.txt`
- Install other dependencies:
    - Package required by sqlalchemy: `pip install psycopg2-binary`
    - pgcli installation: `pip install pgcli`
    - If it doesn't work try:
        - `conda install -c conda-forge pgcli`
        - `pip install -U mycli`

## Prefect dashboard
- Start server: `prefect orion start`
- Configure prefect to communicate with the server: `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`
- Check out the dashboard at: `http://127.0.0.1:4200`
- Setup a block:
    - Create a new block
    - Choose for instance sqlalchemy block
    - Connection Info { "driver": "postgresql+psycopg2", "database": "ny_taxi", "username": "root", "password": "root", "host": "localhost", "port": "5432"}
- Register a block through CLI: `prefect block register -m prefect_gcp`
- Connect to GCS services with Prefect blocks:
    - Create block with GCP credentials (service account key)
    - Create block, choose block and GCS bucket
- Troubleshoot: upload_from_path function was not working properly. It was not recognizing the path and it was uploading the path as a filename. For instance: "path/to/file.csv" was uploaded as a unique file "path/to/file.csv" and not creating the folders path and to. Solution: use latest prefect-gcp package = 0.2.6 and apply path.as_posix() to the to_path argument.

## Prefect deployment - locally
- Build a YAML file: `prefect deployment build flows/03_deployment/parameterized_flows.py:etl_parent_flow -n "Parameterized ETL"`
- Setup parameters in the built YAML file: parameters: `{"color": "yellow", "months": [1,2,3], "year":2021}`
- Running the flow - apply the YAML file and sent metadata to prefect API: `prefect deployment apply etl_parent_flow-deployment.yaml`
- Go to the Orion UI. We should see the deployment model is there.
- Click on Quick run button.
- Select Flow Runs in the left menu. Orion UI should indicate that our run is in Scheduled state. In my case, I see Late state.
- The Scheduled state indicates that our flow a ready to be run but we have no agent picking of this run.
- A agent is a very lightly python process that is living in my executing environment.
- Work queues define the work to be done and agents poll a specific work queue for new work.
- Start an agent: `prefect agent start --work-queue "default"`
- And in the Orion UI, we see now that the run is completed.
- Scheduling a deployment: `prefect deployment build flows/03_deployments/parameterized_flow.py:etl_parent_flow -n etl2 --cron "0 0 * * *" -a`
- We can obtain help on prefect command:
    - `prefect deployment --help`
    - `prefect deployment build --help`
    - `prefect deployment apply --help`

## Run prefect inside a docker container
- Create a dockerfile:
    - `RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir`
        - --trusted-host pypi.python.org: This flag tells pip to trust the specified PyPI host and not to verify SSL certificates. This is useful in cases where SSL verification fails due to corporate firewalls or other security policies.
        - The --no-cache-dir option can be used with pip install to tell the package manager to ignore the cache directory and always download the packages from the remote server.
- Build base docker image: `docker build -t alexquant1993/prefect:zoom .`
- Push the image to docker hub: `docker push alexquant1993/prefect:zoom`
- Create a docker container prefect block. Note that this is also possible from Python:

```python

from prefect.infrastructure.docker import DockerContainer

## alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="alexquant1993/prefect:zoom",  # insert your image here
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("docker-container", overwrite=True)
```
- See a list of available profiles: `prefect profile ls`
- Use a local Orion API server, so the docker container can know where to locate it: `prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"`
- Start the agent: `prefect agent start -q default`
- Run the flow from the command line with certain parameters: `prefect deployment run etl-parent-flow/docker-flow -p "months=[1,2]"`
- What did we learn?: We’ve seen how to bring our code into a Docker container and a Docker image, put that Docker image into our docker hub and run a flow into this container.

## Prefect Cloud and Additional Resources
See [DE Zoomcamp 2.2.7 - Prefect Cloud/Additional resources](https://www.youtube.com/watch?v=gGC23ZK7lr8)

We will see:

- Using Prefect Cloud instead of local Prefect
- Workspaces
- Running flows on GCP

Recommended links:

- [Prefect docs](https://docs.prefect.io/)
- [Prefect Discourse](https://discourse.prefect.io/)
- Prefect Cloud
- Prefect Slack
- Anna Geller GitHub

