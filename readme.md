# Create virtual env
`python -m venv venv`
`venv\Scripts\activate`
# Install requirements
`pip install -r requirements.txt`
# How to run
## Init airflow
`docker-compose up --build airflow-init`
## Run containers
`docker-compose up`