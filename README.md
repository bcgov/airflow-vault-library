# airflow-vault-library
Python library for Apache Airflow to retrieve Secrets from Vault. This is the demo to execute Oracle queries on airflow with retrieved secrets from vault via library

# Prerequisition

## Secrets in Vault

Secrets for DB connection have been configured in Vault under the path pattern as /apps/env/project/service/ in [Vault](https://knox.io.nrs.gov.bc.ca/)

## Broker Account Setup for project/service

Only broker account associated with your team has access to secrets that have been configured via [Broker](https://broker.io.nrs.gov.bc.ca/home)
    - Document for project/service configuration (https://bcgov-nr.github.io/nr-broker/#/)

# Airflow startup locally

Apache Airflow is an open-source workflow management platform for data engineering pipelines. Use docker to run 'docker compose' command under your local folder where has docker-compose.yaml located to start airflow locally

```
docker compose up
```

Configure broker_jwt and role_id associated with team and project/service in airflow variable once airflow up in local environment