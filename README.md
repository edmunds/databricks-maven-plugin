# Databricks Maven Plugin

_This is the maven databricks plugin, which uses the databricks rest api._

### Building, Installing and Running

How to build the project locally:
```mvn clean install```

How to run the project locally (if applicable):


## Running the tests

```mvn clean test```

### End-to-End testing

Please have these set in your .bash_profile
```bash
export DB_USER=myuser
export DB_PASSWORD=mypassword
export DB_URL=my-test-db-instance
export DB_TOKEN=my-db-token
```

```bash
mvn -P run-its clean deploy
```

Please note, this will:
1. create a job, if it does not exist, delete it if it does
2. start the job (e.g. run it once)
3. wait for the job to finish and ensure it's success

## Deployment

Since this code is a library, you do not need to deploy it anywhere, once it passes build, you can just use it in another project.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the process for merging code into master.