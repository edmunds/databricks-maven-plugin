# Databricks Maven Plugin

_This is the maven databricks plugin, which uses the databricks rest api._

## Build Status
[![Build Status](https://travis-ci.org/edmunds/databricks-maven-plugin.svg?branch=master)](https://travis-ci.org/edmunds/databricks-maven-plugin)

## API Overview

[![Javadocs](http://www.javadoc.io/badge/com.edmunds/databricks-maven-plugin.svg)](http://www.javadoc.io/doc/com.edmunds/databricks-maven-plugin)


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

## Configuring

It is recommended that you use maven profiles to allow for credentials per an environment to be defined.
```xml
         <!-- Databricks QA Credentials -->
         <profile>
             <id>QA</id>
             <build>
                 <plugins>
                     <plugin>
                         <groupId>com.edmunds</groupId>
                         <artifactId>databricks-maven-plugin</artifactId>
                         <version>${oss-databricks-maven-plugin-version}</version>
                         <configuration>
                             <bucketName>${s3-bucket-where-you-want-to-store-jars}</bucketName>
                             <!-- This is used to be able to allow for conditional configuration in job settings -->
                             <environment>QA</environment>
                             <host>${qa-host-here}</host>
                             <user>${qa-user-here}</user>
                             <password>${qa-password-here}</password>
                         </configuration>
                     </plugin>
                 </plugins>
             </build>
         </profile>
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the process for merging code into master.