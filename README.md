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

You will want a property somewhere in your pom with this value which represents
where on s3 you want to store your artifacts:
```xml
<databricks.repo>edmunds-repos/artifacts</databricks.repo>
```

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
                             <bucketName>${databricks.repo}</bucketName>
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

## Instructions
### Use Case 1 - Uploading an Artifact to s3, for Databricks
```bash
#This approach will build, run tests and copy your artifacts to s3.
mvn clean deploy

#This approach will only load your artifacts to s3.
mvn databricks:upload-to-s3
```

### Use Case 2 - Attaching a Jar to a Cluster
This will install a library on a databricks cluster, taking care of any restarts necessary.
```bash
mvn clean install databricks:upload-to-s3 \
databricks:library -Dlibrary.command=INSTALL -Dclusters={myDatabricksCluster}
```

### Use Case 3 - Exporting Notebooks to a Workspace
This command demonstrates exporting notebooks to a workspace
as well as uploading a jar and attaching it to a cluster, which is a common
operation when you have a notebook that also depends on library code.
```bash
mvn clean install databricks:upload-to-s3 \
databricks:library databricks:import-workspace \
-Dlibrary.command=INSTALL -Dclusters=sam_test
```

### Use Case 4 - Upsert a Job to a Workspace
You must have a job definition file. This file should be in the resources directory named databricks-plugin/databricks-job-settings.json and should be a serialized form of an array of type JobSettingsDTO. 
```json
[
 {
   "name": "${groupWithoutCompany}/${artifactId}",
   "new_cluster": {
     "spark_version": "4.1.x-scala2.11",
     "aws_attributes": {
       "first_on_demand": 1,
       "availability": "SPOT_WITH_FALLBACK", // Can also set to SPOT
       "instance_profile_arn": arn:aws:iam::625959542261:instance-profile/DatabricksS3Role,
       "spot_bid_price_percent": 100,
       "ebs_volume_type": "GENERAL_PURPOSE_SSD",
       "ebs_volume_count": 1,
       "ebs_volume_size": 100
     },
    "driver_node_type_id" : "r4.xlarge",
     "node_type_id": "m4.large",
     "num_workers": 1,
    "autoscale" : {
      "min_workers" : 1,
      "max_workers" : 3
    },
    "custom_tags": {
        "team": "${groupWithoutCompany}"
      },
     "autotermination_minutes": 0,
     "enable_elastic_disk": false
   },
   "existing_cluster_id": null,
    "spark_conf" : {
//      "spark.databricks.delta.preview.enabled" : "true" if you are using delta
    "spark.databricks.delta.retentionDurationCheck.enabled" : "false"
    },
   "timeout_seconds": 10800, //3hrs
  "schedule" : {
    "quartz_cron_expression" : "0 0/30 * ? * * *",
    "timezone_id" : "America/Los_Angeles"
  },
     "spark_jar_task": {
      "main_class_name": "com.edmunds.dwh.VehicleInventoryHistoryDriver"
    },
    "libraries": [
      {
        "jar": "s3://${projectProperties['databricks.repo']}/${groupId}/${artifactId}/${version}/${artifactId}-${version}.jar"
      }
],
  "email_notifications" : {
    "on_failure" : [ "myEmail@email.com" ],
    "on_start" : null,
    "on_success" : null
  },
   "retry_on_timeout": false,
   "max_retries": 0,
   "min_retry_interval_millis": 0,
   "max_concurrent_runs": 1
 }
]
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the process for merging code into master.