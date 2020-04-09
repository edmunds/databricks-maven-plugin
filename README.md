# Databricks Maven Plugin

_This is the maven databricks plugin, which uses the databricks rest api._

## Build Status
[![Build Status](https://travis-ci.org/edmunds/databricks-maven-plugin.svg?branch=master)](https://travis-ci.org/edmunds/databricks-maven-plugin)

## API Overview

[![Javadocs](http://www.javadoc.io/badge/com.edmunds/databricks-maven-plugin.svg)](http://www.javadoc.io/doc/com.edmunds/databricks-maven-plugin)

## Prerequisites

For Users:
- You have a databricks account
- You are somewhat familiar with Maven and have maven installed
- You have an s3 bucket (we will call databricksRepo) that you will use to store your artifacts. 
- You have AWS keys that can write to this s3 bucket
- Databricks has access to an IAM Role that can read from this bucket.

For Contributors:
- You need to be able execute an integration test that will actually do things on your databricks account.

## Configuring

### System Properties
For databricks specific properties we also support system properties. This can be useful for when you don't want tokens or passwords
stored in a pom or a script and instead want it to be available on a build server.
Currently the following environment properties are supported:
DB_URL -> my-databrics.cloud.databricks.com
DB_TOKEN -> dapiceexampletoken
DB_USER -> my_user
DB_PASSWORD -> my_password

We can continue to support more system properties in the future if users have a compelling reason for it.

### AWS Credentials
For the upload mojo that uploads your artifact to s3, the default aws provider chain is used. As long as you have appropriate permissions on that chain
it should just work.

### All other properties

For all other properties we support configuration in the following ways:
1. via configuration in the mojo
2. via property configuration on the command line or in the <properties> section

### Examples

If you would like to setup default profiles for users, you can take the following approach.
NOTE: if you define like below, you cannot override via CLI args unless you use project properties as well.
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
                             <databricksRepo>${which bucket you want to use to store databricks artifacts}</databricksRepo>
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

Yet another option is to provide all of your credentials when you call the plugin.
You can even rely on System Properties or the default aws provider chain 
for the host/user/password OR token for databricks rest client. Please see End to End testing section or the
BaseDatabricksMojo for information on these system properties. 

```sh
mvn databricks:upload-to-s3 -Ddatabricks.repo=my-repo -Denvironment=QA
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
Here is how you can install a library to a cluster WITHOUT restarting.
```bash
mvn databricks:library -Dlibrary.command=INSTALL -Dclusters=data_engineering -Drestart=false
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

### Use Case 4 - Exporting Notebooks from a Workspace
This command demonstrates how you can export notebooks from a databricks workspace to local.

By default, it uses your maven groupId and artifactId as the databricks workspace prefix:
```bash
mvn databricks:export-workspace
```

If you need to override the default prefix, you can do so here:
```bash
mvn databricks:export-workspace -DworkspacePrefix=deployments/eas-pipeline
```

### Use Case 5 - Upsert a Job to a Workspace
You must have a job definition file. This file should be in the resources directory named databricks-plugin/databricks-job-settings.json and should be a serialized form of an array of type JobSettingsDTO. 
Note that this file is a template, that has access to both the java system properties, as well as the maven project data. It uses freemarker to merge this file, with that data.

```json
[
 {
   //There is validation rules around job names based on groupId and artifactId these can be turned off
   "name": "myTeam/myArtifact",
   "new_cluster": {
     "spark_version": "4.1.x-scala2.11",
     "aws_attributes": {
       "first_on_demand": 1,
       "availability": "SPOT_WITH_FALLBACK", // Can also set to SPOT
       "instance_profile_arn": "<your_arn>",
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
        "team": "myTeam"
      },
     "autotermination_minutes": 0,
     "enable_elastic_disk": false
   },
   "existing_cluster_id": null,
    "spark_conf" : {
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
    //If you emit this section, it will automatically be added to your job
    "libraries": [
      {
        "jar": "s3://${projectProperties['databricks.repo']}/${projectProperties['databricks.repo.key']}"
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
Note that you can specify different libraries types e.g. Maven:
```json
    "libraries": [
      {
        "jar": "s3://${projectProperties['databricks.repo']}/${projectProperties['databricks.repo.key']}"
      },
      {
        "maven": {
          "coordinates": "org.apache.hbase:hbase-common:1.2.0",
          "repo": "",
          "exclusions": []
        }
      }
    ]
```

To upsert your job run the following:
You can invoke it manually, like so, or attach it as an execution (see case 2 for example):
```json
#deploys the current version
mvn databricks:upsert-job

#deploys a specific version
mvn databricks:upsert-job -Ddeploy-version=1.0

#you don't want validation! 
#If so, it could be good to create an issue and let us know where our validation rules are too specific
mvn databricks:upsert-job -Dvalidate=false
```

You can use freemarker templating like so:
```json
      <#if environment == "QA" || environment == "DEV">
      "node_type_id": "r3.xlarge",
      "driver_node_type_id": "r3.xlarge",
      "num_workers": 5,
      <#else>
      "node_type_id": "r3.4xlarge",
      "driver_node_type_id": "r3.xlarge",
      "num_workers": 10,
      </#if>
```

For additional information please consult:
https://docs.databricks.com/api/latest/jobs.html#create
And the JobSettingsDTO in:
https://www.javadoc.io/doc/com.edmunds/databricks-rest-client/

### Use Case 5 - Control a Job (start, stop, restart)
You can control a job (stop it, start it, restart it) via this mojo. 
There is 1 required property:jobCommand. You can add it to your configuration section, or invoke manually, like so:

(note: you can override the jobName in this example, which is otherwise derived from the job settings json file)
```bash
mvn databricks:job -Djob.command=STOP
mvn databricks:job -Djob.command=START
mvn databricks:job -Djob.command=RESTART
```

### Use Case 6 - Control a Cluster (start, stop)
You can control a cluster (stop it, start it) via this mojo. 
```bash
mvn databricks:cluster -Dcluster.command=STOP -Dclusters=cluster_name1,cluster_name2
mvn databricks:cluster -Dcluster.command=START -Dclusters=cluster_name1,cluster_name2
```

### Use Case 7 - Upsert clusters
You're able to create or recreate clusters. 
To upsert cluster you should call the following command:
```bash
mvn databricks:upsert-cluster
```
By default cluster config should be located at
```bash
${project.build.resources[0].directory}/databricks-plugin/databricks-cluster-settings.json
```
but you can overwrite the location:
```bash
mvn databricks:upsert-cluster -DdbClusterFile=${project.build.resources[0].directory}/my.json
```
And also you ought to do it in case of out-of-project plugin usage.

Here you can see an example of cluster configuration:
```json
[
  {
    "num_workers": 2,
    "cluster_name": "myCluster",
    "spark_version": "5.1.x-scala2.11",
    "aws_attributes": {
      "first_on_demand": 1,
      "availability": "SPOT_WITH_FALLBACK",
      "zone_id": "us-west-2b",
      "instance_profile_arn": "<your_arn>",
      "spot_bid_price_percent": 100,
      "ebs_volume_type": "GENERAL_PURPOSE_SSD",
      "ebs_volume_count": 3,
      "ebs_volume_size": 100
    },
    "node_type_id": "r4.xlarge",
    "spark_env_vars": {
      "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 10,
    "artifact_paths": [
      "s3://${projectProperties['databricks.repo']}/${groupId}/${artifactId}/${version}/${artifactId}-${version}.jar"
    ],
    "driver_node_type_id": "r4.xlarge",
    "spark_conf": {},
    "custom_tags": {},
    "ssh_public_keys": []
  }
]
```

Note that you can simultaneously manage multiple clusters with a single .json file by adding multiple array elements.
All configurations will be applied at once in a parallel manner.

## Building, Installing and Running

How to build the project locally:
```mvn clean install```

- Not required! Because you can build and develop without it, but you will likely want Lombok configured with your IDEA:
https://projectlombok.org/setup/intellij

How to run the project locally (if applicable):


### Running the tests

```mvn clean test```


### End-to-End testing

Please have these set in your .bash_profile.

```bash
export DB_USER=myuser
export DB_PASSWORD=mypassword
export DB_URL=my-test-db-instance
export DB_TOKEN=my-db-token
export DB_REPO=my-s3-bucket/my-artifact-location
export INSTANCE_PROFILE_ARN=arn:aws:iam::123456789:instance-profile/MyDatabricksRole
```

```bash
mvn clean -P run-its install
```

Please note, this will:
1. create a job, if it does not exist, delete it if it does
2. start the job (e.g. run it once)
3. wait for the job to finish and ensure it's success

## Releasing

Please see the contributing section on how to RELEASE.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the process for merging code into master.
