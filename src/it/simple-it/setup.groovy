/*
 *  Copyright 2018 Edmunds.com, Inc.
 *  
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *  
 *          http://www.apache.org/licenses/LICENSE-2.0
 *  
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */


import com.edmunds.rest.databricks.DatabricksServiceFactory
import com.edmunds.rest.databricks.DatabricksRestException
import com.edmunds.rest.databricks.service.JobService
import com.edmunds.rest.databricks.service.WorkspaceService

//TODO couldn't use the Environment class here?
def databricksIntegrationTestJobName = "bde.tools.integration-test/databricks-maven-plugin-stream-it/QA"
def databricksServiceFactory = DatabricksServiceFactory.Builder
        .createServiceFactoryWithUserPasswordAuthentication(System.getenv("DB_USER"), System.getenv("DB_PASSWORD"), System.getenv("DB_URL")).build();
def jobService = databricksServiceFactory.getJobService()
def workspaceService = databricksServiceFactory.workspaceService

cleanupExistingJob(jobService, databricksIntegrationTestJobName)

cleanupExistingWorkspace(workspaceService)

private void cleanupExistingJob(JobService jobService, String databricksIntegrationTestJobName) {
    def jobsByName = jobService.getJobsByName(databricksIntegrationTestJobName)
    assert jobsByName.size() <= 1

    if (jobsByName.size() == 1) {
        println("tearing down existing integration tests job")
        jobService.deleteJob(jobsByName.get(0).getJobId())
    } else {
        println("no job to tear down, moving on")
    }

    jobsByName = jobService.getJobsByName(databricksIntegrationTestJobName)
    assert jobsByName.size() == 0
}

private void cleanupExistingWorkspace(WorkspaceService workspaceService) {
    try {
        workspaceService.delete("/bde.tools.integration-test", true)
        println("tore down existing integration tests workspace")
    } catch (DatabricksRestException e) {
        //there is no nice way to check if the resource already exists
        if (!e.getMessage().contains("RESOURCE_DOES_NOT_EXIST")) {
            throw e
        } else {
            println("no workspace to tear down, moving on")
        }
    }
}

