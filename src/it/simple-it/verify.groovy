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


import com.edmunds.rest.databricks.DTO.JobDTO
import com.edmunds.rest.databricks.DTO.RunLifeCycleStateDTO
import com.edmunds.rest.databricks.DTO.RunResultStateDTO
import com.edmunds.rest.databricks.DTO.RunStateDTO
import com.edmunds.rest.databricks.DatabricksRestException
import com.edmunds.rest.databricks.DatabricksServiceFactory
import com.edmunds.rest.databricks.service.JobService
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.DirectoryFileFilter
import org.apache.commons.io.filefilter.SuffixFileFilter
import org.apache.commons.lang3.StringUtils

//TODO couldn't use the Environment class here?
def databricksServiceFactory = DatabricksServiceFactory.Builder
        .createUserPasswordAuthentication(System.getenv("DB_USER"), System.getenv("DB_PASSWORD"), System.getenv("DB_URL")).build();
def jobService = databricksServiceFactory.getJobService()
def databricksIntegrationTestJobName = "bde.tools.integration-test/databricks-maven-plugin-stream-it/QA"

def job = validateJob(jobService, databricksIntegrationTestJobName)

validateWorkspaceExport()

cleanupWorkspace(databricksServiceFactory)

validateJobRun(jobService, job)

private JobDTO validateJob(JobService jobService, String databricksIntegrationTestJobName) {
    def jobsByName = jobService.getJobsByName(databricksIntegrationTestJobName)
    assert jobsByName.size() == 1

    def job = jobsByName.get(0)

    def parameters = job.getSettings().getSparkSubmitTask().getParameters()

    def jarPath = parameters[parameters.length - 2]

    def repoPath = System.getenv("DB_REPO")
    assert jarPath == "s3://" + repoPath + "/artifacts/com.edmunds.bde.tools.integration-test/databricks-maven-plugin-stream-it/1.0-SNAPSHOT/databricks-maven-plugin-stream-it-1.0-SNAPSHOT.jar"

    /**
     * We set a build time property in the pom, that passes through to the job config, which passes through to databricks.
     * This allows us to verify that the job we're looking at is not a stale one.
     */
    def buildTimeFromJob = parameters[parameters.length - 1]

    def jobTime = Integer.parseInt(buildTimeFromJob.substring(buildTimeFromJob.length() - 4))
    println("databricks jobTime property: " + jobTime)
    def mavenBuildTime = Integer.parseInt(buildTimeFromJob.substring(buildTime.length() - 4))
    println("maven build time property: " + mavenBuildTime)

    //when the job was created, and the maven build started should be within a minute
    assert Math.abs(jobTime - mavenBuildTime) <= 1

    job
}


cleanupJob(jobService, job, databricksIntegrationTestJobName)

private void validateJobRun(JobService jobService, JobDTO job) {
    def runs = jobService.listRuns(job.getJobId(), true, 0, 1000).getRuns()

    assert runs.length == 1

    def runState = runs[0].getState()
    runState = waitForRunToFinish(runState, jobService, job)
    assert runState.getResultState() == RunResultStateDTO.SUCCESS
}

private RunStateDTO waitForRunToFinish(RunStateDTO runState, JobService jobService, JobDTO job) {
    def runs
    while (runState.getLifeCycleState() == RunLifeCycleStateDTO.PENDING || runState.getLifeCycleState() == RunLifeCycleStateDTO.RUNNING) {
        Thread.sleep(30000)
        println("waiting on the run to finish, current state: " + runState.getLifeCycleState())

        runs = jobService.listRuns(job.getJobId(), false, 0, 1000).getRuns()
        runState = runs[0].getState()
    }
    runState
}

private void cleanupJob(JobService jobService, JobDTO job, String databricksIntegrationTestJobName) {
    def jobsByName
    jobService.deleteJob(job.getJobId())

    jobsByName = jobService.getJobsByName(databricksIntegrationTestJobName)
    assert jobsByName.size() == 0
}

private void validateWorkspaceExport() {
    def targetBase = targetDir + "/it/simple-it/target/export"

    Collection<File> files = FileUtils.listFiles(new File(targetBase),
            new SuffixFileFilter("scala", "py"),
            DirectoryFileFilter.DIRECTORY);

    assert files.size() == 14

    def expectedPaths = [
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2/level3/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2/level3a/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2/level3b/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2a/level3/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2a/level3a/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2a/level3b/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2a/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2b/level3/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2b/level3a/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2b/level3b/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/level2b/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/test.scala",
            "/bde.tools.integration-test/databricks-maven-plugin-stream-it/level1/test2.py",
    ]

    def observedPaths = files.collect() { StringUtils.substringAfter(it.getPath(), targetBase) }

    assert expectedPaths == observedPaths
}

private void cleanupWorkspace(DatabricksServiceFactory databricksServiceFactory) {
    def workspaceService = databricksServiceFactory.workspaceService
    try {
        workspaceService.delete("/bde.tools.integration-test", true)
    } catch (DatabricksRestException e) {
        //there is no nice way to check if the resource already exists
        if (!e.getMessage().contains("RESOURCE_DOES_NOT_EXIST")) {
            throw e
        }
    }
}