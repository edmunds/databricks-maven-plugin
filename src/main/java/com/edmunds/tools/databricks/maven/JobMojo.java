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

package com.edmunds.tools.databricks.maven;

import com.edmunds.rest.databricks.DTO.RunNowDTO;
import com.edmunds.rest.databricks.DTO.RunsDTO;
import com.edmunds.rest.databricks.DTO.jobs.JobSettingsDTO;
import com.edmunds.rest.databricks.DTO.jobs.RunDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Controls a given databricks job [start\stop\restart].
 * <p>
 * NOTE 1: If a job does not have a unique name, it will fail unless failOnDuplicateJobName=false, in which case only the first one will be updated.
 * </p>
 * <p>
 * NOTE 2: If a job has more than 1 active run, ALL of them will be cancelled on STOP\RESTART.
 * </p>
 */
@Mojo(name = "job", requiresProject = true)
public class JobMojo extends BaseDatabricksJobMojo {

    private static final int RUNS_OFFSET = 0;
    private static final int RUNS_LIMIT = 1000;
    private static final String STREAM = "stream";

    /**
     * Job command to execute.
     */
    public enum JobCommand {
        START, STOP, RESTART
    }

    /**
     * The databricks job name to operate on.
     */
    @Parameter(property = "jobName")
    private String jobName;

    /**
     * The databricks job command to execute.<br>
     *
     * STOP - stop a job.<br>
     * START - start a job.<br>
     * RESTART - restart a running job.<br>
     */
    @Parameter(defaultValue = "RESTART", property = "job.command", required = true)
    private JobCommand command;

    /**
     * Whether the command should be only executed on streaming jobs only.
     * Whether a job is streaming is based on its job name.
     */
    @Parameter(property = "streamingOnly", defaultValue = "true")
    private boolean streamingOnly;

    @Override
    public void execute() throws MojoExecutionException {

        if (isBlank(jobName)) {
            for (JobSettingsDTO settingsDTO : getSettingsUtils().buildSettingsDTOsWithDefaults()) {
                jobName = settingsDTO.getName();
                controlJob();
            }
        } else {
            controlJob();
        }

    }

    private void controlJob() throws MojoExecutionException {
        if (streamingOnly && !containsIgnoreCase(jobName, STREAM)) {
            getLog().warn(String.format("Job: [%s] is not streaming. Either include '%s' in the name if this is incorrect, or set streamingOnly=false to override this.", jobName, STREAM));
            return;
        }

        Long jobId = getJobId(jobName);
        if (jobId != null) {
            getLog().info(String.format("Preparing to run command: [%s] on: https://%s/#job/%s", command, host, jobId));

            try {
                switch (command) {
                    case STOP: {
                        stopActiveRuns(jobId);
                        break;
                    }
                    case START: {
                        startRun(jobId);
                        break;
                    }
                    case RESTART: {
                        stopActiveRuns(jobId);

                        while (getRunDTOs(jobId).length > 0) {
                            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
                            getLog().info("Waiting for jobs to finish stopping.");
                        }

                        startRun(jobId);
                        break;
                    }
                    default: {
                        throw new MojoExecutionException("this should not happen");
                    }
                }
            } catch (DatabricksRestException | IOException e) {
                throw new MojoExecutionException(String.format("Could not control job: [%s] with command: [%s]", jobId, command.name()), e);
            }
        } else {
            getLog().error(String.format("No job id found for: [%s]", jobName));
        }
    }

    RunNowDTO startRun(Long jobId) throws DatabricksRestException, IOException, MojoExecutionException {

        if (getRunDTOs(jobId).length != 0) {
            throw new MojoExecutionException(String.format("Job: [%s] already has an existing active run.", jobId));
        }

        RunNowDTO runNowDTO = getJobService().runJobNow(jobId);
        long numberInJob = runNowDTO.getNumberInJob();

        getLog().info(String.format("Job started, url: https://%s/#job/%s/run/%s", host, jobId, numberInJob));

        return runNowDTO;
    }

    void stopActiveRuns(Long jobId) throws IOException, DatabricksRestException {
        RunDTO[] runs = getRunDTOs(jobId);
        for (RunDTO runDTO : runs) {
            long runId = runDTO.getRunId();
            long numberInJob = runDTO.getNumberInJob();

            getLog().info(String.format("Stopping run: https://%s/#job/%s/run/%s", host, jobId, numberInJob));

            getJobService().cancelRun(runId);
        }
    }

    RunDTO[] getRunDTOs(Long jobId) throws DatabricksRestException, IOException {
        RunsDTO runsDTO = getJobService().listRuns(jobId, true, RUNS_OFFSET, RUNS_LIMIT);
        return ObjectUtils.defaultIfNull(runsDTO.getRuns(), new RunDTO[]{});
    }

    /**
     * NOTE - only for unit testing!
     */
    void setStreamingOnly(boolean streamingOnly) {
        this.streamingOnly = streamingOnly;
    }

}
