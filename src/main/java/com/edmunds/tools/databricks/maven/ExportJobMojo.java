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

import com.edmunds.rest.databricks.DTO.JobDTO;
import com.edmunds.rest.databricks.DTO.JobsDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Exports a job as a JobSettingsDTO json object. Will emit multiple, if job name is not unique.
 */
@Mojo(name = "export-job", requiresProject = true)
public class ExportJobMojo extends BaseDatabricksMojo {

    /**
     * The databricks job name to export.
     */
    @Parameter(property = "jobName", required = true)
    private String jobName;

    @Override
    public void execute() throws MojoExecutionException {
        List<Long> jobIds = getJobIds();

        if (jobIds.size() == 0) {
            getLog().warn(String.format("No jobs found with name: [%s]", jobName));
        }

        for (Long jobId : jobIds) {
            try {
                JobDTO jobDTO = getDatabricksServiceFactory().getJobService().getJob(jobId);
                getLog().info("\n" + ObjectMapperUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jobDTO));
            } catch (DatabricksRestException | IOException e) {
                throw new MojoExecutionException(String.format("Could not get job for id: [%s]", jobId), e);
            }
        }
    }

    List<Long> getJobIds() throws MojoExecutionException {
        ArrayList<Long> jobIds = new ArrayList<>();
        try {
            JobsDTO jobs = getDatabricksServiceFactory().getJobService().listAllJobs();
            for (JobDTO jobDTO : jobs.getJobs()) {
                if (jobDTO.getSettings().getName().equals(jobName)) {
                    jobIds.add(jobDTO.getJobId());
                }
            }
        } catch (DatabricksRestException | IOException e) {
            throw new MojoExecutionException("Could not list clusters.", e);
        }

        return jobIds;
    }
}
