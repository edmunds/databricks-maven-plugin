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
import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;

import java.io.IOException;
import java.util.List;

import static com.edmunds.tools.databricks.maven.util.SettingsUtils.OBJECT_MAPPER;

/**
 * Upserts databricks jobs with the given name based on the artifacts job settings json file.
 * <p>
 * This file should be in the resources directory named ${artifactId}-job-settings.json and
 * should be a serialized form of an array of type JobSettingsDTO.
 * <p>
 * NOTE: If a job does not have a unique name, it will fail unless failOnDuplicateJobName=false, in which case only the first one will be updated.
 */
@Mojo(name = "upsert-job", requiresProject = true)
public class UpsertJobMojo extends BaseDatabricksJobMojo {

    @Override
    public void execute() throws MojoExecutionException {
        upsertJobSettings();
    }

    private void upsertJobSettings() throws MojoExecutionException {
        List<JobSettingsDTO> jobSettingsDTOS = getSettingsUtils().buildSettingsDTOsWithDefaults();
        for (JobSettingsDTO settingsDTO : jobSettingsDTOS) {
            try {
                getJobService().upsertJob(settingsDTO, failOnDuplicateJobName);
                JobDTO job = getJobService().getJobByName(settingsDTO.getName(), failOnDuplicateJobName);
                if (job == null) {
                    throw new MojoExecutionException("Error creating the job... It can't be found!");
                } else {
                    getLog().info("Updated/Created Job at: " + getJobService().getJobLink(job.getJobId()));
                }
            } catch (DatabricksRestException | IOException e) {
                String jobJson = null;
                try {
                    jobJson = OBJECT_MAPPER.writeValueAsString(jobSettingsDTOS);
                } catch (JsonProcessingException jpe) {
                    getLog().error("Fail to stringfy json", jpe);
                }
                throw new MojoExecutionException(String.format("Could not upsert job: [%s] with:\n%s", settingsDTO
                        .getName(), jobJson),
                        e);
            }
        }
    }
}
