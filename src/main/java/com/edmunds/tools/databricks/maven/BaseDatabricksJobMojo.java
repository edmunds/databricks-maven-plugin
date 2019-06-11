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
import com.edmunds.rest.databricks.service.JobService;
import com.edmunds.tools.databricks.maven.model.JobEnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;

/**
 * Base class for Databricks Job Mojos.
 */
public abstract class BaseDatabricksJobMojo extends BaseDatabricksMojo {

    /**
     * The databricks job json file that contains all of the information for how to create one or more databricks jobs.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-job-settings.json", property = "dbJobFile")
    private File dbJobFile;

    /**
     * If true, any command that involves working by databricks job name, will fail if more then one job exists
     * with that job name.
     */
    @Parameter(property = "failOnDuplicateJobName")
    boolean failOnDuplicateJobName = true;

    // These fields are being instantiated within getters to await @Parameter fields initialization
    private SettingsUtils<JobEnvironmentDTO, JobSettingsDTO> settingsUtils;
    private EnvironmentDTOSupplier<JobEnvironmentDTO> environmentDTOSupplier;
    private SettingsInitializer<JobEnvironmentDTO, JobSettingsDTO> settingsInitializer;

    public SettingsUtils<JobEnvironmentDTO, JobSettingsDTO> getSettingsUtils() throws MojoExecutionException {
        if (settingsUtils == null) {
            settingsUtils = new SettingsUtils<>(
                    JobSettingsDTO[].class, "/default-job.json", dbJobFile,
                    getEnvironmentDTOSupplier(), getSettingsInitializer());
        }
        return settingsUtils;
    }

    EnvironmentDTOSupplier<JobEnvironmentDTO> getEnvironmentDTOSupplier() {
        if (environmentDTOSupplier == null) {
            environmentDTOSupplier = () -> {
                if (StringUtils.isBlank(databricksRepo)) {
                    throw new MojoExecutionException("databricksRepo property is missing");
                }
                return new JobEnvironmentDTO(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
            };
        }
        return environmentDTOSupplier;
    }

    SettingsInitializer<JobEnvironmentDTO, JobSettingsDTO> getSettingsInitializer() {
        if (settingsInitializer == null) {
            settingsInitializer = new BaseDatabricksJobMojoSettingsInitializer(validate, prefixToStrip);
        }
        return settingsInitializer;
    }

    JobService getJobService() {
        return getDatabricksServiceFactory().getJobService();
    }

    Long getJobId(String jobName) throws MojoExecutionException {
        try {
            JobDTO jobDTO = getJobService().getJobByName(jobName, failOnDuplicateJobName);
            if (jobDTO == null) {
                return null;
            }
            return jobDTO.getJobId();
        } catch (DatabricksRestException | IOException | IllegalStateException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

}
