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

import com.edmunds.tools.databricks.maven.model.JobEnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

/**
 * Upserts databricks jobs with the given name based on the artifacts job settings json file.
 * <p>
 * This file should be in the resources directory named ${artifactId}-job-settings.json and
 * should be a serialized form of an array of type JobSettingsDTO.
 * <p>
 * NOTE: If a job does not have a unique name, it will fail unless failOnDuplicateJobName=false, in which case only the first one will be updated.
 * <p>
 * no project is split out so we have a mojo that will work with multi-module projects
 */
@Mojo(name = "upsert-job-np", requiresProject = false)
public class UpsertJobMojoNoProject extends UpsertJobMojo {

    /**
     * The serialized job environment dto is required to be passed in a NoProject scenario.
     */
    @Parameter(name = "jobEnvironmentDTOFile", property = "jobEnvironmentDTOFile", required = true)
    private File jobEnvironmentDTOFile;

    @Override
    protected EnvironmentDTOSupplier<JobEnvironmentDTO> createEnvironmentDTOSupplier() {
        return new EnvironmentDTOSupplier<JobEnvironmentDTO>() {
            @Override
            public JobEnvironmentDTO get() throws MojoExecutionException {
                JobEnvironmentDTO serializedJobEnvironment = JobEnvironmentDTO.loadJobEnvironmentDTOFromFile(jobEnvironmentDTOFile);
                //We now set properties that are based on runtime and not buildtime. Ideally this would be enforced.
                //I consider this code ugly
                if (environment != null) {
                    serializedJobEnvironment.setEnvironment(environment);
                }
                return serializedJobEnvironment;
            }

            @Override
            public File getEnvironmentDTOFile() {
                return UpsertJobMojoNoProject.super.dbJobFile;
            }
        };
    }

}
