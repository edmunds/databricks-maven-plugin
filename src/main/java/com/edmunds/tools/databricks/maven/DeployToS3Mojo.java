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

import java.io.File;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Uploads an artifact to an s3 environment path that can be used by databricks jobs (or airflow)
 * as hardcoded environment paths.
 */
@Mojo(name = "deploy-to-s3")
public class DeployToS3Mojo extends BaseDatabricksS3Mojo {

    /**
     * The local file to upload.
     */
    @Parameter(property = "file", required = true,
            defaultValue = "${project.build.directory}/${project.build.finalName}.${project.packaging}")
    private File file;

    @Override
    protected File getSourceFile() {
        return null;
    }

    /**
     * The prefix to deploy to. This allows for jobs to have hardcoded s3 paths.
     * Its especially useful for airflow dags.
     * The default value is an example of how this could be structured.
     */
    @Parameter(name = "databricksRepoDeployKey", property = "databricks.repo.deploy.key",
            defaultValue = "${project.groupId}/${project.artifactId}/DEPLOYED/"
                    + "${environment}/DEPLOYED.${project.packaging}")
    protected String databricksRepoDeployKey;

    @Override
    protected String getDatabricksRepoKey() {
        return databricksRepoDeployKey;
    }

}
