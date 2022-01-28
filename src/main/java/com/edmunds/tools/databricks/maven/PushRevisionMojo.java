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
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Uploads resource artifact to an S3 environment path that can be used as CodeDeploy revision.
 */
@Mojo(name = "push-revision")
public class PushRevisionMojo extends UploadToS3Mojo {

    /**
     * The revision zip to upload.
     */
    @Parameter(property = "file", required = true,
            defaultValue = "${project.build.directory}/${project.build.finalName}.zip")
    protected File file;

    /**
     * The prefix to deploy to. This allows for jobs to have hardcoded s3 paths.
     * Its especially useful for airflow dags.
     * The default value is an example of how this could be structured.
     */
    @Parameter(name = "codeDeployRevisionKey", property = "code.deploy.revision.key",
            defaultValue = "${project.groupId}/${project.artifactId}/${project.version}/${project.build.finalName}"
                    + ".zip")
    protected String codeDeployRevisionKey;

    protected String createSourceFilePath() throws MojoExecutionException {
        return createDeployedAliasPath();
    }

    String createDeployedAliasPath() throws MojoExecutionException {
        validateRepoProperties();
        String modifiedDatabricksRepoType = databricksRepoType + "://";
        String modifiedDatabricksRepo = databricksRepo;
        String modifiedDatabricksRepoKey = codeDeployRevisionKey;
        if (databricksRepo.endsWith("/")) {
            modifiedDatabricksRepo = databricksRepo.substring(0, databricksRepo.length() - 1);
        }
        if (codeDeployRevisionKey.startsWith("/")) {
            modifiedDatabricksRepoKey = codeDeployRevisionKey.substring(1);
        }
        return String.format("%s%s/%s", modifiedDatabricksRepoType, modifiedDatabricksRepo, modifiedDatabricksRepoKey);
    }
}
