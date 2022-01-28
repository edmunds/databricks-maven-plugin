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

import com.amazonaws.services.s3.AmazonS3;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Uploads resource artifact to an S3 environment path that can be used as CodeDeploy revision.
 */
@Mojo(name = "push-revision")
public class PushRevisionMojo extends BaseDatabricksMojo {

    protected AmazonS3 s3Client;

    @Parameter(property = "file", required = true,
            defaultValue = "${project.build.directory}/${project.build.finalName}.zip")
    protected File file;

    /**
     * The prefix to upload revision to in order for CodeDeploy to pick up the resources and deploy
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

    @Override
    public void execute() throws MojoExecutionException {
        if (file.exists()) {
            S3MojoUtils.uploadFile(createSourceFilePath(), file, getLog(), getS3Client());
        } else {
            getLog().warn(String.format("Target upload file does not exist, skipping: [%s]", file.getPath()));
        }
    }

    protected AmazonS3 getS3Client() {
        if (s3Client == null) {
            s3Client = S3MojoUtils.getS3Client(databricksRepoRegion);
        }
        return s3Client;
    }
}
