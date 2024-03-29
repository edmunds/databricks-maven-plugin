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

import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Uploads resource artifact to an S3 environment path that can be used during deployment.
 */
@Mojo(name = "push-resource", defaultPhase = LifecyclePhase.DEPLOY)
public class PushResourceMojo extends BaseDatabricksS3Mojo {

    @Parameter(property = "file", required = true,
            defaultValue = "${project.build.directory}/${project.build.finalName}.zip")
    private File file;

    @Override
    protected File getSourceFile() {
        return file;
    }

    /**
     * The prefix to upload revision to in order for deployment app to pick up the resources and deploy.
     * NOTE: We cannot use a same param name to any of the params in parent classes
     * or our parameter will not be injected correctly.
     */
    @Parameter(name = "deploymentResourceKey", property = "deployment.resource.key",
            defaultValue = "${project.groupId}/${project.artifactId}/${project.version}/${project.build.finalName}"
                    + ".zip")
    protected String deploymentResourceKey;

    @Override
    protected String getDatabricksRepoKey() {
        return deploymentResourceKey;
    }
}
