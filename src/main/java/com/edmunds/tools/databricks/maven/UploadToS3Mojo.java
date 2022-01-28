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
 * Uploads an artifact to s3.
 */
@Mojo(name = "upload-to-s3", defaultPhase = LifecyclePhase.DEPLOY)
public class UploadToS3Mojo extends BaseDatabricksS3Mojo {

    /**
     * The local file to upload.
     */
    @Parameter(property = "file", required = true,
        defaultValue = "${project.build.directory}/${project.build.finalName}.${project.packaging}")
    private File file;

    @Override
    protected File getSourceFile() {
        return file;
    }
}
