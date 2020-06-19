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

import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.DatabricksServiceFactory;
import com.edmunds.rest.databricks.service.DbfsService;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Uploads an artifact to dbfs.
 */
@Mojo(name = "upload-to-dbfs", defaultPhase = LifecyclePhase.DEPLOY)
public class UploadToDbfsMojo extends BaseDatabricksMojo {

    protected DbfsService service;

    /**
     * The local file to upload.
     */
    @Parameter(property = "file", required = true,
            defaultValue = "${project.build.directory}/${project.build.finalName}.${project.packaging}")
    private File file;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            InputStream is = new FileInputStream(file);
            String filePath = createDeployedArtifactPath();

            getDbfsService().write(filePath, is, true);

        } catch (FileNotFoundException e) {
            getLog().warn(String.format("Target upload file does not exist, skipping: [%s]", file.getPath()));
        } catch (IOException | DatabricksRestException e) {
            getLog().warn(String.format("Error during performing write request. Message: %s", e.getMessage()));
        }
    }

    protected DbfsService getDbfsService() {
        if (service == null) {
            DatabricksServiceFactory factory = getDatabricksServiceFactory();
            service = factory.getDbfsService();
        }
        return service;
    }
}
