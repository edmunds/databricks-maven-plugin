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

package com.edmunds.tools.databricks.maven.model;

import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Simple POJO to pass properties to the job template.
 */
public class JobTemplateModel extends BaseModel {

    /**
     * Don't use this - it's for jackson deserialization only!
     */
    public JobTemplateModel() {
    }

    public JobTemplateModel(MavenProject project,
                            String environment, String databricksRepo, String databricksRepoKey, String prefixToStrip) {
        super(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
    }

    public static JobTemplateModel loadJobTemplateModelFromFile(File jobTemplateModelFile) throws
            MojoExecutionException {
        if (jobTemplateModelFile == null) {
            throw new MojoExecutionException("jobTemplateModelFile must be set!");
        }
        try {
            String jobTemplateModelJson = FileUtils.readFileToString(jobTemplateModelFile, Charset.defaultCharset());
            return ObjectMapperUtils.deserialize(jobTemplateModelJson, JobTemplateModel.class);
        } catch (IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}
