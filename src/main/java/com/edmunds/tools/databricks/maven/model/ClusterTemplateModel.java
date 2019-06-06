/*
 *  Copyright 2019 Edmunds.com, Inc.
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

public class ClusterTemplateModel extends BaseModel {

    /**
     * Don't use this - it's for jackson deserialization only!
     */
    public ClusterTemplateModel() {
    }

    public ClusterTemplateModel(MavenProject project,
                                String environment, String databricksRepo, String databricksRepoKey, String prefixToStrip) {
        super(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
    }

    public static ClusterTemplateModel loadClusterTemplateModelFromFile(File clusterTemplateModelFile) throws
            MojoExecutionException {
        if (clusterTemplateModelFile == null) {
            throw new MojoExecutionException("clusterTemplateModelFile must be set!");
        }
        try {
            String clusterTemplateModelJson = FileUtils.readFileToString(clusterTemplateModelFile, Charset.defaultCharset());
            return ObjectMapperUtils.deserialize(clusterTemplateModelJson, ClusterTemplateModel.class);
        } catch (IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}
