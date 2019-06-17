/*
 *    Copyright 2018 Edmunds.com, Inc.
 *
 *        Licensed under the Apache License, Version 2.0 (the "License");
 *        you may not use this file except in compliance with the License.
 *        You may obtain a copy of the License at
 *
 *            http://www.apache.org/licenses/LICENSE-2.0
 *
 *        Unless required by applicable law or agreed to in writing, software
 *        distributed under the License is distributed on an "AS IS" BASIS,
 *        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *        See the License for the specific language governing permissions and
 *        limitations under the License.
 */

package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.LibraryClustersModel;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.util.Arrays;

public abstract class BaseLibraryMojo extends BaseDatabricksMojo {
    static final String JAR = "jar";

    /**
     * This should be a comma separated list of cluster names to install to.
     */
    @Parameter(name = "clusters", property = "clusters")
    protected String[] clusters;

    protected LibraryClustersModel getLibraryClustersModel() throws MojoExecutionException {
        return new LibraryClustersModel(createDeployedArtifactPath(), Arrays.asList(clusters));
    }
}
