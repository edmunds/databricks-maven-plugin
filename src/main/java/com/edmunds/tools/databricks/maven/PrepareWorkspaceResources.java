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

import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import java.io.IOException;

/**
 * Prepares some of the Databricks Resources into the package.
 * For jobs:
 * Evaluates all freemarker templates and validates jobSettings files.
 * Then writes out the evaluated files to the jar to be packaged.
 * <p>
 * For notebooks:
 * It validates that the location of the notebooks meets standards, then copies them to target/notebooks
 * <p>
 * NOTE, this does not cover library preparation, as adding that here would have caused DRY violations and would have been very hacky.
 */
@Mojo(name = "prepare-workspace-resources", requiresProject = true, defaultPhase = LifecyclePhase.PREPARE_PACKAGE)
public class PrepareWorkspaceResources extends BaseWorkspaceMojo {

    @Override
    public void execute() throws MojoExecutionException {
        prepareNotebooks();
    }

    void prepareNotebooks() throws MojoExecutionException {
        if (!sourceWorkspacePath.exists()) {
            getLog().warn("No notebooks found. Skipping packaging and validation: ["
                    + sourceWorkspacePath.getPath() + "]");
            return;
        }
        validateNotebooks(sourceWorkspacePath);
        try {
            getLog().info(String.format("Copying notebooks at: [%s] to [%s]", sourceWorkspacePath,
                    packagedWorkspacePath.toString()));
            FileUtils.copyDirectory(sourceWorkspacePath, packagedWorkspacePath);
        } catch (IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}
