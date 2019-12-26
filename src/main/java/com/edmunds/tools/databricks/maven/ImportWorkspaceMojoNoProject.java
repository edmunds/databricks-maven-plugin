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
import java.io.IOException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * A mojo that is responsible for taking care of importing notebooks into databricks.
 * No project is split out because otherwise it will only work on one pom at a time
 * and thus not work correctly for multi-module projects.
 */
@Mojo(name = "import-workspace-np", requiresProject = false)
public class ImportWorkspaceMojoNoProject extends ImportWorkspaceMojo {

    /**
     * Execute ImportWorkspaceMojoNoProject.
     *
     * @throws MojoExecutionException exception
     */
    public void execute() throws MojoExecutionException {
        try {
            // In this case, validation happened during build so we are ok.
            importWorkspace(packagedWorkspacePath);
        } catch (DatabricksRestException | IOException e) {
            throw new MojoExecutionException(String.format("Could not execute workspace command: [%s]. Local Path: "
                + "[%s] TO DB: [%s]", "IMPORT", packagedWorkspacePath, workspacePrefix), e);
        }
    }
}