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

import com.edmunds.rest.databricks.request.ImportWorkspaceRequest;
import java.io.File;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;

public class ImportWorkspaceMojoTest extends DatabricksMavenPluginTestHarness {


    private final String GOAL = "import-workspace";

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void testImportWorksWithProperPath() throws Exception {
        ImportWorkspaceMojo underTest = getNoOverridesMojo(GOAL);

        File workspacePath = new File(this.getClass().getResource("/notebooks")
                .getPath());

        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);

        Mockito.verify(libraryService, times(0)).install(Matchers.anyString(), Matchers.any());

        underTest.execute();

        Mockito.verify(workspaceService, Mockito.times(3)).importWorkspace(Mockito.any(ImportWorkspaceRequest.class));
    }

    // Cannot use expected exceptions with a MojoExecutionException...
    @Test
    public void testImportFailsWithImproperPath() throws Exception {

        ImportWorkspaceMojo underTest = getNoOverridesMojo(GOAL, "_fails_validation");

        File workspacePath = new File(this.getClass().getResource("/notebooks")
                .getPath());
        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("JOB NAME VALIDATION FAILED [ILLEGAL VALUE]:\n" +
                    "Expected: [failed-test] but found: [test]"));
            return;
        }
        fail();
    }

    @Test
    public void testImportWorksWithProperPathAndOverrides() throws Exception {
        ImportWorkspaceMojo underTest = getOverridesMojo(GOAL);

        File workspacePath = new File(this.getClass().getResource("/notebooks")
                .getPath());

        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);

        Mockito.verify(libraryService, times(0)).install(Matchers.anyString(), Matchers.any());

        underTest.execute();

        Mockito.verify(workspaceService, Mockito.times(3)).importWorkspace(Mockito.any(ImportWorkspaceRequest.class));
    }
}