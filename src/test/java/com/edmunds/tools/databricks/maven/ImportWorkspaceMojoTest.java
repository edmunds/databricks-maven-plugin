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
import com.edmunds.tools.databricks.maven.util.Environment;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.fail;

public class ImportWorkspaceMojoTest extends BaseDatabricksMojoTest {

    private ImportWorkspaceMojo underTest = new ImportWorkspaceMojo();

    @BeforeMethod
    public void init() throws Exception {
        super.init();

        underTest.setDatabricksServiceFactory(databricksServiceFactory);
        underTest.setEnvironment(Environment.QA);
    }

    @Test
    public void testImportWorksWithProperPath() throws Exception {
        File workspacePath = new File(this.getClass().getResource("/notebooks")
            .getPath());
        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);
        Mockito.when(project.getGroupId()).thenReturn("com.edmunds.test");
        Mockito.when(project.getArtifactId()).thenReturn("mycoolartifact");
        underTest.setProject(project);

        underTest.execute();

        Mockito.verify(workspaceService, Mockito.times(3)).importWorkspace(Mockito.any(ImportWorkspaceRequest.class));
    }

    // Cannot use expected exceptions with a MojoExecutionException...
    @Test
    public void testImportFailsWithImproperPath() throws Exception {
        File workspacePath = new File(this.getClass().getResource("/notebooks")
            .getPath());
        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);
        Mockito.when(project.getGroupId()).thenReturn("com.edmunds.nottest");
        Mockito.when(project.getArtifactId()).thenReturn("mycoolartifact");
        underTest.setProject(project);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }
}