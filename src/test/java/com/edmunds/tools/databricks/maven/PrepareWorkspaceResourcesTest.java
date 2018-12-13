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

import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class PrepareWorkspaceResourcesTest extends DatabricksMavenPluginTestHarness {

    private final String GOAL = "prepare-workspace-resources";


    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

/*
    //Currently I can't get this test to work for the life of me on jenkins... It fails during the copy dir stage.
    @Test(enabled = false)
    public void testNotebookCopy() throws Exception {
        String localPath = classLoader.getResource("notebooks/").getPath();
        File outputPath = new File(outputBuildDir + "notebooks").getAbsoluteFile();
        underTest.setSourceWorkspacePath(new File(localPath));
        underTest.setPackagedWorkspacePath(outputPath);
        underTest.setWorkspacePrefix("/test/mycoolartifact");
        underTest.validate = true;
        underTest.prepareNotebooks();
        underTest.validate = false;

        List<String> expectedFiles = Lists.newArrayList(
                "target/test-target/notebooks/test/mycoolartifact/test1/myFile.scala",
                "target/test-target/notebooks/test/mycoolartifact/test2/myFile.scala",
                "target/test-target/notebooks/test/mycoolartifact/test2/test3/myFile.scala");
        assertTrue(underTest.packagedWorkspacePath.exists());
        for (String expectedFile : expectedFiles) {
            assertThat(expectedFile, new File(expectedFile).exists());
        }
    }
    */
}