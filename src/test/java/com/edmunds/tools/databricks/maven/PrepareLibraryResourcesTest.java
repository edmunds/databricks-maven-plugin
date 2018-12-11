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

import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

//Requires building beforehand??
public class PrepareLibraryResourcesTest extends DatabricksMavenPluginTestHarness {

    private final String GOAL = "prepare-library-resources";

    @Test
    public void testCreateArtifactPath_default() throws Exception {
        PrepareLibraryResources underTest = (PrepareLibraryResources) getNoOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/artifacts/unit-test-group" +
                "/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
        //TODO actually test the execute here
        underTest.execute();
    }

    @Test
    public void testCreateArtifactPath_failsWhenMissingMandatoryFields() throws Exception {
        PrepareLibraryResources underTest = (PrepareLibraryResources) getMissingMandatoryMojo(GOAL);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }

    @Test
    public void testCreateArtifactPath_succeedsWithOverrides() throws Exception {
        PrepareLibraryResources underTest = (PrepareLibraryResources) getOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/my-destination"));
    }
}