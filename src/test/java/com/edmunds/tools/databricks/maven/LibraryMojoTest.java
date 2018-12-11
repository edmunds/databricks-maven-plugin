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
public class LibraryMojoTest extends DatabricksMavenPluginTestHarness {

    private final String GOAL = "library";


    //TODO actually test the library functionality!
    @Test
    public void testCreateArtifactPath_default() throws Exception {
        LibraryMojo underTest = (LibraryMojo) getNoOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/artifacts/unit-test-group" +
                "/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
    }

    @Test
    public void testCreateArtifactPath_failsWhenMissingMandatoryFields() throws Exception {
        LibraryMojo underTest = (LibraryMojo) getMissingMandatoryMojo(GOAL);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }

    @Test
    public void testCreateArtifactPath_succeedsWithOverrides() throws Exception {
        LibraryMojo underTest = (LibraryMojo) getOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/artifacts/my-destination"));
    }
}