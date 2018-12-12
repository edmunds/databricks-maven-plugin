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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LibraryMojoTest extends DatabricksMavenPluginTestHarness {

    private final String GOAL = "library";

    //TODO actually test the library functionality!

    public void testCreateArtifactPath_default() throws Exception {
        beforeMethod();
        LibraryMojo underTest = getNoOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/artifacts/unit-test-group" +
                "/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
    }

    public void testCreateArtifactPath_failsWhenMissingMandatoryFields() throws Exception {
        beforeMethod();
        LibraryMojo underTest = getMissingMandatoryMojo(GOAL);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }

    public void testCreateArtifactPath_succeedsWithOverrides() throws Exception {
        LibraryMojo underTest = (LibraryMojo) getOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/artifacts/my-destination"));
    }
}