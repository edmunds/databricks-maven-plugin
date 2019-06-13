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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PrepareLibraryResourcesTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "prepare-library-resources";

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void createArtifactPath_default_NOOP() throws Exception {
        PrepareLibraryResources underTest = getNoOverridesMojo(GOAL);
        assertThat(underTest.createDeployedArtifactPath(), is("s3://my-bucket/artifacts/unit-test-group" +
                "/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
        underTest.execute();
    }

    @Test
    public void createArtifactPath_WhenNoFieldsSpecified_NOOP() throws Exception {
        PrepareLibraryResources underTest = getMissingMandatoryMojo(GOAL);
        underTest.execute();
    }

    @Test
    public void createArtifactPath_WithOverrides_succeeds() throws Exception {
        PrepareLibraryResources underTest = getOverridesMojo(GOAL);
        underTest.execute();

        String expected = String.format("{%n" +
                "  \"artifactPath\" : \"s3://my-bucket/artifacts/unit-test-group/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar\",%n" +
                "  \"clusterNames\" : [ \"my-test-cluster\", \"my-other-cluster\" ]%n" +
                "}");

        String actual = FileUtils.readFileToString(underTest.libaryMappingFileOutput, StandardCharsets.UTF_8);

        assertEquals(expected, actual);
    }
}