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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for @{@link UpsertJobMojo}.
 * <p>
 * For these tests, the regex as part of the expected exceptions no longer works.
 */
public class UploadMojoTest extends DatabricksMavenPluginTestHarness {

    private final String GOAL = "upload-to-s3";

    private UploadMojo underTest;

    @Mock
    AmazonS3Client s3Client;

    public void testDefaultExecute() throws Exception {
        super.beforeMethod();
        underTest = getOverridesMojo(GOAL);
        underTest.s3Client = s3Client;
        underTest.execute();
    }

    public void testMissingProperties() throws Exception {
        super.beforeMethod();
        underTest = getMissingMandatoryMojo(GOAL);
        underTest.s3Client = s3Client;
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("Missing mandatory parameter: ${databricksRepo}"));
            return;
        }
        fail();
    }

    public void testOverridesExecute() throws Exception {
        super.beforeMethod();
        underTest = getOverridesMojo(GOAL);
        underTest.s3Client = s3Client;
        underTest.execute();

        ArgumentCaptor<PutObjectRequest> putRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(s3Client).putObject(putRequestCaptor.capture());
        assertEquals("myBucket", putRequestCaptor.getValue().getBucketName());
        assertEquals("repo/unit-test-group/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar",
            putRequestCaptor.getValue().getKey());
        assertEquals("myFile.csv", putRequestCaptor.getValue().getFile().getName());
    }
}