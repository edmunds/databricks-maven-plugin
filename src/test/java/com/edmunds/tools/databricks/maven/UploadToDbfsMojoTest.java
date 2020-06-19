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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import com.edmunds.rest.databricks.service.DbfsService;
import java.io.InputStream;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for @{@link UploadToDbfsMojo}.
 */
public class UploadToDbfsMojoTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "upload-to-dbfs";

    @Mock
    DbfsService service;
    private UploadToDbfsMojo underTest;

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void testDefaultExecute() throws Exception {
        underTest = getNoOverridesMojo(GOAL);
        underTest.service = service;
        underTest.execute();
    }

    @Test
    public void testMissingProperties() throws Exception {
        underTest = getMissingMandatoryMojo(GOAL);
        underTest.service = service;
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("Missing mandatory parameter: ${databricksRepo}"));
            return;
        }
        fail();
    }

    @Test
    public void testOverridesExecute() throws Exception {
        underTest = getOverridesMojo(GOAL);
        underTest.service = service;
        underTest.execute();

        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
        ArgumentCaptor<Boolean> overwriteCaptor = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(service).write(pathCaptor.capture(), streamCaptor.capture(), overwriteCaptor.capture());
        assertEquals("dbfs:///dbfsFolder/testFolder/myFile.csv", pathCaptor.getValue());
        assertTrue(overwriteCaptor.getValue());
    }
}