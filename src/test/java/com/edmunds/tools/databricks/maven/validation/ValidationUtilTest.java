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

package com.edmunds.tools.databricks.maven.validation;

import junit.framework.TestCase;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.edmunds.tools.databricks.maven.validation.ValidationUtil.ARTIFACT_ID;
import static com.edmunds.tools.databricks.maven.validation.ValidationUtil.GROUP_ID;
import static com.edmunds.tools.databricks.maven.validation.ValidationUtil.validatePath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ValidationUtil}.
 */
public class ValidationUtilTest extends TestCase {

    @Mock
    private MavenProject mavenProject;

    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        System.setProperty(ARTIFACT_ID, "system-property-artifact-id");
        System.setProperty(GROUP_ID, "system-property-group-id");

        when(mavenProject.getGroupId()).thenReturn("maven-group-id");
        when(mavenProject.getArtifactId()).thenReturn("maven-artifact-id");
    }

    public void testValidPath_no_maven() throws Exception {
        when(mavenProject.getArtifactId()).thenReturn("standalone-pom");
        validatePath("/system-property-group-id/system-property-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    public void testValidPath_with_maven() throws Exception {
        System.getProperties().clear();
        validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    public void testValidPath_with_company_name() throws Exception {
        when(mavenProject.getGroupId()).thenReturn("com.edmunds.maven-group-id");
        validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    public void testMissingGroupId() throws Exception {
        System.getProperties().clear();
        when(mavenProject.getGroupId()).thenReturn("");
        try {
            validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("'groupId' is not set"));
            return;
        }
        fail();
    }

    public void testMissingArtifactId() throws Exception {
        System.getProperties().clear();
        when(mavenProject.getArtifactId()).thenReturn("");
        try {
            validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("'artifactId' is not set"));
            return;
        }
        fail();
    }

    public void testInvalidPath_no_maven_missing_artifact_id() throws Exception {
        when(mavenProject.getArtifactId()).thenReturn("standalone-pom");
        try {
            validatePath("/system-property-group-id/", mavenProject.getGroupId(), mavenProject.getArtifactId());
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("Expected: [groupId/artifactId/"));
            return;
        }
        fail();
    }

    public void testInvalidPath_no_maven_bad_artifact_id() throws Exception {
        when(mavenProject.getArtifactId()).thenReturn("standalone-pom");
        try {
            validatePath("/system-property-group-id/foo", mavenProject.getGroupId(), mavenProject.getArtifactId());
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("Expected: [system-property-artifact-id] but found: "));
                return;
        }
        fail();
    }

}