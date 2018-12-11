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

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import static com.edmunds.tools.databricks.maven.validation.ValidationUtil.ARTIFACT_ID;
import static com.edmunds.tools.databricks.maven.validation.ValidationUtil.GROUP_ID;
import static com.edmunds.tools.databricks.maven.validation.ValidationUtil.validatePath;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ValidationUtil}.
 *
 * BE VERY CAREFUL WITH SYSTEM PROPERTIES. THIS CAN CAUSE OTHER TESTS TO FAIL THAT DEPEND ON SYSTEM PROPERTIES TO BE
 * SET.
 */
public class ValidationUtilTest {

    @Mock
    private MavenProject mavenProject;

    @BeforeMethod
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        System.setProperty(ARTIFACT_ID, "system-property-artifact-id");
        System.setProperty(GROUP_ID, "system-property-group-id");

        when(mavenProject.getGroupId()).thenReturn("maven-group-id");
        when(mavenProject.getArtifactId()).thenReturn("maven-artifact-id");
    }

    @Test
    public void testValidPath_no_maven() throws Exception {
        when(mavenProject.getArtifactId()).thenReturn("standalone-pom");
        validatePath("/system-property-group-id/system-property-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    @Test
    public void testValidPath_with_maven() throws Exception {
        System.setProperty(ARTIFACT_ID, "");
        System.setProperty(GROUP_ID, "");
        validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    @Test
    public void testValidPath_with_company_name() throws Exception {
        when(mavenProject.getGroupId()).thenReturn("com.edmunds.maven-group-id");
        validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*'groupId' is not set.*")
    public void testMissingGroupId() throws Exception {
        System.setProperty(ARTIFACT_ID, "");
        System.setProperty(GROUP_ID, "");
        when(mavenProject.getGroupId()).thenReturn("");

        validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*'artifactId' is not set.*")
    public void testMissingArtifactId() throws Exception {
        System.setProperty(ARTIFACT_ID, "");
        System.setProperty(GROUP_ID, "");
        when(mavenProject.getArtifactId()).thenReturn("");

        validatePath("/maven-group-id/maven-artifact-id", mavenProject.getGroupId(), mavenProject.getArtifactId());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*Expected: \\[groupId/artifactId/...\\] but found: \\[1\\] parts.*")
    public void testInvalidPath_no_maven_missing_artifact_id() throws Exception {
        when(mavenProject.getArtifactId()).thenReturn("standalone-pom");
        validatePath("/system-property-group-id/", mavenProject.getGroupId(), mavenProject.getArtifactId());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*Expected: \\[system-property-artifact-id] but found: \\[foo\\].*")
    public void testInvalidPath_no_maven_bad_artifact_id() throws Exception {
        when(mavenProject.getArtifactId()).thenReturn("standalone-pom");
        validatePath("/system-property-group-id/foo", mavenProject.getGroupId(), mavenProject.getArtifactId());
    }

}