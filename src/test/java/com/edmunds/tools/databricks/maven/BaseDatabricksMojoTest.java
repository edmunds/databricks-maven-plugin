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

import com.edmunds.rest.databricks.DatabricksServiceFactory;
import com.edmunds.rest.databricks.service.ClusterService;
import com.edmunds.rest.databricks.service.DbfsService;
import com.edmunds.rest.databricks.service.JobService;
import com.edmunds.rest.databricks.service.LibraryService;
import com.edmunds.rest.databricks.service.WorkspaceService;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.model.Build;
import org.apache.maven.model.Model;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import static com.edmunds.tools.databricks.maven.BaseDatabricksMojo.ARTIFACT_ID;
import static com.edmunds.tools.databricks.maven.BaseDatabricksMojo.GROUP_ID;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@PrepareForTest({DatabricksServiceFactory.class})
public class BaseDatabricksMojoTest extends PowerMockTestCase {

    //Apparently needed for jenkins tests...
    private static Logger log = Logger.getLogger(BaseDatabricksMojoTest.class);

    protected DatabricksServiceFactory databricksServiceFactory;

    @Mock
    protected MavenProject project;
    @Mock
    protected Build build;
    @Mock
    protected Model model;
    @Mock
    protected Artifact artifact;

    @Mock
    protected ClusterService clusterService;
    @Mock
    protected LibraryService libraryService;
    @Mock
    protected WorkspaceService workspaceService;
    @Mock
    protected JobService jobService;
    @Mock
    protected DbfsService dbfsService;

    @Spy
    private BaseDatabricksMojo baseDatabricksMojo = new BaseDatabricksMojo() {
        @Override
        public void execute() throws MojoExecutionException, MojoFailureException {
            //nothing to do here
        }
    };

    @BeforeMethod
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);

        databricksServiceFactory = mock(DatabricksServiceFactory.class);

        when(databricksServiceFactory.getClusterService()).thenReturn(clusterService);
        when(databricksServiceFactory.getLibraryService()).thenReturn(libraryService);
        when(databricksServiceFactory.getWorkspaceService()).thenReturn(workspaceService);
        when(databricksServiceFactory.getJobService()).thenReturn(jobService);
        when(databricksServiceFactory.getDbfsService()).thenReturn(dbfsService);

        when(project.getModel()).thenReturn(model);
        when(project.getBuild()).thenReturn(build);
        when(project.getArtifact()).thenReturn(artifact);

        when(project.getGroupId()).thenReturn("com.edmunds.test");
        when(project.getArtifactId()).thenReturn("mycoolartifact");
        Properties properties = new Properties() {{
            put("databricks.repo", "bucket-name");
        }};
        when(project.getProperties()).thenReturn(properties);
        when(artifact.getArtifactId()).thenReturn("mycoolartifact");
        when(artifact.getType()).thenReturn("jar");
        when(project.getVersion()).thenReturn("1.0");
        when(build.getFinalName()).thenReturn("mycoolartifact-1.0");
        when(build.getOutputDirectory()).thenReturn("target/test-target/");

        System.setProperty(ARTIFACT_ID, "system-property-artifact-id");
        System.setProperty(GROUP_ID, "system-property-group-id");

        Mockito.when(project.getGroupId()).thenReturn("maven-group-id");
        Mockito.when(project.getArtifactId()).thenReturn("maven-artifact-id");

        baseDatabricksMojo.setPrefixToStrip("com\\.edmunds\\.");
    }

    @Test
    public void testValidPath_no_maven() throws Exception {
        Mockito.when(project.getArtifactId()).thenReturn("standalone-pom");
        baseDatabricksMojo.validatePath("/system-property-group-id/system-property-artifact-id", project.getGroupId(), project.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    @Test
    public void testValidPath_with_maven() throws Exception {
        System.setProperty(ARTIFACT_ID, "");
        System.setProperty(GROUP_ID, "");
        baseDatabricksMojo.validatePath("/maven-group-id/maven-artifact-id", project.getGroupId(), project.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    @Test
    public void testValidPath_with_company_name() throws Exception {
        Mockito.when(project.getGroupId()).thenReturn("com.edmunds.maven-group-id");
        baseDatabricksMojo.validatePath("/maven-group-id/maven-artifact-id", project.getGroupId(), project.getArtifactId());

        //nothing to assert, failure will throw an exception
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*'groupId' is not set.*")
    public void testMissingGroupId() throws Exception {
        System.setProperty(ARTIFACT_ID, "");
        System.setProperty(GROUP_ID, "");
        Mockito.when(project.getGroupId()).thenReturn("");

        baseDatabricksMojo.validatePath("/maven-group-id/maven-artifact-id", project.getGroupId(), project.getArtifactId());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*'artifactId' is not set.*")
    public void testMissingArtifactId() throws Exception {
        System.setProperty(ARTIFACT_ID, "");
        System.setProperty(GROUP_ID, "");
        Mockito.when(project.getArtifactId()).thenReturn("");

        baseDatabricksMojo.validatePath("/maven-group-id/maven-artifact-id", project.getGroupId(), project.getArtifactId());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*Expected: \\[groupId/artifactId/...\\] but found: \\[1\\] parts.*")
    public void testInvalidPath_no_maven_missing_artifact_id() throws Exception {
        Mockito.when(project.getArtifactId()).thenReturn("standalone-pom");
        baseDatabricksMojo.validatePath("/system-property-group-id/", project.getGroupId(), project.getArtifactId());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*Expected: \\[system-property-artifact-id] but found: \\[foo\\].*")
    public void testInvalidPath_no_maven_bad_artifact_id() throws Exception {
        Mockito.when(project.getArtifactId()).thenReturn("standalone-pom");
        baseDatabricksMojo.validatePath("/system-property-group-id/foo", project.getGroupId(), project.getArtifactId());
    }

}
