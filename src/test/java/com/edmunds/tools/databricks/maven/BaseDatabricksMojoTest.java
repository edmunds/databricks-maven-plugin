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

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.edmunds.rest.databricks.DatabricksServiceFactory;
import com.edmunds.rest.databricks.service.ClusterService;
import com.edmunds.rest.databricks.service.DbfsService;
import com.edmunds.rest.databricks.service.JobService;
import com.edmunds.rest.databricks.service.LibraryService;
import com.edmunds.rest.databricks.service.WorkspaceService;
import java.util.Properties;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.model.Build;
import org.apache.maven.model.Model;
import org.apache.maven.project.MavenProject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;

@PrepareForTest({DatabricksServiceFactory.class})
public class BaseDatabricksMojoTest extends PowerMockTestCase {

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
    }

}
