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

import com.edmunds.rest.databricks.DTO.ClusterInfoDTO;
import com.edmunds.rest.databricks.DTO.ClusterStateDTO;
import com.edmunds.rest.databricks.DTO.LibraryDTO;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.times;

public class LibraryMojoTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "library";

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void testCreateArtifactPath_whenNoClustersSpecified_DoesNothing() throws Exception {
        LibraryMojo underTest = getNoOverridesMojo(GOAL);
        assertThat(underTest.createDeployedArtifactPath(), is("s3://my-bucket/artifacts/unit-test-group" +
                "/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));

        underTest.execute();

        Mockito.verify(libraryService, times(0)).install(Matchers.anyString(), Matchers.any());
    }

    @Test
    public void install_whenClusterMappingExistsAndonlyOneCluster_attachesLibraryToExistingCluster() throws Exception {
        LibraryMojo underTest = getOverridesMojo(GOAL, "install");
        ClusterInfoDTO clusterOne = createClusterInfoDTO("1", "my-test-cluster", ClusterStateDTO.RUNNING);
        ClusterInfoDTO[] clusters = {clusterOne};
        Mockito.when(clusterService.list()).thenReturn(clusters);
        Mockito.when(clusterService.getInfo("1")).thenReturn(clusterOne);
        ArgumentCaptor<LibraryDTO[]> libraryDTOArgumentCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);

        underTest.execute();

        Mockito.verify(libraryService).install(Matchers.anyString(), libraryDTOArgumentCaptor.capture());
        Mockito.verify(clusterService).delete("1");
        Mockito.verify(clusterService).start("1");

        LibraryDTO libraryOne = libraryDTOArgumentCaptor.getValue()[0];
        assertEquals("s3://my-bucket/artifacts/unit-test-group/unit-test-artifact/" +
                "1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar", libraryOne.getJar());
    }

    @Test
    public void install_whenRestartFalseAndRunning_attachesLibraryButDoesNotRestart() throws Exception {
        LibraryMojo underTest = getOverridesMojo(GOAL, "_install_no_restart");
        ClusterInfoDTO clusterOne = createClusterInfoDTO("1", "my-test-cluster", ClusterStateDTO.RUNNING);
        ClusterInfoDTO[] clusters = {clusterOne};
        Mockito.when(clusterService.list()).thenReturn(clusters);
        Mockito.when(clusterService.getInfo("1")).thenReturn(clusterOne);
        ArgumentCaptor<LibraryDTO[]> libraryDTOArgumentCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);

        underTest.execute();

        Mockito.verify(libraryService).install(Matchers.anyString(), libraryDTOArgumentCaptor.capture());
        Mockito.verify(clusterService, times(0)).delete("1");
        Mockito.verify(clusterService, times(0)).start("1");

        LibraryDTO libraryOne = libraryDTOArgumentCaptor.getValue()[0];
        assertEquals("s3://my-bucket/artifacts/unit-test-group/unit-test-artifact/" +
                "1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar", libraryOne.getJar());
    }

    @Test
    public void install_whenRestartFalseAndTerminated_startsAttachesLibraryAndTerminates() throws Exception {
        LibraryMojo underTest = getOverridesMojo(GOAL, "_install_no_restart");
        ClusterInfoDTO clusterOne = createClusterInfoDTO("1", "my-test-cluster", ClusterStateDTO.TERMINATED);
        ClusterInfoDTO[] clusters = {clusterOne};
        Mockito.when(clusterService.list()).thenReturn(clusters);
        Mockito.when(clusterService.getInfo("1")).thenReturn(clusterOne);
        ArgumentCaptor<LibraryDTO[]> libraryDTOArgumentCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);

        underTest.execute();

        Mockito.verify(libraryService).install(Matchers.anyString(), libraryDTOArgumentCaptor.capture());

        Mockito.verify(clusterService, times(1)).start("1");
        Mockito.verify(clusterService, times(1)).delete("1");

        LibraryDTO libraryOne = libraryDTOArgumentCaptor.getValue()[0];
        assertEquals("s3://my-bucket/artifacts/unit-test-group/unit-test-artifact/" +
                "1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar", libraryOne.getJar());
    }

    @Test
    public void unInstall_whenClusterMappingExistsAndonlyOneCluster_attachesLibraryToExistingCluster() throws Exception {
        LibraryMojo underTest = getOverridesMojo(GOAL, "uninstall");
        ClusterInfoDTO clusterOne = createClusterInfoDTO("1", "my-test-cluster", ClusterStateDTO.RUNNING);
        ClusterInfoDTO[] clusters = {clusterOne};
        Mockito.when(clusterService.list()).thenReturn(clusters);
        Mockito.when(clusterService.getInfo("1")).thenReturn(clusterOne);
        ArgumentCaptor<LibraryDTO[]> libraryDTOArgumentCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);

        underTest.execute();

        Mockito.verify(libraryService).uninstall(Matchers.anyString(), libraryDTOArgumentCaptor.capture());
        Mockito.verify(clusterService).delete("1");
        Mockito.verify(clusterService).start("1");

        LibraryDTO libraryOne = libraryDTOArgumentCaptor.getValue()[0];
        assertEquals("s3://my-bucket/artifacts/unit-test-group/unit-test-artifact/" +
                "1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar", libraryOne.getJar());
    }

    //TODO proper behavior should be to log here?
    @Test
    public void unInstall_whenClusterCantBeFound_doesNothing() throws Exception {
        LibraryMojo underTest = getOverridesMojo(GOAL, "uninstall");
        ClusterInfoDTO clusterOne = createClusterInfoDTO("1", "not-my-cluster", ClusterStateDTO.RUNNING);
        ClusterInfoDTO[] clusters = {clusterOne};
        Mockito.when(clusterService.list()).thenReturn(clusters);
        Mockito.when(clusterService.getInfo("1")).thenReturn(clusterOne);
        ArgumentCaptor<LibraryDTO[]> libraryDTOArgumentCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);

        underTest.execute();

        Mockito.verify(libraryService, times(0)).uninstall(Matchers.anyString(), libraryDTOArgumentCaptor
                .capture());
        Mockito.verify(clusterService, times(0)).delete("1");
        Mockito.verify(clusterService, times(0)).start("1");
    }

    @Test
    public void testCreateArtifactPath_failsWhenMissingMandatoryFields() throws Exception {
        LibraryMojo underTest = getMissingMandatoryMojo(GOAL);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }

    @Test
    public void testCreateArtifactPath_succeedsWithOverrides() throws Exception {
        LibraryMojo underTest = getOverridesMojo(GOAL);
        assertThat(underTest.createDeployedArtifactPath(), is("s3://my-bucket/artifacts/my-destination"));
    }

    private ClusterInfoDTO createClusterInfoDTO(String clusterId, String clusterName, ClusterStateDTO clusterStateDTO) {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.setClusterId(clusterId);
        clusterInfoDTO.setClusterName(clusterName);
        clusterInfoDTO.setState(clusterStateDTO);
        return clusterInfoDTO;
    }
}