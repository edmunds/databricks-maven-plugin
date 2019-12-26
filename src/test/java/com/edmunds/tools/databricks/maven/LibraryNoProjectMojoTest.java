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
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LibraryNoProjectMojoTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "library-np";

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void install_whenClusterMappingExistsAndonlyOneCluster_attachesLibraryToExistingCluster() throws Exception {
        LibraryMojoNoProject underTest = getOverridesMojo(GOAL, "install");
        ClusterInfoDTO clusterOne = createClusterInfoDTO("1", "my-test-cluster");
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
    public void install_whenNoClusterMapping_doesNothing() throws Exception {
        LibraryMojoNoProject underTest = getNoOverridesMojo(GOAL);

        underTest.execute();

        Mockito.verify(libraryService, Mockito.times(0)).install(Matchers.anyString(), Matchers.any());
    }

    @Test
    public void unInstall_whenClusterMappingExistsAndonlyOneCluster_attachesLibraryToExistingCluster() throws
        Exception {
        LibraryMojoNoProject underTest = getOverridesMojo(GOAL, "uninstall");
        ClusterInfoDTO clusterOne = createClusterInfoDTO("1", "my-test-cluster");
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

    private ClusterInfoDTO createClusterInfoDTO(String clusterId, String clusterName) {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.setClusterId(clusterId);
        clusterInfoDTO.setClusterName(clusterName);
        clusterInfoDTO.setState(ClusterStateDTO.RUNNING);
        return clusterInfoDTO;
    }

    @Test
    public void execute_WhenMissingFields_succeeds() throws Exception {
        LibraryMojoNoProject underTest = getMissingMandatoryMojo(GOAL);
        underTest.execute();
    }
}