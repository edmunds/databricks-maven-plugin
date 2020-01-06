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

import static com.edmunds.tools.databricks.maven.util.ClusterUtils.convertClusterNamesToIds;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.contains;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.edmunds.rest.databricks.DTO.clusters.ClusterInfoDTO;
import com.edmunds.rest.databricks.DTO.clusters.ClusterStateDTO;
import com.edmunds.rest.databricks.DTO.libraries.ClusterLibraryStatusesDTO;
import com.edmunds.rest.databricks.DTO.libraries.LibraryDTO;
import com.edmunds.rest.databricks.DTO.libraries.LibraryFullStatusDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.service.ClusterService;
import com.edmunds.rest.databricks.service.LibraryService;
import com.edmunds.tools.databricks.maven.model.LibraryClustersModel;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Installs\Uninstalls a library to\from a databricks cluster.
 * Please NOTE:
 * 1) Libraries attached via the API will NOT show up in the databricks UI - only through the API
 * 2) The library call is async and will not return a response if the library is invalid see <a href="https://docs.databricks.com/api/latest/libraries.html#install">here.</a>
 * 3) dbfs is the only format supported, at least initially - the artifact name will have this prepended
 * 4) In order to modify a library on a cluster, the cluster has to be up.
 * Therefore, this mojo will start a stopped cluster to do its work.
 * It will return the cluster to it's original state when complete.
 */
@Mojo(name = "library", requiresProject = true)
public class LibraryMojo extends BaseLibraryMojo {

    private static final int SLEEP_TIME_MS = 200;
    /**
     * The library command to execute.<br>
     * INSTALL - installs a library to a cluster. It will restart a cluster if necessary.<br>
     * UNINSTALL - removes a library from a cluster. It will restart a cluster if necessary.<br>
     * STATUS - the status of libraries on a cluster.<br>
     */
    @Parameter(name = "libraryCommand", property = "library.command", required = true)
    private LibraryCommand libraryCommand;
    /**
     * If set to true (default), the cluster will be restarted in order for the new library to immediately take effect.
     * If set to false, the cluster will not be restarted which means that in order for new library to be installed,
     * manual restart is necessary.
     */
    @Parameter(property = "restart", required = false, defaultValue = "true")
    private boolean restart;

    /**
     * Execute LibraryMojo.
     *
     * @throws MojoExecutionException exception
     */
    public void execute() throws MojoExecutionException {

        LibraryService libraryService = getDatabricksServiceFactory().getLibraryService();
        ClusterService clusterService = getDatabricksServiceFactory().getClusterService();

        LibraryClustersModel libraryClustersModel = getLibraryClustersModel();
        if (libraryClustersModel == null) {
            return;
        }

        String artifactPath = libraryClustersModel.getArtifactPath();

        for (String clusterId : convertClusterNamesToIds(clusterService, libraryClustersModel.getClusterNames())) {
            try {

                getLog().debug(
                    String.format("preparing to run command [%s] artifact on path: [%s] to cluster id: [%s]",
                        libraryCommand, artifactPath, clusterId));

                switch (libraryCommand) {
                    case INSTALL:
                    case UNINSTALL:
                        runCommand(artifactPath, clusterId, libraryCommand, clusterService, libraryService);
                        break;
                    case STATUS:
                        listLibraryStatus(clusterId, libraryService);
                        break;
                    default:
                        throw new IllegalStateException("No valid library command was found.");
                }
            } catch (DatabricksRestException | IOException e) {
                throw new MojoExecutionException(
                    String.format("Could not [%s] library: [%s] to [%s]", libraryCommand, artifactPath, clusterId), e);
            }
        }

    }

    private void uninstallPreviousVersions(String clusterId, LibraryService libraryService)
        throws IOException, DatabricksRestException, MojoExecutionException {
        LibraryFullStatusDTO[] libraryFullStatuses = getLibraryFullStatusDTOs(clusterId, libraryService);
        for (LibraryFullStatusDTO libraryFullStatus : libraryFullStatuses) {
            String jar = libraryFullStatus.getLibrary().getJar();
            if (contains(jar, project.getGroupId()) && contains(jar, project.getArtifactId())) {
                getLog().info(String.format("uninstalling previously installed version: [%s]", jar));
                libraryService.uninstall(clusterId, getLibraryDTOs(jar));
            }
        }
    }

    private void runCommand(String artifactPath, String clusterId, LibraryCommand libraryCommand, ClusterService
        clusterService, LibraryService libraryService)
        throws IOException, DatabricksRestException, MojoExecutionException {

        if (project.getArtifact().getType().equals(JAR)) {
            ClusterStateDTO originalState = startCluster(clusterId, clusterService);
            if (libraryCommand == LibraryCommand.INSTALL) {
                uninstallPreviousVersions(clusterId, libraryService);
                libraryService.install(clusterId, getLibraryDTOs(artifactPath));
            } else {
                libraryService.uninstall(clusterId, getLibraryDTOs(artifactPath));
            }

            listLibraryStatus(clusterId, libraryService);
            manageClusterState(clusterId, originalState, clusterService, restart);
        } else {
            getLog().warn(String.format("skipping install for non-jar artifact: [%s]", project.getArtifact()));
        }

    }

    /**
     * There are 2 valid operations:
     * 1. If the cluster was stopped prior to this mojo, turn it back off.
     * 2. If the cluster was running, restart it.
     * Both of these actions are there to account for how library-cluster interaction works in databricks.
     * e.g. You cannot modify libraries on a stopped cluster, and a restart is required on running ones.
     *
     * @param clusterId - the cluster id we're working on
     * @param originalState - the state the cluster was in prior to this mojo
     * @param clusterService - cluster service
     * @param restart - whether to restart the cluster
     */
    private void manageClusterState(String clusterId, ClusterStateDTO originalState, ClusterService clusterService,
        boolean restart) throws
        IOException, DatabricksRestException {
        switch (originalState) {
            case PENDING:
            case RESTARTING:
            case RESIZING:
            case RUNNING:
                if (restart) {
                    getLog().info("Restarting cluster!");
                    restartCluster(clusterId, clusterService);
                } else {
                    getLog().info("restart set to false. "
                        + "Users need to restart cluster in order for new library to take effect");
                }
                break;
            default:
                getLog().info("Stopping cluster to return cluster to its initial state.");
                stopCluster(clusterId, clusterService);
                break;
        }
    }

    private void restartCluster(String clusterId, ClusterService clusterService) throws
        IOException, DatabricksRestException {
        getLog().info(String.format("restarting cluster: [%s]", clusterId));

        stopCluster(clusterId, clusterService);

        clusterService.start(clusterId);
    }

    private void stopCluster(String clusterId, ClusterService clusterService) throws
        IOException, DatabricksRestException {
        clusterService.delete(clusterId);

        while (clusterService.getInfo(clusterId).getState() == ClusterStateDTO.TERMINATING) {
            getLog().info(String.format("waiting to shut down cluster: [%s]", clusterId));
            Uninterruptibles.sleepUninterruptibly(SLEEP_TIME_MS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * NOOP if the cluster is already running.
     *
     * @param clusterId - the cluster id to start
     * @return - the cluster state
     */
    private ClusterStateDTO startCluster(String clusterId, ClusterService clusterService) throws
        IOException, DatabricksRestException {
        ClusterInfoDTO info = clusterService.getInfo(clusterId);
        ClusterStateDTO originalState = info.getState();
        switch (originalState) {
            case PENDING:
            case RESTARTING:
            case RESIZING:
            case RUNNING:
                getLog()
                    .info(String.format("cluster: [%s] is in state: [%s], skipping start.", clusterId, originalState));
                break;
            default:
                clusterService.start(clusterId);
                getLog().info(String.format("cluster: [%s] previous state: [%s], starting.", clusterId, originalState));
                break;
        }

        return originalState;
    }

    private void listLibraryStatus(String clusterId, LibraryService libraryService) throws
        IOException, DatabricksRestException {
        LibraryFullStatusDTO[] libraryFullStatuses = getLibraryFullStatusDTOs(clusterId, libraryService);
        for (LibraryFullStatusDTO libraryFullStatus : libraryFullStatuses) {
            String jar = libraryFullStatus.getLibrary().getJar();
            if (isNotBlank(jar)) {
                getLog().info(String.format("library status: [%s] for jar: [%s]", libraryFullStatus.getStatus(), jar));
            }
        }
    }

    private LibraryFullStatusDTO[] getLibraryFullStatusDTOs(String clusterId, LibraryService libraryService) throws
        IOException, DatabricksRestException {
        ClusterLibraryStatusesDTO clusterLibraryStatuses = defaultIfNull(libraryService.clusterStatus(clusterId),
            new ClusterLibraryStatusesDTO());
        return defaultIfNull(clusterLibraryStatuses.getLibraryFullStatuses(), new LibraryFullStatusDTO[]{});
    }

    private LibraryDTO[] getLibraryDTOs(String artifactPath) {
        LibraryDTO lib = new LibraryDTO();
        lib.setJar(artifactPath);
        return new LibraryDTO[]{lib};
    }

    /**
     * The library commands.
     */
    public enum LibraryCommand {
        INSTALL, UNINSTALL, STATUS
    }
}
