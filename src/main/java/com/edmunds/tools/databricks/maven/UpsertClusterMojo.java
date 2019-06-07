/*
 *  Copyright 2019 Edmunds.com, Inc.
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

import com.edmunds.rest.databricks.DTO.ClusterStateDTO;
import com.edmunds.rest.databricks.DTO.ClusterTagDTO;
import com.edmunds.rest.databricks.DTO.LibraryDTO;
import com.edmunds.rest.databricks.DTO.LibraryFullStatusDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.request.CreateClusterRequest;
import com.edmunds.rest.databricks.request.EditClusterRequest;
import com.edmunds.rest.databricks.service.ClusterService;
import com.edmunds.rest.databricks.service.LibraryService;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateDTO;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.edmunds.tools.databricks.maven.util.ClusterUtils.convertClusterNamesToIds;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Cluster mojo, to perform databricks cluster upsert (create or update through recreation).
 */
@Mojo(name = "upsert-cluster", requiresProject = true)
public class UpsertClusterMojo extends BaseDatabricksUpsertClusterMojo {

    @Override
    public void execute() throws MojoExecutionException {
        upsertJobSettings();
    }

    private void upsertJobSettings() throws MojoExecutionException {
        List<ClusterTemplateDTO> cts = getSettingsUtils().buildTemplateDTOsWithDefaults();
        if (cts.size() == 0) {
            return;
        }

        getLog().info("Environment: " + environment);

        // Upserting clusters in parallel manner
        ForkJoinPool forkJoinPool = new ForkJoinPool(cts.size());
        for (ClusterTemplateDTO ct : cts) {
            forkJoinPool.execute(() -> {
                        try {
                            ClusterService clusterService = getDatabricksServiceFactory().getClusterService();
                            String clusterId = convertClusterNamesToIds(clusterService, Collections.singletonList(ct.getClusterName()))
                                    .stream().findFirst().orElse(EMPTY);

                            // setting mandatory fields
                            CreateClusterRequest.CreateClusterRequestBuilder requestBuilder = new CreateClusterRequest.CreateClusterRequestBuilder(
                                    ct.getNumWorkers(), ct.getClusterName(), ct.getSparkVersion(), ct.getNodeTypeId())
                                    .withAwsAttributes(ct.getAwsAttributes())
                                    .withAutoterminationMinutes(ct.getAutoTerminationMinutes())
                                    .withSparkEnvVars(ct.getSparkEnvVars());

                            // setting optional fields
                            CreateClusterRequest request = requestBuilder
                                    .withDriverNodeTypeId(ct.getDriverNodeTypeId())
                                    .withSparkConf(ct.getSparkConf())
                                    .withClusterLogConf(ct.getClusterLogConf())
                                    .withCustomTags(convertCustomTags(ct.getCustomTags()))
                                    .withSshPublicKeys(ct.getSshPublicKeys())
                                    .build();

                            String logMessage = EMPTY;
                            try {
                                // create new cluster
                                if (StringUtils.isEmpty(clusterId)) {
                                    logMessage = String.format("Creating cluster: name=[%s]", ct.getClusterName());
                                    getLog().info(logMessage);
                                    clusterId = clusterService.create(request);
                                    attachLibraries(ct, clusterId, Collections.emptySet());
                                }
                                // update existing cluster
                                else {
                                    logMessage = String.format("Updating cluster: name=[%s], id=[%s]", ct.getClusterName(), clusterId);
                                    getLog().info(logMessage);

                                    Set<LibraryDTO> clusterLibraries = getClusterLibraries(clusterId);
                                    detachLibraries(ct, clusterId, clusterLibraries);
                                    startCluster(ct, clusterId);
                                    attachLibraries(ct, clusterId, clusterLibraries);
                                    editCluster(ct, clusterId);
                                }
                            } catch (DatabricksRestException | IOException e) {
                                throw new MojoExecutionException(String.format("Exception while [%s]. ClusterTemplateDTO=[%s]", logMessage, ct), e);
                            }
                        } catch (MojoExecutionException e) {
                            getLog().error(e);
                        }
                    }
            );
        }
        forkJoinPool.shutdown();
        try {
            forkJoinPool.awaitTermination(15, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            getLog().error(e);
        }
    }

    /**
     * Apply cluster configuration changes. This action is being followed by cluster restart.
     *
     * @param ct        cluster configuration to be applied
     * @param clusterId cluster id
     * @throws IOException
     * @throws DatabricksRestException
     */
    private void editCluster(ClusterTemplateDTO ct, String clusterId) throws IOException, DatabricksRestException {
        getLog().info(String.format("Applying cluster configuration: name=[%s], id=[%s]", ct.getClusterName(), clusterId));

        // setting mandatory fields
        EditClusterRequest.EditClusterRequestBuilder requestBuilder = new EditClusterRequest.EditClusterRequestBuilder(
                ct.getNumWorkers(), clusterId, ct.getClusterName(), ct.getSparkVersion(), ct.getNodeTypeId()
        )
                .withAwsAttributes(ct.getAwsAttributes())
                .withAutoterminationMinutes(ct.getAutoTerminationMinutes())
                .withSparkEnvVars(ct.getSparkEnvVars());

        // setting optional fields
        EditClusterRequest request = requestBuilder
                .withDriverNodeTypeId(ct.getDriverNodeTypeId())
                .withSparkConf(ct.getSparkConf())
                .withClusterLogConf(ct.getClusterLogConf())
                .withCustomTags(convertCustomTags(ct.getCustomTags()))
                .withSshPublicKeys(ct.getSshPublicKeys())
                .build();

        getDatabricksServiceFactory().getClusterService().edit(request);
    }

    /**
     * Check whether the cluster in a RUNNING state and do start if required.
     *
     * @param ct        cluster configuration
     * @param clusterId cluster id
     * @throws IOException
     * @throws DatabricksRestException
     */
    private void startCluster(ClusterTemplateDTO ct, String clusterId) throws IOException, DatabricksRestException {
        ClusterService clusterService = getDatabricksServiceFactory().getClusterService();
        ClusterStateDTO clusterState = clusterService.getInfo(clusterId).getState();
        if (clusterState != ClusterStateDTO.RUNNING) {
            getLog().info(String.format("Starting cluster: name=[%s], id=[%s]. Current state=[%s]", ct.getClusterName(), clusterId, clusterState));
            if (clusterState == ClusterStateDTO.TERMINATED || clusterState == ClusterStateDTO.TERMINATING
                    || clusterState == ClusterStateDTO.ERROR || clusterState == ClusterStateDTO.UNKNOWN) {
                clusterService.start(clusterId);
            }
            while (clusterState != ClusterStateDTO.RUNNING) {
                getLog().info(String.format("Current cluster state is [%s]. Waiting for RUNNING state", clusterState));
                // sleep some time to avoid excessive requests to databricks API
                Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
                clusterState = clusterService.getInfo(clusterId).getState();
            }
        }
    }

    /**
     * Retrieve libraries currently deployed on specified cluster.
     *
     * @param clusterId cluster id
     * @return cluster libraries
     * @throws IOException
     * @throws DatabricksRestException
     */
    private Set<LibraryDTO> getClusterLibraries(String clusterId) throws IOException, DatabricksRestException {
        return Arrays.stream(getDatabricksServiceFactory().getLibraryService().clusterStatus(clusterId).getLibraryFullStatuses())
                // skip all clusters libraries
                .filter(status -> !status.isLibraryForAllClusters())
                .map(LibraryFullStatusDTO::getLibrary)
                .collect(Collectors.toSet());
    }

    /**
     * Delete redundant libraries from the cluster.
     *
     * @param ct               cluster configuration
     * @param clusterId        cluster id
     * @param clusterLibraries libraries already installed on the cluster
     * @throws IOException
     * @throws DatabricksRestException
     */
    private void detachLibraries(ClusterTemplateDTO ct, String clusterId, Set<LibraryDTO> clusterLibraries) throws IOException, DatabricksRestException {
        getLog().info(String.format("Removing libraries from the cluster: name=[%s], id=[%s]", ct.getClusterName(), clusterId));
        Set<LibraryDTO> libsToDelete = getLibrariesToDelete(clusterLibraries, ct.getArtifactPaths());
        if (CollectionUtils.isNotEmpty(libsToDelete)) {
            LibraryService libraryService = getDatabricksServiceFactory().getLibraryService();
            libraryService.uninstall(clusterId, libsToDelete.toArray(new LibraryDTO[]{}));
        }
    }

    /**
     * Install new libraries on the cluster.
     *
     * @param ct               cluster configuration
     * @param clusterId        cluster id
     * @param clusterLibraries libraries already installed on the cluster
     * @throws IOException
     * @throws DatabricksRestException
     */
    private void attachLibraries(ClusterTemplateDTO ct, String clusterId, Set<LibraryDTO> clusterLibraries) throws IOException, DatabricksRestException {
        getLog().info(String.format("Attaching libraries to the cluster: name=[%s], id=[%s]", ct.getClusterName(), clusterId));
        Set<LibraryDTO> libsToInstall = getLibrariesToInstall(clusterLibraries, ct.getArtifactPaths());
        if (CollectionUtils.isNotEmpty(libsToInstall)) {
            getDatabricksServiceFactory().getLibraryService().install(clusterId, libsToInstall.toArray(new LibraryDTO[]{}));
        }
    }

    /**
     * Distinguish libraries which should be deployed on the cluster to achieve desired configuration.
     * At the moment only JAR files supported.
     *
     * @param clusterLibraries libraries already installed on the cluster
     * @param artifactPaths    libraries which should be installed in the end
     * @return libraries to install
     */
    private Set<LibraryDTO> getLibrariesToInstall(Set<LibraryDTO> clusterLibraries, Collection<String> artifactPaths) {
        if (CollectionUtils.isEmpty(artifactPaths)) {
            return Collections.emptySet();
        }

        Set clusterLibrariesPaths = clusterLibraries.stream().map(LibraryDTO::getJar).collect(Collectors.toSet());
        Set<LibraryDTO> libsToInstall = new HashSet<>();
        for (String artifactPath : artifactPaths) {
            // library already installed
            if (clusterLibrariesPaths.contains(artifactPath)) {
                getLog().info(String.format("Omitting deployment for [%s]. This library already installed", artifactPath));
                continue;
            }
            // library extension differs from .jar
            if (!artifactPath.endsWith(".jar")) {
                getLog().error(String.format("Cannot attach [%s]. Only .jar files supported", artifactPath));
                continue;
            }
            LibraryDTO lib = new LibraryDTO();
            lib.setJar(artifactPath);
            libsToInstall.add(lib);
        }

        getLog().info("Libraries to install: " + libsToInstall);
        return libsToInstall;
    }

    /**
     * Distinguish libraries which should be deleted from the cluster to achieve desired configuration.
     *
     * @param clusterLibraries libraries already installed on the cluster
     * @param artifactPaths    libraries which should be installed in the end
     * @return libraries to delete
     */
    private Set<LibraryDTO> getLibrariesToDelete(Set<LibraryDTO> clusterLibraries, Collection<String> artifactPaths) {
        Set<LibraryDTO> libsToDelete = clusterLibraries.stream()
                .filter(lib -> !artifactPaths.contains(lib.getJar()))
                .collect(Collectors.toSet());

        getLog().info("Libraries to delete: " + libsToDelete);
        return libsToDelete;
    }

    // TODO ClusterAttributesDTO.customTags - change type from Map<String, String> to ClusterTagDTO[]
    private ClusterTagDTO[] convertCustomTags(Map<String, String> customTags) {
        if (MapUtils.isEmpty(customTags)) {
            return new ClusterTagDTO[]{};
        }
        return customTags.entrySet().stream()
                .map(entry -> {
                    ClusterTagDTO ct = new ClusterTagDTO();
                    ct.setKey(entry.getKey());
                    ct.setValue(entry.getValue());
                    return ct;
                }).toArray(ClusterTagDTO[]::new);
    }

}
