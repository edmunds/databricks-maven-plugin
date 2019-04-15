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
import com.edmunds.tools.databricks.maven.model.ClusterTemplateModel;
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
@Mojo(name = "upsert-cluster")
public class UpsertClusterMojo extends BaseDatabricksMojo {

    /**
     * The databricks cluster json file that contains all of the information for how to create databricks cluster.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-cluster-settings.json", property = "dbClusterFile")
    protected File dbClusterFile;

    public void execute() throws MojoExecutionException {
        ClusterTemplateModel[] cts;
        try {
            cts = ObjectMapperUtils.deserialize(dbClusterFile, ClusterTemplateModel[].class);
        } catch (IOException e) {
            String config = dbClusterFile.getName();
            try {
                config = new String(Files.readAllBytes(Paths.get(dbClusterFile.toURI())));
            } catch (IOException ex) {
            }
            throw new MojoExecutionException("Failed to parse config: " + config, e);
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool(cts.length);
        for (ClusterTemplateModel ct : cts) {
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
                            } catch (DatabricksRestException | IOException | InterruptedException e) {
                                throw new MojoExecutionException("Exception while " + logMessage + ". ClusterTemplateModel=" + ct, e);
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

    private void editCluster(ClusterTemplateModel ct, String clusterId) throws IOException, DatabricksRestException {
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

    private void startCluster(ClusterTemplateModel ct, String clusterId) throws IOException, DatabricksRestException, InterruptedException {
        ClusterService clusterService = getDatabricksServiceFactory().getClusterService();
        ClusterStateDTO clusterState = clusterService.getInfo(clusterId).getState();
        if (clusterState != ClusterStateDTO.RUNNING) {
            getLog().info(String.format("Starting cluster: name=[%s], id=[%s]. Current state=[%s]", ct.getClusterName(), clusterId, clusterState));
            if (clusterState == ClusterStateDTO.TERMINATED || clusterState == ClusterStateDTO.TERMINATING
                    || clusterState == ClusterStateDTO.ERROR || clusterState == ClusterStateDTO.UNKNOWN) {
                clusterService.start(clusterId);
            }
            while (clusterService.getInfo(clusterId).getState() != ClusterStateDTO.RUNNING) {
                Thread.sleep(5000);
            }
        }
    }

    private Set<LibraryDTO> getClusterLibraries(String clusterId) throws IOException, DatabricksRestException {
        return Arrays.stream(getDatabricksServiceFactory().getLibraryService().clusterStatus(clusterId).getLibraryFullStatuses())
                .filter(status -> !status.isLibraryForAllClusters())
                .map(LibraryFullStatusDTO::getLibrary)
                .collect(Collectors.toSet());
    }

    private void detachLibraries(ClusterTemplateModel ct, String clusterId, Set<LibraryDTO> clusterLibraries) throws IOException, DatabricksRestException {
        getLog().info(String.format("Removing libraries from the cluster: name=[%s], id=[%s]", ct.getClusterName(), clusterId));
        Set<LibraryDTO> libsToDelete = getLibrariesToDelete(clusterLibraries, ct.getArtifactPaths());
        if (CollectionUtils.isNotEmpty(libsToDelete)) {
            LibraryService libraryService = getDatabricksServiceFactory().getLibraryService();
            libraryService.uninstall(clusterId, libsToDelete.toArray(new LibraryDTO[]{}));
        }
    }

    private void attachLibraries(ClusterTemplateModel ct, String clusterId, Set<LibraryDTO> clusterLibraries) throws IOException, DatabricksRestException {
        getLog().info(String.format("Attaching libraries to the cluster: name=[%s], id=[%s]", ct.getClusterName(), clusterId));
        Set<LibraryDTO> libsToInstall = getLibrariesToInstall(clusterLibraries, ct.getArtifactPaths());
        if (CollectionUtils.isNotEmpty(libsToInstall)) {
            getDatabricksServiceFactory().getLibraryService().install(clusterId, libsToInstall.toArray(new LibraryDTO[]{}));
        }
    }

    private Set<LibraryDTO> getLibrariesToInstall(Set<LibraryDTO> clusterLibraries, Collection<String> artifactPaths) {
        if (CollectionUtils.isEmpty(artifactPaths)) {
            return Collections.emptySet();
        }

        Set clusterLibrariesPaths = clusterLibraries.stream().map(LibraryDTO::getJar).collect(Collectors.toSet());
        Set<LibraryDTO> libsToInstall = new HashSet<>();
        for (String artifactPath : artifactPaths) {
            // library already installed
            if (clusterLibrariesPaths.contains(artifactPath)) {
                continue;
            }
            if (!artifactPath.endsWith(".jar")) {
                getLog().error("Cannot attach library " + artifactPath + " - only .jar files supported");
                continue;
            }
            LibraryDTO lib = new LibraryDTO();
            lib.setJar(artifactPath);
            libsToInstall.add(lib);
        }

        getLog().info("Libraries to install: " + libsToInstall);
        return libsToInstall;
    }

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
