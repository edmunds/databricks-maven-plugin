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

import com.edmunds.rest.databricks.DTO.ClusterTagDTO;
import com.edmunds.rest.databricks.DTO.LibraryDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.request.CreateClusterRequest;
import com.edmunds.rest.databricks.service.ClusterService;
import com.edmunds.rest.databricks.service.LibraryService;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateModel;
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

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

    /**
     * If true, command will fail if cluster with specified name already exists.
     */
    @Parameter(property = "failOnClusterExists")
    protected boolean failOnClusterExists = true;

    public void execute() throws MojoExecutionException {
        ClusterTemplateModel[] cts;
        try {
            cts = ObjectMapperUtils.deserialize(dbClusterFile, ClusterTemplateModel[].class);
        } catch (IOException e) {
            String config = dbClusterFile.getName();
            try {
                config = new String(Files.readAllBytes(Paths.get(dbClusterFile.toURI())));
            } catch (IOException e1) {
            }
            throw new MojoExecutionException("Failed to parse config: " + config, e);
        }

        for (ClusterTemplateModel ct : cts) {
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
                    attachLibraries(ct, clusterId);
                }
                // update existing cluster
                else {
                    logMessage = String.format("Updating cluster: name=[%s], id=[%s]", ct.getClusterName(), clusterId);
                    getLog().info(logMessage);
                    if (failOnClusterExists) {
                        throw new MojoExecutionException("Exception while " + logMessage + ". Cluster already exists");
                    }
                    //note that delete is an alias to terminate: https://docs.databricks.com/api/latest/clusters.html#delete-terminate
                    clusterService.delete(clusterId);
                    clusterId = clusterService.create(request);
                    attachLibraries(ct, clusterId);
                }
            } catch (DatabricksRestException | IOException e) {
                throw new MojoExecutionException("Exception while " + logMessage + ". ClusterTemplateModel=" + ct, e);
            }

        }
    }

    private void attachLibraries(ClusterTemplateModel ct, String clusterId) throws IOException, DatabricksRestException {
        getLog().info(String.format("Attaching libraries to the cluster: name=[%s], id=[%s], libs=[%s]",
                ct.getClusterName(), clusterId, ct.getArtifactPaths()));
        LibraryService libraryService = getDatabricksServiceFactory().getLibraryService();
        LibraryDTO[] libs = getLibraryDTO(ct.getArtifactPaths());
        if (ArrayUtils.isNotEmpty(libs)) {
            libraryService.install(clusterId, libs);
        }
    }

    private LibraryDTO[] getLibraryDTO(Collection<String> artifactPaths) {
        if (CollectionUtils.isEmpty(artifactPaths)) {
            return new LibraryDTO[]{};
        }

        LibraryDTO[] libs = new LibraryDTO[artifactPaths.size()];
        int i = 0;
        for (String artifactPath : artifactPaths) {
            if (!artifactPath.endsWith(".jar")) {
                getLog().error("Cannot attach library " + artifactPath + " - only .jar files supported");
                continue;
            }
            LibraryDTO lib = new LibraryDTO();
            lib.setJar(artifactPath);
            libs[i++] = lib;
        }
        return libs;
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
