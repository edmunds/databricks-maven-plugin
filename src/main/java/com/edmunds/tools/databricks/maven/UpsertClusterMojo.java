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

import com.edmunds.rest.databricks.DTO.LibraryDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.request.CreateClusterRequest;
import com.edmunds.rest.databricks.service.ClusterService;
import com.edmunds.rest.databricks.service.LibraryService;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateModel;
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.edmunds.tools.databricks.maven.util.ClusterUtils.convertClusterNamesToIds;

/**
 * Cluster mojo, to perform databricks cluster upsert (create or update through recreation).
 */
@Mojo(name = "upsert-cluster")
public class UpsertClusterMojo extends BaseDatabricksMojo {

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperUtils.getObjectMapper();
    private static final Map<String, String> SPARK_ENV_VARS = new HashMap<String, String>() {{
        put("PYSPARK_PYTHON", "/databricks/python3/bin/python3");
    }};

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
            cts = OBJECT_MAPPER.readValue(dbClusterFile, ClusterTemplateModel[].class);
        } catch (IOException e) {
            throw new MojoExecutionException("Failed to parse config", e);
        }

        for (ClusterTemplateModel ct : cts) {
            ClusterService clusterService = getDatabricksServiceFactory().getClusterService();
            String clusterId = convertClusterNamesToIds(clusterService, Collections.singletonList(ct.getClusterName()))
                    .stream().findFirst().orElse(StringUtils.EMPTY);

            CreateClusterRequest request = new CreateClusterRequest.CreateClusterRequestBuilder(
                    ct.getNumWorkers(), ct.getClusterName(), ct.getSparkVersion(), ct.getNodeTypeId())
                    .withAwsAttributes(ct.getAwsAttributes())
                    .withAutoterminationMinutes(ct.getAutoterminationMinutes())
                    .withSparkEnvVars(SPARK_ENV_VARS)
                    .build();

            String logMessage = String.format("Creating cluster: name=[%s]", ct.getClusterName());
            try {
                if (StringUtils.isNotEmpty(clusterId)) {
                    logMessage = String.format("Updating cluster: name=[%s], id=[%s]", ct.getClusterName(), clusterId);
                    if (failOnClusterExists) {
                        throw new MojoExecutionException("Exception while " + logMessage + ". Cluster already exists");
                    }
                }
                getLog().info(logMessage);
                if (StringUtils.isNotEmpty(clusterId)) {
                    //note that delete is an alias to terminate: https://docs.databricks.com/api/latest/clusters.html#delete-terminate
                    clusterService.delete(clusterId);
                }
                clusterId = clusterService.create(request);

                getLog().info(String.format("Attaching libraries to the cluster: name=[%s], id=[%s], libs=[%s]",
                        ct.getClusterName(), clusterId, ct.getArtifactPaths()));
                LibraryService libraryService = getDatabricksServiceFactory().getLibraryService();
                LibraryDTO[] libs = getLibraryDTO(ct.getArtifactPaths());
                if (ArrayUtils.isNotEmpty(libs)) {
                    libraryService.install(clusterId, libs);
                }
            } catch (DatabricksRestException | IOException e) {
                throw new MojoExecutionException("Exception while " + logMessage, e);
            }
        }
    }

    private LibraryDTO[] getLibraryDTO(Collection<String> artifactPaths) {
        if (CollectionUtils.isEmpty(artifactPaths)) {
            return new LibraryDTO[]{};
        }

        LibraryDTO[] libs = new LibraryDTO[artifactPaths.size()];
        int i = 0;
        for (String artifactPath : artifactPaths) {
            LibraryDTO lib = new LibraryDTO();
            lib.setJar(artifactPath);
            libs[i++] = lib;
        }
        return libs;
    }

}
