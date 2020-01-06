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

import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.service.ClusterService;
import java.io.IOException;
import java.util.Arrays;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Cluster mojo, to perform basic ops on a databricks cluster (e.g. start, stop).
 */
@Mojo(name = "cluster")
public class ClusterMojo extends BaseDatabricksMojo {

    /**
     * This should be a list of cluster names to operate on.
     */
    @Parameter(property = "clusters", required = true)
    private String[] clusters;
    /**
     * What command to run.<br>
     * STOP - stop a cluster.<br>
     * START - start a cluster.<br>
     */
    @Parameter(property = "cluster.command", required = true)
    private ClusterCommand command;

    /**
     * Execute ClusterMojo.
     * @throws MojoExecutionException exception
     */
    public void execute() throws MojoExecutionException {
        for (String clusterId : convertClusterNamesToIds(getDatabricksServiceFactory().getClusterService(),
            Arrays.asList(clusters))) {
            try {

                getLog().info(String.format("preparing to [%s] cluster id: [%s]", command, clusterId));

                ClusterService clusterService = getDatabricksServiceFactory().getClusterService();
                switch (command) {
                    case STOP:
                        //note that delete is an alias to terminate: https://docs.databricks.com/api/latest/clusters.html#delete-terminate
                        clusterService.delete(clusterId);
                        break;
                    case START:
                        clusterService.start(clusterId);
                        break;
                    //TODO - consider adding restart?
                    default:
                        throw new IllegalStateException("No valid cluster command was found.");
                }

            } catch (DatabricksRestException | IOException e) {
                throw new MojoExecutionException(
                    String.format("Could not run command: [%s] on [%s]", command, clusterId), e);
            }
        }
    }

    /**
     * The cluster commands.
     */
    public enum ClusterCommand {
        STOP, START
    }
}
