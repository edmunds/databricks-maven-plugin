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

package com.edmunds.tools.databricks.maven.util;

import com.edmunds.rest.databricks.DTO.ClusterInfoDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.service.ClusterService;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Common utilities needed for working with the clusters api.
 */
public class ClusterUtils {

    public static List<String> convertClusterNamesToIds(ClusterService clusterService, Collection<String> clusterNamesToConvert) throws MojoExecutionException {
        List<String> clusterIds = new ArrayList<>();
        if (clusterNamesToConvert.isEmpty()) {
            return clusterIds;
        }
        try {
            ClusterInfoDTO[] clusters = clusterService.list();
            if (clusters == null) {
                throw new MojoExecutionException("Could not list clusters.");
            }
            for (ClusterInfoDTO cluster : clusters) {
                if (clusterNamesToConvert.contains(cluster.getClusterName())) {
                    clusterIds.add(cluster.getClusterId());
                }
            }
        } catch (DatabricksRestException | IOException e) {
            throw new MojoExecutionException("Could not list clusters.", e);
        }
        //TODO should log if not all clusters could be found!
        return clusterIds;
    }
}
