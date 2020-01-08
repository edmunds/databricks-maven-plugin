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

import com.edmunds.rest.databricks.DTO.clusters.ClusterInfoDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.service.ClusterService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

/**
 * Common utilities needed for working with the clusters api.
 */
public class ClusterUtils {

    private static final Log log = new SystemStreamLog();

    /**
     * Convert cluster names to ids.
     *
     * @param clusterService clusterService
     * @param clusterNamesToConvert clusterNamesToConvert
     * @return clusters ids
     * @throws MojoExecutionException exception
     */
    public static List<String> convertClusterNamesToIds(ClusterService clusterService,
        Collection<String> clusterNamesToConvert) throws MojoExecutionException {
        List<String> clusterIds = new ArrayList<>();
        if (clusterNamesToConvert.isEmpty()) {
            return clusterIds;
        }
        Map<String, Integer> foundNames = new HashMap<>();
        try {
            ClusterInfoDTO[] clusters = clusterService.list();
            if (clusters == null) {
                throw new MojoExecutionException("Could not list clusters.");
            }
            for (ClusterInfoDTO cluster : clusters) {
                if (clusterNamesToConvert.contains(cluster.getClusterName())) {
                    clusterIds.add(cluster.getClusterId());
                    foundNames.merge(cluster.getClusterName(), 1, (a, b) -> a + b);
                }
            }
        } catch (DatabricksRestException | IOException e) {
            throw new MojoExecutionException("Could not list clusters.", e);
        }

        List<String> duplicateNames = foundNames.entrySet().stream().filter(x -> x.getValue() > 1)
            .map(x -> x.getKey() + "=" + x.getValue()).collect(Collectors.toList());
        if (duplicateNames.size() > 0) {
            log.error(String.format("Duplicate cluster names found: [%s]", duplicateNames.toString()));
        }

        List<String> notFoundNames = clusterNamesToConvert.stream()
            .filter(x -> !foundNames.containsKey(x)).collect(Collectors.toList());
        if (notFoundNames.size() > 0) {
            log.error(String.format("Some cluster names not found: [%s]", notFoundNames.toString()));
        }

        return clusterIds;
    }
}
