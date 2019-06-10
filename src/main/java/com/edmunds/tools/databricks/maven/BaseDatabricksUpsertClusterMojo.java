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

import com.edmunds.tools.databricks.maven.model.ClusterEnvironmentDTO;
import com.edmunds.tools.databricks.maven.model.ClusterSettingsDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

/**
 * Base class for Databricks UpsertCluster Mojos.
 */
public abstract class BaseDatabricksUpsertClusterMojo extends BaseDatabricksMojo {

    /**
     * The databricks cluster json file that contains all of the information for how to create databricks cluster.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-cluster-settings.json", property = "dbClusterFile")
    protected File dbClusterFile;

    private SettingsUtils<BaseDatabricksUpsertClusterMojo, ClusterEnvironmentDTO, ClusterSettingsDTO> settingsUtils;
    private SettingsInitializer<ClusterEnvironmentDTO, ClusterSettingsDTO> settingsInitializer =
            new BaseDatabricksUpsertClusterMojoSettingsInitializer(validate);

    public SettingsUtils<BaseDatabricksUpsertClusterMojo, ClusterEnvironmentDTO, ClusterSettingsDTO> getSettingsUtils() {
        if (settingsUtils == null) {
            settingsUtils = new SettingsUtils<>(
                    BaseDatabricksUpsertClusterMojo.class, ClusterSettingsDTO[].class, dbClusterFile, "/default-cluster.json",
                    createEnvironmentDTOSupplier(), settingsInitializer);
        }
        return settingsUtils;
    }

    protected EnvironmentDTOSupplier<ClusterEnvironmentDTO> createEnvironmentDTOSupplier() {
        return () -> {
            if (StringUtils.isBlank(databricksRepo)) {
                throw new MojoExecutionException("databricksRepo property is missing");
            }
            return new ClusterEnvironmentDTO(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
        };
    }

}
