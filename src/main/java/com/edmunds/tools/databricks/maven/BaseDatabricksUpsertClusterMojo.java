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

import com.edmunds.rest.databricks.DTO.AwsAttributesDTO;
import com.edmunds.rest.databricks.DTO.AwsAvailabilityDTO;
import com.edmunds.rest.databricks.DTO.ClusterLogConfDTO;
import com.edmunds.rest.databricks.DTO.EbsVolumeTypeDTO;
import com.edmunds.tools.databricks.maven.model.ClusterSettingsDTO;
import com.edmunds.tools.databricks.maven.model.ClusterEnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public abstract class BaseDatabricksUpsertClusterMojo extends BaseDatabricksMojo {

    private SettingsUtils<BaseDatabricksUpsertClusterMojo, ClusterEnvironmentDTO, ClusterSettingsDTO> settingsUtils;

    /**
     * The databricks cluster json file that contains all of the information for how to create databricks cluster.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-cluster-settings.json", property = "dbClusterFile")
    protected File dbClusterFile;

    public SettingsUtils<BaseDatabricksUpsertClusterMojo, ClusterEnvironmentDTO, ClusterSettingsDTO> getSettingsUtils() {
        if (settingsUtils == null) {
            settingsUtils = new SettingsUtils<>(
                    BaseDatabricksUpsertClusterMojo.class, ClusterSettingsDTO[].class, "/default-cluster.json",
                    createEnvironmentDTOSupplier(), createSettingsInitializer());
        }
        return settingsUtils;
    }

    protected EnvironmentDTOSupplier<ClusterEnvironmentDTO> createEnvironmentDTOSupplier() {
        return new EnvironmentDTOSupplier<ClusterEnvironmentDTO>() {
            @Override
            public ClusterEnvironmentDTO get() throws MojoExecutionException {
                if (StringUtils.isBlank(databricksRepo)) {
                    throw new MojoExecutionException("databricksRepo property is missing");
                }
                return new ClusterEnvironmentDTO(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
            }

            @Override
            public File getEnvironmentDTOFile() {
                return dbClusterFile;
            }
        };
    }

    private SettingsInitializer<ClusterEnvironmentDTO, ClusterSettingsDTO> createSettingsInitializer() {
        return new SettingsInitializer<ClusterEnvironmentDTO, ClusterSettingsDTO>() {
            @Override
            public void fillInDefaults(ClusterSettingsDTO settingsDTO, ClusterSettingsDTO defaultSettingsDTO, ClusterEnvironmentDTO environmentDTO) {
                String clusterName = settingsDTO.getClusterName();
                if (StringUtils.isEmpty(clusterName)) {
                    clusterName = environmentDTO.getGroupWithoutCompany() + "/" + environmentDTO.getArtifactId();
                    settingsDTO.setClusterName(clusterName);
                    getLog().info(String.format("set CusterName with %s", clusterName));
                }

                int numWorkers = settingsDTO.getNumWorkers();
                if (numWorkers == 0) {
                    numWorkers = defaultSettingsDTO.getNumWorkers();
                    settingsDTO.setNumWorkers(numWorkers);
                    getLog().info(String.format("%s|set NumWorkers with %s", clusterName, numWorkers));
                }

                String sparkVersion = settingsDTO.getSparkVersion();
                if (StringUtils.isEmpty(sparkVersion)) {
                    sparkVersion = defaultSettingsDTO.getSparkVersion();
                    settingsDTO.setSparkVersion(sparkVersion);
                    getLog().info(String.format("%s|set SparkVersion with %s", clusterName, sparkVersion));
                }

                // AwsAttributes
                AwsAttributesDTO awsAttributes = settingsDTO.getAwsAttributes();
                AwsAttributesDTO awsAttributesDefault = defaultSettingsDTO.getAwsAttributes();
                if (awsAttributes == null) {
                    awsAttributes = awsAttributesDefault;
                    settingsDTO.setAwsAttributes(awsAttributes);
                    getLog().info(String.format("%s|set AwsAttributes with %s", clusterName, awsAttributes));
                } else {
                    AwsAvailabilityDTO availability = awsAttributes.getAvailability();
                    if (availability == null) {
                        availability = awsAttributesDefault.getAvailability();
                        awsAttributes.setAvailability(availability);
                        getLog().info(String.format("%s|set Availability with %s", clusterName, availability));
                    }

                    int ebsVolumeCount = awsAttributes.getEbsVolumeCount();
                    if (ebsVolumeCount == 0) {
                        ebsVolumeCount = awsAttributesDefault.getEbsVolumeCount();
                        awsAttributes.setEbsVolumeCount(ebsVolumeCount);
                        getLog().info(String.format("%s|set EbsVolumeCount with %s", clusterName, ebsVolumeCount));
                    }

                    int ebsVolumeSize = awsAttributes.getEbsVolumeSize();
                    if (ebsVolumeSize == 0) {
                        ebsVolumeSize = awsAttributesDefault.getEbsVolumeSize();
                        awsAttributes.setEbsVolumeSize(ebsVolumeSize);
                        getLog().info(String.format("%s|set EbsVolumeSize with %s", clusterName, ebsVolumeSize));
                    }

                    EbsVolumeTypeDTO ebsVolumeType = awsAttributes.getEbsVolumeType();
                    if (ebsVolumeType == null) {
                        ebsVolumeType = awsAttributesDefault.getEbsVolumeType();
                        awsAttributes.setEbsVolumeType(ebsVolumeType);
                        getLog().info(String.format("%s|set EbsVolumeType with %s", clusterName, ebsVolumeType));
                    }

                    // Should we handle this?
                    int firstOnDemand = awsAttributes.getFirstOnDemand();
                    if (firstOnDemand == 0) {
                        firstOnDemand = awsAttributesDefault.getFirstOnDemand();
                        awsAttributes.setFirstOnDemand(firstOnDemand);
                        getLog().info(String.format("%s|set FirstOnDemand with %s", clusterName, firstOnDemand));
                    }

                    // Or just throw an exception here
                    String instanceProfileArn = awsAttributes.getInstanceProfileArn();
                    if (StringUtils.isEmpty(instanceProfileArn)) {
                        instanceProfileArn = awsAttributesDefault.getInstanceProfileArn();
                        awsAttributes.setInstanceProfileArn(instanceProfileArn);
                        getLog().info(String.format("%s|set InstanceProfileArn with %s", clusterName, instanceProfileArn));
                    }

                    int spotBidPricePercent = awsAttributes.getSpotBidPricePercent();
                    if (spotBidPricePercent == 0) {
                        spotBidPricePercent = awsAttributesDefault.getSpotBidPricePercent();
                        awsAttributes.setSpotBidPricePercent(spotBidPricePercent);
                        getLog().info(String.format("%s|set SpotBidPricePercent with %s", clusterName, spotBidPricePercent));
                    }

                    String zoneId = awsAttributes.getZoneId();
                    if (StringUtils.isEmpty(zoneId)) {
                        zoneId = awsAttributesDefault.getZoneId();
                        awsAttributes.setZoneId(zoneId);
                        getLog().info(String.format("%s|set ZoneId with %s", clusterName, zoneId));
                    }
                }

                String nodeTypeId = settingsDTO.getNodeTypeId();
                if (StringUtils.isEmpty(nodeTypeId)) {
                    nodeTypeId = defaultSettingsDTO.getNodeTypeId();
                    settingsDTO.setNodeTypeId(nodeTypeId);
                    getLog().info(String.format("%s|set NodeTypeId with %s", clusterName, nodeTypeId));
                }

                // Do we need it?
                Map<String, String> sparkEnvVars = settingsDTO.getSparkEnvVars();
                if (MapUtils.isEmpty(sparkEnvVars)) {
                    sparkEnvVars = defaultSettingsDTO.getSparkEnvVars();
                    settingsDTO.setSparkEnvVars(sparkEnvVars);
                    getLog().info(String.format("%s|set SparkEnvVars with %s", clusterName, sparkEnvVars));
                }

                int autoTerminationMinutes = settingsDTO.getAutoTerminationMinutes();
                if (autoTerminationMinutes == 0) {
                    autoTerminationMinutes = defaultSettingsDTO.getAutoTerminationMinutes();
                    settingsDTO.setAutoTerminationMinutes(autoTerminationMinutes);
                    getLog().info(String.format("%s|set AutoTerminationMinutes with %s", clusterName, autoTerminationMinutes));
                }

                Collection<String> artifactPaths = settingsDTO.getArtifactPaths();
                if (CollectionUtils.isEmpty(artifactPaths)) {
                    artifactPaths = defaultSettingsDTO.getArtifactPaths();
                    settingsDTO.setArtifactPaths(artifactPaths);
                    getLog().info(String.format("%s|set ArtifactPaths with %s", clusterName, artifactPaths));
                }

                String driverNodeTypeId = settingsDTO.getDriverNodeTypeId();
                if (StringUtils.isEmpty(driverNodeTypeId)) {
                    driverNodeTypeId = defaultSettingsDTO.getDriverNodeTypeId();
                    settingsDTO.setDriverNodeTypeId(driverNodeTypeId);
                    getLog().info(String.format("%s|set DriverNodeTypeId with %s", clusterName, driverNodeTypeId));
                }

                Map<String, String> sparkConf = settingsDTO.getSparkConf();
                if (MapUtils.isEmpty(sparkConf)) {
                    sparkConf = defaultSettingsDTO.getSparkConf();
                    settingsDTO.setSparkConf(sparkConf);
                    getLog().info(String.format("%s|set SparkConf with %s", clusterName, sparkConf));
                }

                Map<String, String> defaultCustomTags = defaultSettingsDTO.getCustomTags();
                if (MapUtils.isEmpty(settingsDTO.getCustomTags()) && MapUtils.isNotEmpty(defaultCustomTags)) {
                    settingsDTO.setCustomTags(defaultCustomTags);
                    getLog().info(String.format("%s|set CustomTags with %s", clusterName, defaultCustomTags));
                }

                ClusterLogConfDTO defaultClusterLogConf = defaultSettingsDTO.getClusterLogConf();
                if (settingsDTO.getClusterLogConf() == null && defaultClusterLogConf != null
                        && (defaultClusterLogConf.getDbfs() != null || defaultClusterLogConf.getS3() != null)) {
                    settingsDTO.setClusterLogConf(defaultClusterLogConf);
                    getLog().info(String.format("%s|set ClusterLogConf with %s", clusterName, defaultClusterLogConf));
                }
            }

            @Override
            public void validate(ClusterSettingsDTO settingsDTO, ClusterEnvironmentDTO environmentDTO) throws MojoExecutionException {
                // Validate all cluster settings. If any fail terminate.
                if (validate) {
                    int numWorkers = settingsDTO.getNumWorkers();
                    if (numWorkers == 0) {
                        throw new MojoExecutionException("REQUIRED FIELD [num_workers] was empty. VALIDATION FAILED.");
                    }
                }
            }
        };
    }

}
