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
import com.edmunds.rest.databricks.DTO.EbsVolumeTypeDTO;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateDTO;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateModel;
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

public abstract class BaseDatabricksUpsertClusterMojo extends BaseDatabricksMojo {

    private final SettingsUtils<ClusterTemplateModel> settingsUtils = new SettingsUtils<>();

    /**
     * The databricks cluster json file that contains all of the information for how to create databricks cluster.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-cluster-settings.json", property = "dbClusterFile")
    protected File dbClusterFile;

    protected ClusterTemplateDTO[] getClusterTemplateDTOs() throws MojoExecutionException {
        return loadClusterTemplateDTOsFromFile(dbClusterFile);
    }

    protected ClusterTemplateDTO[] loadClusterTemplateDTOsFromFile(File clustersConfig) throws MojoExecutionException {
        if (!clustersConfig.exists()) {
            getLog().info("No clusters config file exists");
            return new ClusterTemplateDTO[]{};
        }
        ClusterTemplateDTO[] cts;
        try {
            cts = ObjectMapperUtils.deserialize(clustersConfig, ClusterTemplateDTO[].class);
        } catch (IOException e) {
            String config = clustersConfig.getName();
            try {
                config = new String(Files.readAllBytes(Paths.get(clustersConfig.toURI())));
            } catch (IOException ex) {
                // Exception while trying to read configuration file content. No need to log it
            }
            throw new MojoExecutionException("Failed to parse config: " + config, e);
        }
        return cts;
    }

    protected ClusterTemplateModel getClusterTemplateModel() throws MojoExecutionException {
        if (StringUtils.isBlank(databricksRepo)) {
            throw new MojoExecutionException("databricksRepo property is missing");
        }
        return new ClusterTemplateModel(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
    }

    ClusterTemplateDTO[] buildClusterTemplateDTOsWithDefault() throws MojoExecutionException {
        ClusterTemplateModel clusterTemplateModel = getClusterTemplateModel();
        String clusterSettings = settingsUtils.getSettingsFromTemplate("clusterSettings", dbClusterFile, clusterTemplateModel);
        if (clusterSettings == null) {
            return new ClusterTemplateDTO[]{};
        }

        ClusterTemplateDTO defaultClusterTemplateDTO = defaultClusterTemplateDTO();

        ClusterTemplateDTO[] clusterTemplateDTOs = deserializeClusterTemplateDTOs(clusterSettings, readDefaultCluster());
        for (ClusterTemplateDTO clusterTemplateDTO : clusterTemplateDTOs) {
            try {
                fillInDefaultClusterSettings(clusterTemplateDTO, defaultClusterTemplateDTO, clusterTemplateModel);
            } catch (JsonProcessingException e) {
                throw new MojoExecutionException("Fail to fill empty-value with default", e);
            }

            // Validate all cluster settings. If any fail terminate.
            if (validate) {
                validateClusterTemplate(clusterTemplateDTO, clusterTemplateModel);
            }
        }

        return clusterTemplateDTOs;
    }

    private void validateClusterTemplate(ClusterTemplateDTO clusterTemplateDTO, ClusterTemplateModel clusterTemplateModel) throws MojoExecutionException {
        int numWorkers = clusterTemplateDTO.getNumWorkers();
        if (numWorkers == 0) {
            throw new MojoExecutionException("REQUIRED FIELD [num_workers] was empty. VALIDATION FAILED.");
        }
    }

    /**
     * FIXME - it is possible for the example to be invalid, and the cluster file being valid. This should be fixed.
     *
     * <p>
     * Default ClusterTemplateDTO is used to fill the value when user cluster has missing value.
     *
     * @return
     * @throws MojoExecutionException
     */
    public ClusterTemplateDTO defaultClusterTemplateDTO() throws MojoExecutionException {
        return deserializeClusterTemplateDTOs(settingsUtils.getModelFromTemplate(readDefaultCluster(), getClusterTemplateModel()), readDefaultCluster())[0];
    }

    private static ClusterTemplateDTO[] deserializeClusterTemplateDTOs(String settingsJson, String defaultSettingsJson) throws MojoExecutionException {
        try {
            return ObjectMapperUtils.deserialize(settingsJson, ClusterTemplateDTO[].class);
        } catch (IOException e) {
            throw new MojoExecutionException(String.format("Failed to unmarshal cluster templates to object:\n[%s]\nHere is an example, of what it should look like:\n[%s]\n",
                    settingsJson,
                    defaultSettingsJson), e);
        }
    }

    private static String readDefaultCluster() {
        try {
            return IOUtils.toString(BaseDatabricksUpsertClusterMojo.class.getResourceAsStream("/default-cluster.json"), Charset.defaultCharset());
        } catch (Exception e) {
            return ExceptionUtils.getStackTrace(e);
        }
    }

    /**
     * Check the value of targetDTO and fill targetDTO with defaultDTO if value do not exist.
     *
     * @param targetDTO
     * @param defaultDTO
     * @throws JsonProcessingException
     */
    public void fillInDefaultClusterSettings(ClusterTemplateDTO targetDTO, ClusterTemplateDTO defaultDTO, ClusterTemplateModel clusterTemplateModel) throws JsonProcessingException {

        String clusterName = targetDTO.getClusterName();
        if (StringUtils.isEmpty(clusterName)) {
            clusterName = clusterTemplateModel.getGroupWithoutCompany() + "/" + clusterTemplateModel.getArtifactId();
            targetDTO.setClusterName(clusterName);
            getLog().info(String.format("set CusterName with %s", clusterName));
        }

        int numWorkers = targetDTO.getNumWorkers();
        if (numWorkers == 0) {
            numWorkers = defaultDTO.getNumWorkers();
            targetDTO.setNumWorkers(numWorkers);
            getLog().info(String.format("%s|set NumWorkers with %s", clusterName, numWorkers));
        }

        String sparkVersion = targetDTO.getSparkVersion();
        if (StringUtils.isEmpty(sparkVersion)) {
            sparkVersion = defaultDTO.getSparkVersion();
            targetDTO.setSparkVersion(sparkVersion);
            getLog().info(String.format("%s|set SparkVersion with %s", clusterName, sparkVersion));
        }

        // AwsAttributes
        AwsAttributesDTO awsAttributes = targetDTO.getAwsAttributes();
        AwsAttributesDTO awsAttributesDefault = defaultDTO.getAwsAttributes();
        if (awsAttributes == null) {
            awsAttributes = awsAttributesDefault;
            targetDTO.setAwsAttributes(awsAttributes);
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

        String nodeTypeId = targetDTO.getNodeTypeId();
        if (StringUtils.isEmpty(nodeTypeId)) {
            nodeTypeId = defaultDTO.getNodeTypeId();
            targetDTO.setNodeTypeId(nodeTypeId);
            getLog().info(String.format("%s|set NodeTypeId with %s", clusterName, nodeTypeId));
        }

        // Do we need it?
        Map<String, String> sparkEnvVars = targetDTO.getSparkEnvVars();
        if (MapUtils.isEmpty(sparkEnvVars)) {
            sparkEnvVars = defaultDTO.getSparkEnvVars();
            targetDTO.setSparkEnvVars(sparkEnvVars);
            getLog().info(String.format("%s|set SparkEnvVars with %s", clusterName, sparkEnvVars));
        }

        int autoTerminationMinutes = targetDTO.getAutoTerminationMinutes();
        if (autoTerminationMinutes == 0) {
            autoTerminationMinutes = defaultDTO.getAutoTerminationMinutes();
            targetDTO.setAutoTerminationMinutes(autoTerminationMinutes);
            getLog().info(String.format("%s|set AutoTerminationMinutes with %s", clusterName, autoTerminationMinutes));
        }

        Collection<String> artifactPaths = targetDTO.getArtifactPaths();
        if (CollectionUtils.isEmpty(artifactPaths)) {
            artifactPaths = defaultDTO.getArtifactPaths();
            targetDTO.setArtifactPaths(artifactPaths);
            getLog().info(String.format("%s|set ArtifactPaths with %s", clusterName, artifactPaths));
        }

        String driverNodeTypeId = targetDTO.getDriverNodeTypeId();
        if (StringUtils.isEmpty(driverNodeTypeId)) {
            driverNodeTypeId = defaultDTO.getDriverNodeTypeId();
            targetDTO.setDriverNodeTypeId(driverNodeTypeId);
            getLog().info(String.format("%s|set DriverNodeTypeId with %s", clusterName, driverNodeTypeId));
        }

        Map<String, String> sparkConf = targetDTO.getSparkConf();
        if (MapUtils.isEmpty(sparkConf)) {
            sparkConf = defaultDTO.getSparkConf();
            targetDTO.setSparkConf(sparkConf);
            getLog().info(String.format("%s|set SparkConf with %s", clusterName, sparkConf));
        }

//        Map<String, String> customTags = targetDTO.getCustomTags();
//        if (MapUtils.isEmpty(customTags)) {
//            customTags = defaultDTO.getCustomTags();
//            targetDTO.setCustomTags(customTags);
//            getLog().info(String.format("%s|set CustomTags with %s", clusterName, customTags));
//        }

//        ClusterLogConfDTO clusterLogConf = targetDTO.getClusterLogConf();
//        if (clusterLogConf == null) {
//            clusterLogConf = defaultDTO.getClusterLogConf();
//            targetDTO.setClusterLogConf(clusterLogConf);
//            getLog().info(String.format("%s|set ClusterLogConf with %s", clusterName, clusterLogConf));
//        }
    }

}
