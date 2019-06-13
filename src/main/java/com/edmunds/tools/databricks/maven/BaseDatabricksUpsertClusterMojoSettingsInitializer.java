package com.edmunds.tools.databricks.maven;

import com.edmunds.rest.databricks.DTO.AwsAttributesDTO;
import com.edmunds.rest.databricks.DTO.AwsAvailabilityDTO;
import com.edmunds.rest.databricks.DTO.ClusterLogConfDTO;
import com.edmunds.rest.databricks.DTO.EbsVolumeTypeDTO;
import com.edmunds.tools.databricks.maven.model.ClusterEnvironmentDTO;
import com.edmunds.tools.databricks.maven.model.ClusterSettingsDTO;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.util.Collection;
import java.util.Map;

/**
 * Class contains logic of {@link BaseDatabricksUpsertClusterMojo} Settings DTO fields initialization.
 */
public class BaseDatabricksUpsertClusterMojoSettingsInitializer implements SettingsInitializer<ClusterEnvironmentDTO, ClusterSettingsDTO> {

    private static final Log log = new SystemStreamLog();

    private final boolean validate;

    BaseDatabricksUpsertClusterMojoSettingsInitializer(boolean validate) {
        this.validate = validate;
    }

    @Override
    public void fillInDefaults(ClusterSettingsDTO settingsDTO, ClusterSettingsDTO defaultSettingsDTO, ClusterEnvironmentDTO environmentDTO) {
        String clusterName = settingsDTO.getClusterName();
        if (StringUtils.isEmpty(clusterName)) {
            clusterName = environmentDTO.getGroupWithoutCompany() + "/" + environmentDTO.getArtifactId();
            settingsDTO.setClusterName(clusterName);
            log.info(String.format("set CusterName with %s", clusterName));
        }

        int numWorkers = settingsDTO.getNumWorkers();
        if (numWorkers == 0) {
            numWorkers = defaultSettingsDTO.getNumWorkers();
            settingsDTO.setNumWorkers(numWorkers);
            log.info(String.format("%s|set NumWorkers with %s", clusterName, numWorkers));
        }

        String sparkVersion = settingsDTO.getSparkVersion();
        if (StringUtils.isEmpty(sparkVersion)) {
            sparkVersion = defaultSettingsDTO.getSparkVersion();
            settingsDTO.setSparkVersion(sparkVersion);
            log.info(String.format("%s|set SparkVersion with %s", clusterName, sparkVersion));
        }

        // AwsAttributes
        AwsAttributesDTO awsAttributes = settingsDTO.getAwsAttributes();
        AwsAttributesDTO awsAttributesDefault = defaultSettingsDTO.getAwsAttributes();
        if (awsAttributes == null) {
            awsAttributes = awsAttributesDefault;
            settingsDTO.setAwsAttributes(awsAttributes);
            log.info(String.format("%s|set AwsAttributes with %s", clusterName, awsAttributes));
        } else {
            AwsAvailabilityDTO availability = awsAttributes.getAvailability();
            if (availability == null) {
                availability = awsAttributesDefault.getAvailability();
                awsAttributes.setAvailability(availability);
                log.info(String.format("%s|set Availability with %s", clusterName, availability));
            }

            int ebsVolumeCount = awsAttributes.getEbsVolumeCount();
            if (ebsVolumeCount == 0) {
                ebsVolumeCount = awsAttributesDefault.getEbsVolumeCount();
                awsAttributes.setEbsVolumeCount(ebsVolumeCount);
                log.info(String.format("%s|set EbsVolumeCount with %s", clusterName, ebsVolumeCount));
            }

            int ebsVolumeSize = awsAttributes.getEbsVolumeSize();
            if (ebsVolumeSize == 0) {
                ebsVolumeSize = awsAttributesDefault.getEbsVolumeSize();
                awsAttributes.setEbsVolumeSize(ebsVolumeSize);
                log.info(String.format("%s|set EbsVolumeSize with %s", clusterName, ebsVolumeSize));
            }

            EbsVolumeTypeDTO ebsVolumeType = awsAttributes.getEbsVolumeType();
            if (ebsVolumeType == null) {
                ebsVolumeType = awsAttributesDefault.getEbsVolumeType();
                awsAttributes.setEbsVolumeType(ebsVolumeType);
                log.info(String.format("%s|set EbsVolumeType with %s", clusterName, ebsVolumeType));
            }

            // Should we handle this?
            int firstOnDemand = awsAttributes.getFirstOnDemand();
            if (firstOnDemand == 0) {
                firstOnDemand = awsAttributesDefault.getFirstOnDemand();
                awsAttributes.setFirstOnDemand(firstOnDemand);
                log.info(String.format("%s|set FirstOnDemand with %s", clusterName, firstOnDemand));
            }

            // Or just throw an exception here
            String instanceProfileArn = awsAttributes.getInstanceProfileArn();
            if (StringUtils.isEmpty(instanceProfileArn)) {
                instanceProfileArn = awsAttributesDefault.getInstanceProfileArn();
                awsAttributes.setInstanceProfileArn(instanceProfileArn);
                log.info(String.format("%s|set InstanceProfileArn with %s", clusterName, instanceProfileArn));
            }

            int spotBidPricePercent = awsAttributes.getSpotBidPricePercent();
            if (spotBidPricePercent == 0) {
                spotBidPricePercent = awsAttributesDefault.getSpotBidPricePercent();
                awsAttributes.setSpotBidPricePercent(spotBidPricePercent);
                log.info(String.format("%s|set SpotBidPricePercent with %s", clusterName, spotBidPricePercent));
            }

            String zoneId = awsAttributes.getZoneId();
            if (StringUtils.isEmpty(zoneId)) {
                zoneId = awsAttributesDefault.getZoneId();
                awsAttributes.setZoneId(zoneId);
                log.info(String.format("%s|set ZoneId with %s", clusterName, zoneId));
            }
        }

        String nodeTypeId = settingsDTO.getNodeTypeId();
        if (StringUtils.isEmpty(nodeTypeId)) {
            nodeTypeId = defaultSettingsDTO.getNodeTypeId();
            settingsDTO.setNodeTypeId(nodeTypeId);
            log.info(String.format("%s|set NodeTypeId with %s", clusterName, nodeTypeId));
        }

        // Do we need it?
        Map<String, String> sparkEnvVars = settingsDTO.getSparkEnvVars();
        if (MapUtils.isEmpty(sparkEnvVars)) {
            sparkEnvVars = defaultSettingsDTO.getSparkEnvVars();
            settingsDTO.setSparkEnvVars(sparkEnvVars);
            log.info(String.format("%s|set SparkEnvVars with %s", clusterName, sparkEnvVars));
        }

        int autoTerminationMinutes = settingsDTO.getAutoTerminationMinutes();
        if (autoTerminationMinutes == 0) {
            autoTerminationMinutes = defaultSettingsDTO.getAutoTerminationMinutes();
            settingsDTO.setAutoTerminationMinutes(autoTerminationMinutes);
            log.info(String.format("%s|set AutoTerminationMinutes with %s", clusterName, autoTerminationMinutes));
        }

        Collection<String> artifactPaths = settingsDTO.getArtifactPaths();
        if (CollectionUtils.isEmpty(artifactPaths)) {
            artifactPaths = defaultSettingsDTO.getArtifactPaths();
            settingsDTO.setArtifactPaths(artifactPaths);
            log.info(String.format("%s|set ArtifactPaths with %s", clusterName, artifactPaths));
        }

        String driverNodeTypeId = settingsDTO.getDriverNodeTypeId();
        if (StringUtils.isEmpty(driverNodeTypeId)) {
            driverNodeTypeId = defaultSettingsDTO.getDriverNodeTypeId();
            settingsDTO.setDriverNodeTypeId(driverNodeTypeId);
            log.info(String.format("%s|set DriverNodeTypeId with %s", clusterName, driverNodeTypeId));
        }

        Map<String, String> sparkConf = settingsDTO.getSparkConf();
        if (MapUtils.isEmpty(sparkConf)) {
            sparkConf = defaultSettingsDTO.getSparkConf();
            settingsDTO.setSparkConf(sparkConf);
            log.info(String.format("%s|set SparkConf with %s", clusterName, sparkConf));
        }

        Map<String, String> defaultCustomTags = defaultSettingsDTO.getCustomTags();
        if (MapUtils.isEmpty(settingsDTO.getCustomTags()) && MapUtils.isNotEmpty(defaultCustomTags)) {
            settingsDTO.setCustomTags(defaultCustomTags);
            log.info(String.format("%s|set CustomTags with %s", clusterName, defaultCustomTags));
        }

        ClusterLogConfDTO defaultClusterLogConf = defaultSettingsDTO.getClusterLogConf();
        if (settingsDTO.getClusterLogConf() == null && defaultClusterLogConf != null
                && (defaultClusterLogConf.getDbfs() != null || defaultClusterLogConf.getS3() != null)) {
            settingsDTO.setClusterLogConf(defaultClusterLogConf);
            log.info(String.format("%s|set ClusterLogConf with %s", clusterName, defaultClusterLogConf));
        }
    }

    @Override
    public void validate(ClusterSettingsDTO settingsDTO, ClusterEnvironmentDTO environmentDTO) throws MojoExecutionException {
        // Validate all cluster settings. If any fail terminate.
        if (validate) {
            if (StringUtils.isEmpty(settingsDTO.getClusterName())) {
                throw new MojoExecutionException("REQUIRED FIELD [cluster_name] was empty. VALIDATION FAILED.");
            }
        }
    }
}
