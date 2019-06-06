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

package com.edmunds.tools.databricks.maven.util;

import com.edmunds.rest.databricks.DTO.AwsAttributesDTO;
import com.edmunds.rest.databricks.DTO.AwsAvailabilityDTO;
import com.edmunds.rest.databricks.DTO.ClusterLogConfDTO;
import com.edmunds.rest.databricks.DTO.EbsVolumeTypeDTO;
import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.tools.databricks.maven.model.BaseModel;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateDTO;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateModel;
import com.edmunds.tools.databricks.maven.model.JobTemplateModel;
import com.edmunds.tools.databricks.maven.validation.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import freemarker.cache.FileTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SettingsUtils {

    public static final String TEAM_TAG = "team";
    public static final String DELTA_TAG = "delta";
    public static final ObjectMapper OBJECT_MAPPER = ObjectMapperUtils.getObjectMapper();

    private static Log log;

    public static Log getLog() {
        if (log == null) {
            log = new SystemStreamLog();
        }

        return log;
    }

    public static String getJobSettingsFromTemplate(String settingsName, File settingsFile, BaseModel templateModel) throws MojoExecutionException {
        if (!settingsFile.exists()) {
            getLog().info(String.format("No %s file exists", settingsName));
            return null;
        }
        StringWriter stringWriter = new StringWriter();
        try {
            TemplateLoader templateLoader = new FileTemplateLoader(settingsFile.getParentFile());
            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate(settingsFile.getName());
            temp.process(templateModel, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process %s file as template: [%s]\nFreemarker message:\n%s", settingsName, settingsFile.getAbsolutePath(), e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    public static Configuration getFreemarkerConfiguration(TemplateLoader templateLoader) throws IOException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setTemplateLoader(templateLoader);
        cfg.setDefaultEncoding(Charset.defaultCharset().name());
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        return cfg;
    }

    /**
     * Check the value of targetDTO and fill targetDTO with defaultDTO if value do not exist.
     *
     * @param targetDTO
     * @param defaultDTO
     * @param jobTemplateModel
     */
    public static void fillInDefaultJobSettings(JobSettingsDTO targetDTO, JobSettingsDTO defaultDTO, JobTemplateModel jobTemplateModel) throws JsonProcessingException {

        String jobName = targetDTO.getName();
        if (StringUtils.isEmpty(targetDTO.getName())) {
            jobName = jobTemplateModel.getGroupWithoutCompany() + "/" + jobTemplateModel.getArtifactId();
            targetDTO.setName(jobName);
            getLog().info(String.format("set JobName with %s", jobName));
        }

        // email_notifications
        if (targetDTO.getEmailNotifications() == null) {
            targetDTO.setEmailNotifications(SerializationUtils.clone(defaultDTO.getEmailNotifications()));
            getLog().info(String.format("%s|set email_notifications with %s"
                    , jobName, OBJECT_MAPPER.writeValueAsString(defaultDTO.getEmailNotifications())));

        } else if (targetDTO.getEmailNotifications().getOnFailure() == null
                || targetDTO.getEmailNotifications().getOnFailure().length == 0
                || StringUtils.isEmpty(targetDTO.getEmailNotifications().getOnFailure()[0])) {
            targetDTO.getEmailNotifications().setOnFailure(defaultDTO.getEmailNotifications().getOnFailure());
            getLog().info(String.format("%s|set email_notifications.on_failure with %s"
                    , jobName, OBJECT_MAPPER.writeValueAsString(defaultDTO.getEmailNotifications().getOnFailure())));
        }

        // ClusterInfo
        if (StringUtils.isEmpty(targetDTO.getExistingClusterId())) {
            if (targetDTO.getNewCluster() == null) {
                targetDTO.setNewCluster(SerializationUtils.clone(defaultDTO.getNewCluster()));
                getLog().info(String.format("%s|set new_cluster with %s", jobName, OBJECT_MAPPER.writeValueAsString(defaultDTO.getNewCluster())));

            } else {
                if (StringUtils.isEmpty(targetDTO.getNewCluster().getSparkVersion())) {
                    targetDTO.getNewCluster().setSparkVersion(defaultDTO.getNewCluster().getSparkVersion());
                    getLog().info(String.format("%s|set new_cluster.spark_version with %s", jobName, defaultDTO.getNewCluster().getSparkVersion()));
                }

                if (StringUtils.isEmpty(targetDTO.getNewCluster().getNodeTypeId())) {
                    targetDTO.getNewCluster().setNodeTypeId(defaultDTO.getNewCluster().getNodeTypeId());
                    getLog().info(String.format("%s|set new_cluster.node_type_id with %s", jobName, defaultDTO.getNewCluster().getNodeTypeId()));
                }

                if (targetDTO.getNewCluster().getAutoScale() == null && targetDTO.getNewCluster().getNumWorkers() < 1) {
                    targetDTO.getNewCluster().setNumWorkers(defaultDTO.getNewCluster().getNumWorkers());
                    getLog().info(String.format("%s|set new_cluster.num_workers with %s", jobName, defaultDTO.getNewCluster().getNumWorkers()));
                }


                //aws_attributes
                if (targetDTO.getNewCluster().getAwsAttributes() == null) {
                    targetDTO.getNewCluster().setAwsAttributes(SerializationUtils.clone(defaultDTO.getNewCluster().getAwsAttributes()));
                    getLog().info(String.format("%s|set new_cluster.aws_attributes with %s"
                            , jobName, OBJECT_MAPPER.writeValueAsString(defaultDTO.getNewCluster().getAwsAttributes())));
                }
            }
        }

        if (targetDTO.getTimeoutSeconds() == null) {
            targetDTO.setTimeoutSeconds(defaultDTO.getTimeoutSeconds());
            getLog().info(String.format("%s|set timeout_seconds with %s", jobName, defaultDTO.getTimeoutSeconds()));
        }

        // Can't have libraries if its a spark submit task
        if ((targetDTO.getLibraries() == null || targetDTO.getLibraries().length == 0) && targetDTO.getSparkSubmitTask() == null) {
            targetDTO.setLibraries(SerializationUtils.clone(defaultDTO.getLibraries()));
            getLog().info(String.format("%s|set libraries with %s", jobName, OBJECT_MAPPER.writeValueAsString(defaultDTO.getLibraries())));
        }

        if (targetDTO.getMaxConcurrentRuns() == null) {
            targetDTO.setMaxConcurrentRuns(defaultDTO.getMaxConcurrentRuns());
            getLog().info(String.format("%s|set max_concurrent_runs with %s", jobName, defaultDTO.getMaxConcurrentRuns()));
        }

        if (targetDTO.getMaxRetries() == null) {
            targetDTO.setMaxRetries(defaultDTO.getMaxRetries());
            getLog().info(String.format("%s|set max_retries with %s", jobName, defaultDTO.getMaxRetries()));
        }

        if (targetDTO.getMaxRetries() != 0 && targetDTO.getMinRetryIntervalMillis() == null) {
            targetDTO.setMinRetryIntervalMillis(defaultDTO.getMinRetryIntervalMillis());
            getLog().info(String.format("%s|set min_retry_interval_millis with %s", jobName, defaultDTO.getMinRetryIntervalMillis()));
        }


        //set InstanceTags
        if (targetDTO.getNewCluster() != null) {
            String groupId = jobTemplateModel.getGroupWithoutCompany();
            Map<String, String> tagMap = targetDTO.getNewCluster().getCustomTags();
            if (tagMap == null) {
                tagMap = new HashMap();
                targetDTO.getNewCluster().setCustomTags(tagMap);
            }

            if (!tagMap.containsKey(TEAM_TAG) || StringUtils.isEmpty(tagMap.get(TEAM_TAG))) {
                tagMap.put(TEAM_TAG, groupId);
                getLog().info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to [%s]", jobName, TEAM_TAG, tagMap.get(TEAM_TAG), groupId));
            }


            if (ValidationUtil.isDeltaEnabled(targetDTO.getNewCluster())
                    && !"true".equalsIgnoreCase(tagMap.get(DELTA_TAG))) {
                tagMap.put(DELTA_TAG, "true");
                getLog().info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to true", jobName, DELTA_TAG, tagMap.get(DELTA_TAG)));
            }
        }

    }

    /**
     * Check the value of targetDTO and fill targetDTO with defaultDTO if value do not exist.
     *
     * @param targetDTO
     * @param defaultDTO
     * @throws JsonProcessingException
     */
    public static void fillInDefaultClusterSettings(ClusterTemplateDTO targetDTO, ClusterTemplateDTO defaultDTO, ClusterTemplateModel clusterTemplateModel) throws JsonProcessingException {

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
