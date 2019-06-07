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

import com.edmunds.rest.databricks.DTO.JobDTO;
import com.edmunds.rest.databricks.DTO.JobEmailNotificationsDTO;
import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.service.JobService;
import com.edmunds.tools.databricks.maven.model.JobTemplateModel;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import com.edmunds.tools.databricks.maven.util.TemplateModelSupplier;
import com.edmunds.tools.databricks.maven.validation.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.edmunds.tools.databricks.maven.util.SettingsUtils.OBJECT_MAPPER;

/**
 * Base class for databricks job mojos.
 */
public abstract class BaseDatabricksJobMojo extends BaseDatabricksMojo {

    static final String TEAM_TAG = "team";
    static final String DELTA_TAG = "delta";
    protected final SettingsUtils<BaseDatabricksJobMojo, JobTemplateModel, JobSettingsDTO> settingsUtils =
            new SettingsUtils<>(
                    BaseDatabricksJobMojo.class, JobSettingsDTO[].class, "/default-job.json",
                    createSupplier());

    /**
     * The databricks job json file that contains all of the information for how to create one or more databricks jobs.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-job-settings.json", property = "dbJobFile")
    protected File dbJobFile;

    /**
     * If true, any command that involves working by databricks job name, will fail if more then one job exists
     * with that job name.
     */
    @Parameter(property = "failOnDuplicateJobName")
    boolean failOnDuplicateJobName = true;

    protected TemplateModelSupplier<JobTemplateModel> createSupplier() {
        return () -> {
            if (StringUtils.isBlank(databricksRepo)) {
                throw new MojoExecutionException("databricksRepo property is missing");
            }
            return new JobTemplateModel(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
        };
    }

    Long getJobId(String jobName) throws MojoExecutionException {
        try {
            JobDTO jobDTO = getJobService().getJobByName(jobName, failOnDuplicateJobName);
            if (jobDTO == null) {
                return null;
            }
            return jobDTO.getJobId();
        } catch (DatabricksRestException | IOException | IllegalStateException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    JobSettingsDTO[] buildJobSettingsDTOsWithDefault() throws MojoExecutionException {
        JobTemplateModel jobTemplateModel = settingsUtils.getTemplateModel();
        String jobSettings = settingsUtils.getSettingsFromTemplate("jobSettings", dbJobFile, jobTemplateModel);
        if (jobSettings == null) {
            return new JobSettingsDTO[]{};
        }

        JobSettingsDTO defaultJobSettingDTO = settingsUtils.defaultTemplateDTO();

        JobSettingsDTO[] jobSettingsDTOS = settingsUtils.deserializeSettings(
                jobSettings, settingsUtils.readDefaultSettings());
        for (JobSettingsDTO settingsDTO : jobSettingsDTOS) {
            try {
                fillInDefaultJobSettings(settingsDTO, defaultJobSettingDTO, jobTemplateModel);
            } catch (JsonProcessingException e) {
                throw new MojoExecutionException("Fail to fill empty-value with default", e);
            }

            // Validate all job settings. If any fail terminate.
            if (validate) {
                validateJobSettings(settingsDTO, jobTemplateModel);
            }
        }

        return jobSettingsDTOS;
    }

    private void validateJobSettings(JobSettingsDTO settingsDTO, JobTemplateModel jobTemplateModel) throws MojoExecutionException {
        JobEmailNotificationsDTO emailNotifications = settingsDTO.getEmailNotifications();
        if (emailNotifications == null || ArrayUtils.isEmpty(emailNotifications.getOnFailure())) {
            throw new MojoExecutionException("REQUIRED FIELD [email_notifications.on_failure] was empty. VALIDATION FAILED.");
        }

        ValidationUtil.validatePath(settingsDTO.getName(), jobTemplateModel.getGroupWithoutCompany(), jobTemplateModel.getArtifactId(), prefixToStrip);
    }

    JobService getJobService() {
        return getDatabricksServiceFactory().getJobService();
    }

    /**
     * Check the value of targetDTO and fill targetDTO with defaultDTO if value do not exist.
     *
     * @param targetDTO
     * @param defaultDTO
     * @param jobTemplateModel
     */
    void fillInDefaultJobSettings(JobSettingsDTO targetDTO, JobSettingsDTO defaultDTO, JobTemplateModel jobTemplateModel) throws JsonProcessingException {

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

}
