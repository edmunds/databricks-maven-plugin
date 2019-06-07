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
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
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
    private SettingsUtils<BaseDatabricksJobMojo, JobTemplateModel, JobSettingsDTO> settingsUtils;

    public SettingsUtils<BaseDatabricksJobMojo, JobTemplateModel, JobSettingsDTO> getSettingsUtils() {
        if (settingsUtils == null) {
            settingsUtils = new SettingsUtils<>(
                    BaseDatabricksJobMojo.class, JobSettingsDTO[].class, "/default-job.json",
                    createTemplateModelSupplier(), createSettingsInitializer());
        }
        return settingsUtils;
    }

    /**
     * The databricks job json file that contains all of the information for how to create one or more databricks jobs.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-job-settings.json", property = "dbJobFile")
    File dbJobFile;

    /**
     * If true, any command that involves working by databricks job name, will fail if more then one job exists
     * with that job name.
     */
    @Parameter(property = "failOnDuplicateJobName")
    boolean failOnDuplicateJobName = true;

    protected TemplateModelSupplier<JobTemplateModel> createTemplateModelSupplier() {
        return new TemplateModelSupplier<JobTemplateModel>() {
            @Override
            public JobTemplateModel get() throws MojoExecutionException {
                if (StringUtils.isBlank(databricksRepo)) {
                    throw new MojoExecutionException("databricksRepo property is missing");
                }
                return new JobTemplateModel(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
            }

            @Override
            public File getSettingsFile() {
                return dbJobFile;
            }
        };
    }

    private SettingsInitializer<JobTemplateModel, JobSettingsDTO> createSettingsInitializer() {
        return new SettingsInitializer<JobTemplateModel, JobSettingsDTO>() {
            @Override
            public void fillInDefaults(JobSettingsDTO settings, JobSettingsDTO defaultSettings,
                                       JobTemplateModel templateModel) throws JsonProcessingException {
                String jobName = settings.getName();
                if (StringUtils.isEmpty(settings.getName())) {
                    jobName = templateModel.getGroupWithoutCompany() + "/" + templateModel.getArtifactId();
                    settings.setName(jobName);
                    getLog().info(String.format("set JobName with %s", jobName));
                }

                // email_notifications
                if (settings.getEmailNotifications() == null) {
                    settings.setEmailNotifications(SerializationUtils.clone(defaultSettings.getEmailNotifications()));
                    getLog().info(String.format("%s|set email_notifications with %s", jobName,
                            OBJECT_MAPPER.writeValueAsString(defaultSettings.getEmailNotifications())));

                } else if (settings.getEmailNotifications().getOnFailure() == null
                        || settings.getEmailNotifications().getOnFailure().length == 0
                        || StringUtils.isEmpty(settings.getEmailNotifications().getOnFailure()[0])) {
                    settings.getEmailNotifications().setOnFailure(defaultSettings.getEmailNotifications().getOnFailure());
                    getLog().info(String.format("%s|set email_notifications.on_failure with %s", jobName,
                            OBJECT_MAPPER.writeValueAsString(defaultSettings.getEmailNotifications().getOnFailure())));
                }

                // ClusterInfo
                if (StringUtils.isEmpty(settings.getExistingClusterId())) {
                    if (settings.getNewCluster() == null) {
                        settings.setNewCluster(SerializationUtils.clone(defaultSettings.getNewCluster()));
                        getLog().info(String.format("%s|set new_cluster with %s", jobName,
                                OBJECT_MAPPER.writeValueAsString(defaultSettings.getNewCluster())));

                    } else {
                        if (StringUtils.isEmpty(settings.getNewCluster().getSparkVersion())) {
                            settings.getNewCluster().setSparkVersion(defaultSettings.getNewCluster().getSparkVersion());
                            getLog().info(String.format("%s|set new_cluster.spark_version with %s", jobName,
                                    defaultSettings.getNewCluster().getSparkVersion()));
                        }

                        if (StringUtils.isEmpty(settings.getNewCluster().getNodeTypeId())) {
                            settings.getNewCluster().setNodeTypeId(defaultSettings.getNewCluster().getNodeTypeId());
                            getLog().info(String.format("%s|set new_cluster.node_type_id with %s", jobName,
                                    defaultSettings.getNewCluster().getNodeTypeId()));
                        }

                        if (settings.getNewCluster().getAutoScale() == null && settings.getNewCluster().getNumWorkers() < 1) {
                            settings.getNewCluster().setNumWorkers(defaultSettings.getNewCluster().getNumWorkers());
                            getLog().info(String.format("%s|set new_cluster.num_workers with %s", jobName,
                                    defaultSettings.getNewCluster().getNumWorkers()));
                        }

                        //aws_attributes
                        if (settings.getNewCluster().getAwsAttributes() == null) {
                            settings.getNewCluster().setAwsAttributes(SerializationUtils.clone(defaultSettings.getNewCluster().getAwsAttributes()));
                            getLog().info(String.format("%s|set new_cluster.aws_attributes with %s", jobName,
                                    OBJECT_MAPPER.writeValueAsString(defaultSettings.getNewCluster().getAwsAttributes())));
                        }
                    }
                }

                if (settings.getTimeoutSeconds() == null) {
                    settings.setTimeoutSeconds(defaultSettings.getTimeoutSeconds());
                    getLog().info(String.format("%s|set timeout_seconds with %s", jobName, defaultSettings.getTimeoutSeconds()));
                }

                // Can't have libraries if its a spark submit task
                if ((settings.getLibraries() == null || settings.getLibraries().length == 0) && settings.getSparkSubmitTask() == null) {
                    settings.setLibraries(SerializationUtils.clone(defaultSettings.getLibraries()));
                    getLog().info(String.format("%s|set libraries with %s", jobName, OBJECT_MAPPER.writeValueAsString(defaultSettings.getLibraries())));
                }

                if (settings.getMaxConcurrentRuns() == null) {
                    settings.setMaxConcurrentRuns(defaultSettings.getMaxConcurrentRuns());
                    getLog().info(String.format("%s|set max_concurrent_runs with %s", jobName, defaultSettings.getMaxConcurrentRuns()));
                }

                if (settings.getMaxRetries() == null) {
                    settings.setMaxRetries(defaultSettings.getMaxRetries());
                    getLog().info(String.format("%s|set max_retries with %s", jobName, defaultSettings.getMaxRetries()));
                }

                if (settings.getMaxRetries() != 0 && settings.getMinRetryIntervalMillis() == null) {
                    settings.setMinRetryIntervalMillis(defaultSettings.getMinRetryIntervalMillis());
                    getLog().info(String.format("%s|set min_retry_interval_millis with %s", jobName, defaultSettings.getMinRetryIntervalMillis()));
                }

                //set InstanceTags
                if (settings.getNewCluster() != null) {
                    String groupId = templateModel.getGroupWithoutCompany();
                    Map<String, String> tagMap = settings.getNewCluster().getCustomTags();
                    if (tagMap == null) {
                        tagMap = new HashMap<>();
                        settings.getNewCluster().setCustomTags(tagMap);
                    }

                    if (!tagMap.containsKey(TEAM_TAG) || StringUtils.isEmpty(tagMap.get(TEAM_TAG))) {
                        tagMap.put(TEAM_TAG, groupId);
                        getLog().info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to [%s]", jobName, TEAM_TAG, tagMap.get(TEAM_TAG), groupId));
                    }


                    if (ValidationUtil.isDeltaEnabled(settings.getNewCluster())
                            && !"true".equalsIgnoreCase(tagMap.get(DELTA_TAG))) {
                        tagMap.put(DELTA_TAG, "true");
                        getLog().info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to true", jobName, DELTA_TAG, tagMap.get(DELTA_TAG)));
                    }
                }
            }

            @Override
            public void validate(JobSettingsDTO settingsDTO, JobTemplateModel jobTemplateModel) throws MojoExecutionException {
                if (validate) {
                    JobEmailNotificationsDTO emailNotifications = settingsDTO.getEmailNotifications();
                    if (emailNotifications == null || ArrayUtils.isEmpty(emailNotifications.getOnFailure())) {
                        throw new MojoExecutionException("REQUIRED FIELD [email_notifications.on_failure] was empty. VALIDATION FAILED.");
                    }
                    ValidationUtil.validatePath(settingsDTO.getName(), jobTemplateModel.getGroupWithoutCompany(), jobTemplateModel.getArtifactId(), prefixToStrip);
                }
            }
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

    JobService getJobService() {
        return getDatabricksServiceFactory().getJobService();
    }

}
