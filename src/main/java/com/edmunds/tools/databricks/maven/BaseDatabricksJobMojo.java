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
import com.edmunds.tools.databricks.maven.model.JobEnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
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
    private SettingsUtils<BaseDatabricksJobMojo, JobEnvironmentDTO, JobSettingsDTO> settingsUtils;

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

    public SettingsUtils<BaseDatabricksJobMojo, JobEnvironmentDTO, JobSettingsDTO> getSettingsUtils() {
        if (settingsUtils == null) {
            settingsUtils = new SettingsUtils<>(
                    BaseDatabricksJobMojo.class, JobSettingsDTO[].class, "/default-job.json",
                    createEnvironmentDTOSupplier(), createSettingsInitializer());
        }
        return settingsUtils;
    }

    protected EnvironmentDTOSupplier<JobEnvironmentDTO> createEnvironmentDTOSupplier() {
        return new EnvironmentDTOSupplier<JobEnvironmentDTO>() {
            @Override
            public JobEnvironmentDTO get() throws MojoExecutionException {
                if (StringUtils.isBlank(databricksRepo)) {
                    throw new MojoExecutionException("databricksRepo property is missing");
                }
                return new JobEnvironmentDTO(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
            }

            @Override
            public File getEnvironmentDTOFile() {
                return dbJobFile;
            }
        };
    }

    JobService getJobService() {
        return getDatabricksServiceFactory().getJobService();
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

    private SettingsInitializer<JobEnvironmentDTO, JobSettingsDTO> createSettingsInitializer() {
        return new SettingsInitializer<JobEnvironmentDTO, JobSettingsDTO>() {
            @Override
            public void fillInDefaults(JobSettingsDTO settingsDTO, JobSettingsDTO defaultSettingsDTO,
                                       JobEnvironmentDTO environmentDTO) throws JsonProcessingException {
                String jobName = settingsDTO.getName();
                if (StringUtils.isEmpty(settingsDTO.getName())) {
                    jobName = environmentDTO.getGroupWithoutCompany() + "/" + environmentDTO.getArtifactId();
                    settingsDTO.setName(jobName);
                    getLog().info(String.format("set JobName with %s", jobName));
                }

                // email_notifications
                if (settingsDTO.getEmailNotifications() == null) {
                    settingsDTO.setEmailNotifications(SerializationUtils.clone(defaultSettingsDTO.getEmailNotifications()));
                    getLog().info(String.format("%s|set email_notifications with %s", jobName,
                            OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getEmailNotifications())));

                } else if (settingsDTO.getEmailNotifications().getOnFailure() == null
                        || settingsDTO.getEmailNotifications().getOnFailure().length == 0
                        || StringUtils.isEmpty(settingsDTO.getEmailNotifications().getOnFailure()[0])) {
                    settingsDTO.getEmailNotifications().setOnFailure(defaultSettingsDTO.getEmailNotifications().getOnFailure());
                    getLog().info(String.format("%s|set email_notifications.on_failure with %s", jobName,
                            OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getEmailNotifications().getOnFailure())));
                }

                // ClusterInfo
                if (StringUtils.isEmpty(settingsDTO.getExistingClusterId())) {
                    if (settingsDTO.getNewCluster() == null) {
                        settingsDTO.setNewCluster(SerializationUtils.clone(defaultSettingsDTO.getNewCluster()));
                        getLog().info(String.format("%s|set new_cluster with %s", jobName,
                                OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getNewCluster())));

                    } else {
                        if (StringUtils.isEmpty(settingsDTO.getNewCluster().getSparkVersion())) {
                            settingsDTO.getNewCluster().setSparkVersion(defaultSettingsDTO.getNewCluster().getSparkVersion());
                            getLog().info(String.format("%s|set new_cluster.spark_version with %s", jobName,
                                    defaultSettingsDTO.getNewCluster().getSparkVersion()));
                        }

                        if (StringUtils.isEmpty(settingsDTO.getNewCluster().getNodeTypeId())) {
                            settingsDTO.getNewCluster().setNodeTypeId(defaultSettingsDTO.getNewCluster().getNodeTypeId());
                            getLog().info(String.format("%s|set new_cluster.node_type_id with %s", jobName,
                                    defaultSettingsDTO.getNewCluster().getNodeTypeId()));
                        }

                        if (settingsDTO.getNewCluster().getAutoScale() == null && settingsDTO.getNewCluster().getNumWorkers() < 1) {
                            settingsDTO.getNewCluster().setNumWorkers(defaultSettingsDTO.getNewCluster().getNumWorkers());
                            getLog().info(String.format("%s|set new_cluster.num_workers with %s", jobName,
                                    defaultSettingsDTO.getNewCluster().getNumWorkers()));
                        }

                        //aws_attributes
                        if (settingsDTO.getNewCluster().getAwsAttributes() == null) {
                            settingsDTO.getNewCluster().setAwsAttributes(SerializationUtils.clone(defaultSettingsDTO.getNewCluster().getAwsAttributes()));
                            getLog().info(String.format("%s|set new_cluster.aws_attributes with %s", jobName,
                                    OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getNewCluster().getAwsAttributes())));
                        }
                    }
                }

                if (settingsDTO.getTimeoutSeconds() == null) {
                    settingsDTO.setTimeoutSeconds(defaultSettingsDTO.getTimeoutSeconds());
                    getLog().info(String.format("%s|set timeout_seconds with %s", jobName, defaultSettingsDTO.getTimeoutSeconds()));
                }

                // Can't have libraries if its a spark submit task
                if ((settingsDTO.getLibraries() == null || settingsDTO.getLibraries().length == 0) && settingsDTO.getSparkSubmitTask() == null) {
                    settingsDTO.setLibraries(SerializationUtils.clone(defaultSettingsDTO.getLibraries()));
                    getLog().info(String.format("%s|set libraries with %s", jobName, OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getLibraries())));
                }

                if (settingsDTO.getMaxConcurrentRuns() == null) {
                    settingsDTO.setMaxConcurrentRuns(defaultSettingsDTO.getMaxConcurrentRuns());
                    getLog().info(String.format("%s|set max_concurrent_runs with %s", jobName, defaultSettingsDTO.getMaxConcurrentRuns()));
                }

                if (settingsDTO.getMaxRetries() == null) {
                    settingsDTO.setMaxRetries(defaultSettingsDTO.getMaxRetries());
                    getLog().info(String.format("%s|set max_retries with %s", jobName, defaultSettingsDTO.getMaxRetries()));
                }

                if (settingsDTO.getMaxRetries() != 0 && settingsDTO.getMinRetryIntervalMillis() == null) {
                    settingsDTO.setMinRetryIntervalMillis(defaultSettingsDTO.getMinRetryIntervalMillis());
                    getLog().info(String.format("%s|set min_retry_interval_millis with %s", jobName, defaultSettingsDTO.getMinRetryIntervalMillis()));
                }

                //set InstanceTags
                if (settingsDTO.getNewCluster() != null) {
                    String groupId = environmentDTO.getGroupWithoutCompany();
                    Map<String, String> tagMap = settingsDTO.getNewCluster().getCustomTags();
                    if (tagMap == null) {
                        tagMap = new HashMap<>();
                        settingsDTO.getNewCluster().setCustomTags(tagMap);
                    }

                    if (!tagMap.containsKey(TEAM_TAG) || StringUtils.isEmpty(tagMap.get(TEAM_TAG))) {
                        tagMap.put(TEAM_TAG, groupId);
                        getLog().info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to [%s]", jobName, TEAM_TAG, tagMap.get(TEAM_TAG), groupId));
                    }


                    if (ValidationUtil.isDeltaEnabled(settingsDTO.getNewCluster())
                            && !"true".equalsIgnoreCase(tagMap.get(DELTA_TAG))) {
                        tagMap.put(DELTA_TAG, "true");
                        getLog().info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to true", jobName, DELTA_TAG, tagMap.get(DELTA_TAG)));
                    }
                }
            }

            @Override
            public void validate(JobSettingsDTO settingsDTO, JobEnvironmentDTO environmentDTO) throws MojoExecutionException {
                if (validate) {
                    JobEmailNotificationsDTO emailNotifications = settingsDTO.getEmailNotifications();
                    if (emailNotifications == null || ArrayUtils.isEmpty(emailNotifications.getOnFailure())) {
                        throw new MojoExecutionException("REQUIRED FIELD [email_notifications.on_failure] was empty. VALIDATION FAILED.");
                    }
                    ValidationUtil.validatePath(settingsDTO.getName(), environmentDTO.getGroupWithoutCompany(), environmentDTO.getArtifactId(), prefixToStrip);
                }
            }
        };
    }

}
