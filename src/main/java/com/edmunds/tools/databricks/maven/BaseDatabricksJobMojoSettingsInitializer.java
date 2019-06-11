package com.edmunds.tools.databricks.maven;

import com.edmunds.rest.databricks.DTO.JobEmailNotificationsDTO;
import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.tools.databricks.maven.model.JobEnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.validation.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.util.HashMap;
import java.util.Map;

import static com.edmunds.tools.databricks.maven.util.ObjectMapperUtils.OBJECT_MAPPER;

/**
 * Class contains logic of {@link BaseDatabricksJobMojo} Settings DTO fields initialization.
 */
public class BaseDatabricksJobMojoSettingsInitializer implements SettingsInitializer<JobEnvironmentDTO, JobSettingsDTO> {

    static final String TEAM_TAG = "team";
    static final String DELTA_TAG = "delta";
    private static final Log log = new SystemStreamLog();

    private final boolean validate;
    private final String prefixToStrip;

    BaseDatabricksJobMojoSettingsInitializer(boolean validate, String prefixToStrip) {
        this.validate = validate;
        this.prefixToStrip = prefixToStrip;
    }

    @Override
    public void fillInDefaults(JobSettingsDTO settingsDTO, JobSettingsDTO defaultSettingsDTO,
                               JobEnvironmentDTO environmentDTO) throws JsonProcessingException {
        String jobName = settingsDTO.getName();
        if (StringUtils.isEmpty(settingsDTO.getName())) {
            jobName = environmentDTO.getGroupWithoutCompany() + "/" + environmentDTO.getArtifactId();
            settingsDTO.setName(jobName);
            log.info(String.format("set JobName with %s", jobName));
        }

        // email_notifications
        if (settingsDTO.getEmailNotifications() == null) {
            settingsDTO.setEmailNotifications(SerializationUtils.clone(defaultSettingsDTO.getEmailNotifications()));
            log.info(String.format("%s|set email_notifications with %s", jobName,
                    OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getEmailNotifications())));

        } else if (settingsDTO.getEmailNotifications().getOnFailure() == null
                || settingsDTO.getEmailNotifications().getOnFailure().length == 0
                || StringUtils.isEmpty(settingsDTO.getEmailNotifications().getOnFailure()[0])) {
            settingsDTO.getEmailNotifications().setOnFailure(defaultSettingsDTO.getEmailNotifications().getOnFailure());
            log.info(String.format("%s|set email_notifications.on_failure with %s", jobName,
                    OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getEmailNotifications().getOnFailure())));
        }

        // ClusterInfo
        if (StringUtils.isEmpty(settingsDTO.getExistingClusterId())) {
            if (settingsDTO.getNewCluster() == null) {
                settingsDTO.setNewCluster(SerializationUtils.clone(defaultSettingsDTO.getNewCluster()));
                log.info(String.format("%s|set new_cluster with %s", jobName,
                        OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getNewCluster())));

            } else {
                if (StringUtils.isEmpty(settingsDTO.getNewCluster().getSparkVersion())) {
                    settingsDTO.getNewCluster().setSparkVersion(defaultSettingsDTO.getNewCluster().getSparkVersion());
                    log.info(String.format("%s|set new_cluster.spark_version with %s", jobName,
                            defaultSettingsDTO.getNewCluster().getSparkVersion()));
                }

                if (StringUtils.isEmpty(settingsDTO.getNewCluster().getNodeTypeId())) {
                    settingsDTO.getNewCluster().setNodeTypeId(defaultSettingsDTO.getNewCluster().getNodeTypeId());
                    log.info(String.format("%s|set new_cluster.node_type_id with %s", jobName,
                            defaultSettingsDTO.getNewCluster().getNodeTypeId()));
                }

                if (settingsDTO.getNewCluster().getAutoScale() == null && settingsDTO.getNewCluster().getNumWorkers() < 1) {
                    settingsDTO.getNewCluster().setNumWorkers(defaultSettingsDTO.getNewCluster().getNumWorkers());
                    log.info(String.format("%s|set new_cluster.num_workers with %s", jobName,
                            defaultSettingsDTO.getNewCluster().getNumWorkers()));
                }

                //aws_attributes
                if (settingsDTO.getNewCluster().getAwsAttributes() == null) {
                    settingsDTO.getNewCluster().setAwsAttributes(SerializationUtils.clone(defaultSettingsDTO.getNewCluster().getAwsAttributes()));
                    log.info(String.format("%s|set new_cluster.aws_attributes with %s", jobName,
                            OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getNewCluster().getAwsAttributes())));
                }
            }
        }

        if (settingsDTO.getTimeoutSeconds() == null) {
            settingsDTO.setTimeoutSeconds(defaultSettingsDTO.getTimeoutSeconds());
            log.info(String.format("%s|set timeout_seconds with %s", jobName, defaultSettingsDTO.getTimeoutSeconds()));
        }

        // Can't have libraries if its a spark submit task
        if ((settingsDTO.getLibraries() == null || settingsDTO.getLibraries().length == 0) && settingsDTO.getSparkSubmitTask() == null) {
            settingsDTO.setLibraries(SerializationUtils.clone(defaultSettingsDTO.getLibraries()));
            log.info(String.format("%s|set libraries with %s", jobName, OBJECT_MAPPER.writeValueAsString(defaultSettingsDTO.getLibraries())));
        }

        if (settingsDTO.getMaxConcurrentRuns() == null) {
            settingsDTO.setMaxConcurrentRuns(defaultSettingsDTO.getMaxConcurrentRuns());
            log.info(String.format("%s|set max_concurrent_runs with %s", jobName, defaultSettingsDTO.getMaxConcurrentRuns()));
        }

        if (settingsDTO.getMaxRetries() == null) {
            settingsDTO.setMaxRetries(defaultSettingsDTO.getMaxRetries());
            log.info(String.format("%s|set max_retries with %s", jobName, defaultSettingsDTO.getMaxRetries()));
        }

        if (settingsDTO.getMaxRetries() != 0 && settingsDTO.getMinRetryIntervalMillis() == null) {
            settingsDTO.setMinRetryIntervalMillis(defaultSettingsDTO.getMinRetryIntervalMillis());
            log.info(String.format("%s|set min_retry_interval_millis with %s", jobName, defaultSettingsDTO.getMinRetryIntervalMillis()));
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
                log.info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to [%s]", jobName, TEAM_TAG, tagMap.get(TEAM_TAG), groupId));
            }


            if (ValidationUtil.isDeltaEnabled(settingsDTO.getNewCluster())
                    && !"true".equalsIgnoreCase(tagMap.get(DELTA_TAG))) {
                tagMap.put(DELTA_TAG, "true");
                log.info(String.format("%s|set new_cluster.custom_tags.%s from [%s] to true", jobName, DELTA_TAG, tagMap.get(DELTA_TAG)));
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
}
