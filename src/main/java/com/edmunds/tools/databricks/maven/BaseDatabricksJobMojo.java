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
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import com.edmunds.tools.databricks.maven.validation.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import freemarker.cache.FileTemplateLoader;
import freemarker.cache.StringTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Base class for databricks job mojos.
 */
//TODO this class is doing too much.
public abstract class BaseDatabricksJobMojo extends BaseDatabricksMojo {

    public static final String DEFAULT_JOB_JSON = "default-job.json";
    public static final String TEAM_TAG = "team";
    public static final String DELTA_TAG = "delta";

    protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperUtils.getObjectMapper();

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

    public final static String MODEL_FILE_NAME = "job-template-model.json";

    /**
     * This file is used by the databricks-maven-plugin internally to inject information from maven.
     */
    @Parameter(property = "jobTemplateModelFile", defaultValue = "${project.build.directory}/databricks-plugin/" + MODEL_FILE_NAME)
    protected File jobTemplateModelFile;

    /**
     * If set to true, this project is being built locally.
     */
    //TODO this parameter should be removed
    @Parameter(property = "isLocalBuild", defaultValue = "true")
    protected boolean isLocalBuild = true;

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

    static JobSettingsDTO[] deserializeJobSettingsDTOs(String jobSettings) throws MojoExecutionException {
        try {
            return ObjectMapperUtils.deserialize(jobSettings, JobSettingsDTO[].class);
        } catch (IOException e) {
            throw new MojoExecutionException(String.format("Failed to unmarshal jobSettings to object:\n[%s]\nHere is an example, of what it should look like:\n[%s]\n",
                    jobSettings,
                    readDefaultJob()), e);
        }
    }

    String getJobSettingsFromTemplate(JobTemplateModel jobTemplateModel) throws MojoExecutionException {
        if (!dbJobFile.exists()) {
            getLog().info("No db job file exists");
            return null;
        }
        StringWriter stringWriter = new StringWriter();
        try {
            TemplateLoader templateLoader = new FileTemplateLoader(dbJobFile.getParentFile());
            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate(dbJobFile.getName());
            temp.process(jobTemplateModel, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process job file as template: [%s]\nFreemarker message:\n%s", dbJobFile.getAbsolutePath(), e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    JobTemplateModel getJobTemplateModel() throws MojoExecutionException {
        try {
            JobTemplateModel jobTemplateModel;
            if (jobTemplateModelFile.exists()) {
                String jobTemplateModelJson = FileUtils.readFileToString(jobTemplateModelFile);
                jobTemplateModel = ObjectMapperUtils.deserialize(jobTemplateModelJson, JobTemplateModel.class);
            } else {
                if (isLocalBuild) {
                    jobTemplateModel = new JobTemplateModel(project);
                } else {
                    throw new MojoExecutionException(String.format("[%s] file was not found in the build. Please ensure prepare-package was ran during build.", MODEL_FILE_NAME));
                }
            }

            // [BDD-3114] - we want the current environment, to honor what was passed into the build, and not what was serialized [SAE]
            jobTemplateModel.setEnvironment(environment);
            if (StringUtils.isBlank(databricksRepo)) {
                throw new MojoExecutionException("missing property: databricks.repo");
            }
            jobTemplateModel.getProjectProperties().setProperty("databricks.repo", databricksRepo);
            jobTemplateModel.getProjectProperties().setProperty("databricks.repo.key", databricksRepoKey);

            return jobTemplateModel;
        } catch (IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    String getJobSettingsFromTemplate(String templateText, JobTemplateModel jobTemplateModel) throws MojoExecutionException {
        StringWriter stringWriter = new StringWriter();
        try {
            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate("defaultTemplate", templateText);

            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate("defaultTemplate");
            temp.process(jobTemplateModel, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process job template: [%s]\nFreemarker message:\n%s", templateText, e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    Configuration getFreemarkerConfiguration(TemplateLoader templateLoader) throws IOException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setTemplateLoader(templateLoader);
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        return cfg;
    }


    JobSettingsDTO[] buildJobSettingsDTOsWithDefault() throws MojoExecutionException {
        JobTemplateModel jobTemplateModel = getJobTemplateModel();
        String jobSettings = getJobSettingsFromTemplate(jobTemplateModel);
        if (jobSettings == null) {
            return new JobSettingsDTO[]{};
        }

        JobSettingsDTO defaultJobSettingDTO = defaultJobSettingDTO();

        JobSettingsDTO[] jobSettingsDTOS = deserializeJobSettingsDTOs(jobSettings);
        for (JobSettingsDTO settingsDTO : jobSettingsDTOS) {
            try {
                fillInDefaultJobSettings(settingsDTO, defaultJobSettingDTO, jobTemplateModel);
            } catch (JsonProcessingException e) {
                throw new MojoExecutionException(String.format("Fail to fill empty-value with default"), e);
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

        ValidationUtil.validatePath(settingsDTO.getName(), jobTemplateModel.getGroupWithoutCompany(), jobTemplateModel.getArtifactId());
    }

    private static String readDefaultJob() {
        try {
            return IOUtils.toString(BaseDatabricksJobMojo.class.getResourceAsStream("/default-job.json"));
        } catch (Exception e) {
            return ExceptionUtils.getStackTrace(e);
        }
    }

    /**
     * FIXME - it is possible for the example to be invalid, and the job file being valid. This should be fixed.
     *
     * <p>
     * Default JobSettingsDTO is used to fill the value when user job has missing value.
     *
     * @return
     * @throws MojoExecutionException
     */
    public JobSettingsDTO defaultJobSettingDTO() throws MojoExecutionException {
        return deserializeJobSettingsDTOs(getJobSettingsFromTemplate(readDefaultJob(), getJobTemplateModel()))[0];
    }

    protected JobService getJobService() {
        return getDatabricksServiceFactory().getJobService();
    }

    /**
     * NOTE - only for unit testing!
     */
    void setFailOnDuplicateJobName(boolean failOnDuplicateJobName) {
        this.failOnDuplicateJobName = failOnDuplicateJobName;
    }

    /**
     * NOTE - only for unit testing!
     */
    void setDbJobFile(File dbJobFile) {
        this.dbJobFile = dbJobFile;
    }


    /**
     * Check the value of targetDTO and fill targetDTO with defaultDTO if value do not exist.
     *
     * @param targetDTO
     * @param defaultDTO
     * @param jobTemplateModel
     */
    public void fillInDefaultJobSettings(JobSettingsDTO targetDTO, JobSettingsDTO defaultDTO, JobTemplateModel jobTemplateModel) throws JsonProcessingException {

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

    void setJobTemplateModelFile(File jobTemplateModelFile) {
        this.jobTemplateModelFile = jobTemplateModelFile;
    }

}
