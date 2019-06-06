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
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import com.edmunds.tools.databricks.maven.validation.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import freemarker.cache.StringTemplateLoader;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;

/**
 * Base class for databricks job mojos.
 */
//TODO this class is doing too much.
public abstract class BaseDatabricksJobMojo extends BaseDatabricksMojo {

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

    protected JobTemplateModel getJobTemplateModel() throws MojoExecutionException {
        if (StringUtils.isBlank(databricksRepo)) {
            throw new MojoExecutionException("databricksRepo property is missing");
        }
        return new JobTemplateModel(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
    }

    String getJobSettingsFromTemplate(String templateText, JobTemplateModel jobTemplateModel) throws MojoExecutionException {
        StringWriter stringWriter = new StringWriter();
        try {
            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate("defaultTemplate", templateText);

            Template temp = SettingsUtils.getFreemarkerConfiguration(templateLoader).getTemplate("defaultTemplate");
            temp.process(jobTemplateModel, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process job template: [%s]\nFreemarker message:\n%s", templateText, e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    JobSettingsDTO[] buildJobSettingsDTOsWithDefault() throws MojoExecutionException {
        JobTemplateModel jobTemplateModel = getJobTemplateModel();
        String jobSettings = SettingsUtils.getJobSettingsFromTemplate("jobSettings", dbJobFile, jobTemplateModel);
        if (jobSettings == null) {
            return new JobSettingsDTO[]{};
        }

        JobSettingsDTO defaultJobSettingDTO = defaultJobSettingDTO();

        JobSettingsDTO[] jobSettingsDTOS = deserializeJobSettingsDTOs(jobSettings, readDefaultJob());
        for (JobSettingsDTO settingsDTO : jobSettingsDTOS) {
            try {
                SettingsUtils.fillInDefaultJobSettings(settingsDTO, defaultJobSettingDTO, jobTemplateModel);
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

    private static JobSettingsDTO[] deserializeJobSettingsDTOs(String settingsJson, String defaultSettingsJson) throws MojoExecutionException {
        try {
            return ObjectMapperUtils.deserialize(settingsJson, JobSettingsDTO[].class);
        } catch (IOException e) {
            throw new MojoExecutionException(String.format("Failed to unmarshal job settings to object:\n[%s]\nHere is an example, of what it should look like:\n[%s]\n",
                    settingsJson,
                    defaultSettingsJson), e);
        }
    }

    private void validateJobSettings(JobSettingsDTO settingsDTO, JobTemplateModel jobTemplateModel) throws MojoExecutionException {
        JobEmailNotificationsDTO emailNotifications = settingsDTO.getEmailNotifications();
        if (emailNotifications == null || ArrayUtils.isEmpty(emailNotifications.getOnFailure())) {
            throw new MojoExecutionException("REQUIRED FIELD [email_notifications.on_failure] was empty. VALIDATION FAILED.");
        }

        ValidationUtil.validatePath(settingsDTO.getName(), jobTemplateModel.getGroupWithoutCompany(), jobTemplateModel.getArtifactId(), prefixToStrip);
    }

    private static String readDefaultJob() {
        try {
            return IOUtils.toString(BaseDatabricksJobMojo.class.getResourceAsStream("/default-job.json"), Charset.defaultCharset());
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
        return deserializeJobSettingsDTOs(getJobSettingsFromTemplate(readDefaultJob(), getJobTemplateModel()), readDefaultJob())[0];
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

}
