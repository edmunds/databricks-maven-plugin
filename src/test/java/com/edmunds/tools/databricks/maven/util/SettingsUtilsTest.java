package com.edmunds.tools.databricks.maven.util;

import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.tools.databricks.maven.BaseDatabricksJobMojo;
import com.edmunds.tools.databricks.maven.DatabricksMavenPluginTestHarness;
import com.edmunds.tools.databricks.maven.UpsertJobMojoTest;
import com.edmunds.tools.databricks.maven.model.BaseModel;
import com.edmunds.tools.databricks.maven.model.JobTemplateModel;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.Test;

import java.io.File;

public class SettingsUtilsTest extends DatabricksMavenPluginTestHarness {

    @Test(expectedExceptions = MojoExecutionException.class,
            expectedExceptionsMessageRegExp = "Failed to process settings template file.*" +
                    "The following has evaluated to null or missing.*groupId.*in template \"bad-example-job.json\".*")
    public void testGetJobSettingsFromTemplate_missing_freemarker_variable() throws MojoExecutionException {
        new SettingsUtils<>(
                BaseDatabricksJobMojo.class, JobSettingsDTO[].class, "/default-job.json",
                () -> null,
                new SettingsInitializer<BaseModel, JobSettingsDTO>() {
                    @Override
                    public File getSettingsFile() {
                        return new File(UpsertJobMojoTest.class.getClassLoader()
                                .getResource("bad-example-job.json").getFile());
                    }

                    @Override
                    public void fillInDefaults(JobSettingsDTO settings, JobSettingsDTO defaultSettings, BaseModel templateModel) {

                    }

                    @Override
                    public void validate(JobSettingsDTO settings, BaseModel templateModel) {

                    }
                }
        ).getSettingsFromTemplate(new JobTemplateModel());
    }

}