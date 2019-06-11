package com.edmunds.tools.databricks.maven.util;

import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.tools.databricks.maven.DatabricksMavenPluginTestHarness;
import com.edmunds.tools.databricks.maven.UpsertJobMojoTest;
import com.edmunds.tools.databricks.maven.model.JobEnvironmentDTO;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.Test;

import java.io.File;

public class SettingsUtilsTest extends DatabricksMavenPluginTestHarness {

    @Test(expectedExceptions = MojoExecutionException.class,
            expectedExceptionsMessageRegExp = "Failed to process User Settings file.*" +
                    "The following has evaluated to null or missing.*groupId.*in template \"bad-example-job.json\".*")
    public void testGetJobSettingsFromTemplate_missing_freemarker_variable() throws MojoExecutionException {
        new SettingsUtils<>(JobSettingsDTO[].class, "/default-job.json",
                new File(UpsertJobMojoTest.class.getClassLoader().getResource("bad-example-job.json").getFile()), JobEnvironmentDTO::new,
                new SettingsInitializer<JobEnvironmentDTO, JobSettingsDTO>() {
                    @Override
                    public void fillInDefaults(JobSettingsDTO settingsDTO, JobSettingsDTO defaultSettingsDTO, JobEnvironmentDTO environmentDTO) {

                    }

                    @Override
                    public void validate(JobSettingsDTO settingsDTO, JobEnvironmentDTO environmentDTO) {

                    }
                }
        ).getUserSettingsJson();
    }

}