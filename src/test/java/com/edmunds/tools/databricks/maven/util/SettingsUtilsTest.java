package com.edmunds.tools.databricks.maven.util;

import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.tools.databricks.maven.DatabricksMavenPluginTestHarness;
import com.edmunds.tools.databricks.maven.UpsertJobMojoTest;
import com.edmunds.tools.databricks.maven.model.EnvironmentDTO;
import java.io.File;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.Test;

public class SettingsUtilsTest extends DatabricksMavenPluginTestHarness {

    @Test(expectedExceptions = MojoExecutionException.class,
        expectedExceptionsMessageRegExp = "Failed to process User Settings file.*" +
            "The following has evaluated to null or missing.*groupId.*in template \"bad-example-job.json\".*")
    public void testGetJobSettingsFromTemplate_missing_freemarker_variable() throws MojoExecutionException {
        new SettingsUtils<>(JobSettingsDTO[].class, "/default-job.json",
            new File(UpsertJobMojoTest.class.getClassLoader().getResource("bad-example-job.json").getFile()),
            EnvironmentDTO::new,
            new SettingsInitializer<JobSettingsDTO>() {
                @Override
                public void fillInDefaults(JobSettingsDTO settingsDTO, JobSettingsDTO defaultSettingsDTO,
                    EnvironmentDTO environmentDTO) {

                }

                @Override
                public void validate(JobSettingsDTO settingsDTO, EnvironmentDTO environmentDTO) {

                }
            }
        ).getUserSettingsJson();
    }

}