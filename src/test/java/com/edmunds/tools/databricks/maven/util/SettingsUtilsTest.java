package com.edmunds.tools.databricks.maven.util;

import com.edmunds.tools.databricks.maven.DatabricksMavenPluginTestHarness;
import com.edmunds.tools.databricks.maven.UpsertJobMojoTest;
import com.edmunds.tools.databricks.maven.model.JobTemplateModel;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.Test;

import java.io.File;

public class SettingsUtilsTest extends DatabricksMavenPluginTestHarness {

    @Test(expectedExceptions = MojoExecutionException.class,
            expectedExceptionsMessageRegExp = "Failed to process jobSettings file as template.*" +
                    "The following has evaluated to null or missing.*groupId.*in template \"bad-example-job.json\".*")
    public void testGetJobSettingsFromTemplate_missing_freemarker_variable() throws MojoExecutionException {
        new SettingsUtils<>().getSettingsFromTemplate("jobSettings", new File(UpsertJobMojoTest.class
                .getClassLoader().getResource("bad-example-job.json").getFile()), new JobTemplateModel());
    }

}