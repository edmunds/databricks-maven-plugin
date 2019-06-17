/*
 *    Copyright 2018 Edmunds.com, Inc.
 *
 *        Licensed under the Apache License, Version 2.0 (the "License");
 *        you may not use this file except in compliance with the License.
 *        You may obtain a copy of the License at
 *
 *            http://www.apache.org/licenses/LICENSE-2.0
 *
 *        Unless required by applicable law or agreed to in writing, software
 *        distributed under the License is distributed on an "AS IS" BASIS,
 *        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *        See the License for the specific language governing permissions and
 *        limitations under the License.
 */

package com.edmunds.tools.databricks.maven;

import com.edmunds.rest.databricks.DTO.JobDTO;
import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.verify;

/**
 * Tests for @{@link UpsertJobMojo}.
 * <p>
 * For these tests, the regex as part of the expected exceptions no longer works.
 */
public class UpsertJobMojoNoProjectTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "upsert-job-np";

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void execute_NoJobFile_NothingHappens() throws Exception {
        UpsertJobMojoNoProject underTest = getNoOverridesMojo(GOAL);
        Mockito.when(jobService.getJobByName("unit-test-group/unit-test-artifact", true)).thenReturn(createJobDTO
                ("unit-test-group/unit-test-artifact", 1));
        underTest.execute();

        List<JobSettingsDTO> jobSettingsDTOS = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();
        assert (jobSettingsDTOS.size() == 0);
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = "" +
            ".*jobEnvironmentDTOFile must be set.*")
    public void execute_whenMissingProperties_fail() throws Exception {
        UpsertJobMojoNoProject underTest = getMissingMandatoryMojo(GOAL);
        underTest.execute();
    }

    /**
     * This is testing the case where a job file is going to be upserted outside of the context of a project.
     * A use case for this is when a company does not want to use CD and instead wants to be able to deploy specific
     * artifacts deterministically after the fact. This requires that the artifact contains everything that is needed
     * to do that. In this case, given a serialized json with project information, a job template and an environment can
     * we upsert the job?
     *
     * @throws Exception
     */
    @Test
    public void execute_whenJobFileAndTemplateExists_upsertsJob() throws Exception {
        UpsertJobMojoNoProject underTest = getOverridesMojo(GOAL, "2");
        Mockito.when(jobService.getJobByName("dwh/inventory-databricks", true)).thenReturn(createJobDTO
                ("dwh/inventory-databricks", 1));

        underTest.execute();

        List<JobSettingsDTO> jobSettingsDTOS = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();
        assertThat(jobSettingsDTOS.size(), is(1));
        assertThat(jobSettingsDTOS.get(0).getEmailNotifications().getOnFailure(), is(new String[]{"QA.com"}));
        assertThat(jobSettingsDTOS.get(0).getName(), is("dwh/inventory-databricks"));
        assertThat(jobSettingsDTOS.get(0).getLibraries()[0].getJar(), is
                ("s3://edmunds-repos/artifacts/com.edmunds" +
                        ".dwh/inventory-databricks/1.1.182-SNAPSHOT/inventory-databricks-1.1.182-SNAPSHOT" +
                        ".jar"));

        ArgumentCaptor<JobSettingsDTO> jobCaptor = ArgumentCaptor.forClass(JobSettingsDTO.class);
        verify(jobService, Mockito.times(1)).upsertJob(jobCaptor.capture(), anyBoolean());
        assertEquals(jobSettingsDTOS.get(0), jobCaptor.getValue());
    }

    @Test
    public void execute_whenJobFileAndTemplateExistsAndEnvironmentIsProd_upsertsJob() throws Exception {
        UpsertJobMojoNoProject underTest = getOverridesMojo(GOAL, "-prod");
        Mockito.when(jobService.getJobByName("dwh/inventory-databricks", true)).thenReturn(createJobDTO
                ("dwh/inventory-databricks", 1));

        underTest.execute();

        List<JobSettingsDTO> jobSettingsDTOs = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();
        assertThat(jobSettingsDTOs.size(), is(1));
        assertThat(jobSettingsDTOs.get(0).getEmailNotifications().getOnFailure(), is(new String[]{"PROD.com"}));
    }

    private JobDTO createJobDTO(String jobName, long jobId) {
        JobDTO jobDTO = new JobDTO();
        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        jobSettingsDTO.setName(jobName);
        jobDTO.setSettings(jobSettingsDTO);
        jobDTO.setJobId(jobId);
        return jobDTO;
    }
}