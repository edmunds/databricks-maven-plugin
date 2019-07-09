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
import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.rest.databricks.DTO.JobsDTO;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for @{@link UpsertJobMojo}.
 * <p>
 * For these tests, the regex as part of the expected exceptions no longer works.
 */
public class UpsertJobMojoTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "upsert-job";

    private UpsertJobMojo underTest;

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
        underTest = getNoOverridesMojo(GOAL);
    }

    @Test
    public void test_executeWithDefault() throws Exception {
        underTest = getNoOverridesMojo(GOAL);
        Mockito.when(jobService.getJobByName("unit-test-group/unit-test-artifact", true)).thenReturn(createJobDTO
                ("unit-test-group/unit-test-artifact", 1));
        underTest.execute();

        List<JobSettingsDTO> jobSettingsDTOs = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();
        assert (jobSettingsDTOs.size() == 1);
        ArgumentCaptor<JobSettingsDTO> jobCaptor = ArgumentCaptor.forClass(JobSettingsDTO.class);
        verify(jobService, Mockito.times(1)).upsertJob(jobCaptor.capture(), anyBoolean());
        assertEquals(jobSettingsDTOs.get(0), jobCaptor.getValue());
    }

    @Test
    public void test_executeWithProjectProperties() throws Exception {
        underTest = getOverridesMojo(GOAL, "_viaProperties");
        Mockito.when(jobService.getJobByName("unit-test-group/unit-test-artifact", true)).thenReturn(createJobDTO
                ("unit-test-group/unit-test-artifact", 1));
        List<JobSettingsDTO> jobSettingsDTOs = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();
        underTest.execute();
        assertThat(jobSettingsDTOs.size(), is(1));
        assertThat(jobSettingsDTOs.get(0).getLibraries()[0].getJar(), is
                ("s3://projectProperty/unit-test-group/unit-test-artifact/" +
                        "1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
        assertThat(jobSettingsDTOs.get(0).getLibraries()[1].getMaven().getCoordinates(), is
                ("Huawei-Spark:Spark-SQL-on-HBase:1.0.0"));
        ArgumentCaptor<JobSettingsDTO> jobCaptor = ArgumentCaptor.forClass(JobSettingsDTO.class);
        verify(jobService, Mockito.times(1)).upsertJob(jobCaptor.capture(), anyBoolean());
        assertEquals(jobSettingsDTOs.get(0), jobCaptor.getValue());
    }

    @Test
    public void test_executeWithProjectPropertiesAndConfig() throws Exception {
        underTest = getOverridesMojo(GOAL, "_viaBothSettings");
        Mockito.when(jobService.getJobByName("unit-test-group/unit-test-artifact", true)).thenReturn(createJobDTO
                ("unit-test-group/unit-test-artifact", 1));
        List<JobSettingsDTO> jobSettingsDTOs = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();
        underTest.execute();
        assertThat(jobSettingsDTOs.size(), is(1));
        assertThat(jobSettingsDTOs.get(0).getLibraries()[0].getJar(), is
                ("s3://configProperty/unit-test-group/unit-test-artifact/" +
                        "1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
        ArgumentCaptor<JobSettingsDTO> jobCaptor = ArgumentCaptor.forClass(JobSettingsDTO.class);
        verify(jobService, Mockito.times(1)).upsertJob(jobCaptor.capture(), anyBoolean());
        assertEquals(jobSettingsDTOs.get(0), jobCaptor.getValue());
    }

    @Test
    public void test_executeWithMissingProperties() throws Exception {
        underTest = getMissingMandatoryMojo(GOAL);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }

    @Test
    public void test_getJobSettingsDefault_returnsDefaultFile() throws Exception {
        underTest = getNoOverridesMojo(GOAL);
        List<JobSettingsDTO> jobSettingsDTOs = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();

        assertThat(jobSettingsDTOs.size(), is(1));
        assertThat(jobSettingsDTOs.get(0).getName(), is("unit-test-group/unit-test-artifact"));
        assertThat(jobSettingsDTOs.get(0).getLibraries()[0].getJar(), is
                ("s3://my-bucket/unit-test-group/unit-test-artifact/" +
                        "1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
    }

    @Test
    public void test_getJobSettingsDtoWithFile_returnsNoFile() throws Exception {
        underTest = getOverridesMojo(GOAL);
        List<JobSettingsDTO> jobSettingsDTOs = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();

        assertThat(jobSettingsDTOs.size(), is(0));
    }

    @Test
    public void testGetJobId_single() throws Exception {
        when(jobService.listAllJobs()).thenReturn(createJobsDTO(createJobDTO("test-job", 123L)));
        when(jobService.getJobByName("test-job", true)).thenReturn(createJobDTO("test-job", 123L));
        Long jobId = underTest.getJobId("test-job");
        assertThat(jobId, is(123L));
    }

    @Test
    public void testGetJobId_multiple() throws Exception {
        when(jobService.listAllJobs()).thenReturn(createJobsDTO(createJobDTO("test-job", 123L), createJobDTO("test-job", 456L)));
        when(jobService.getJobByName("test-job", true)).thenThrow(new IllegalStateException());
        try {
            underTest.getJobId("test-job");
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }

    @Test
    public void testGetJobId_multiple_no_exception() throws Exception {
        underTest.failOnDuplicateJobName = false;
        when(jobService.listAllJobs()).thenReturn(createJobsDTO(createJobDTO("test-job", 123L), createJobDTO("test-job", 456L)));

        when(jobService.getJobByName("test-job", false)).thenReturn(createJobDTO("test-job", 123L));

        Long jobId = underTest.getJobId("test-job");
        assertThat(jobId, is(123L));
    }

    @Test
    public void testGetJobId_none() throws Exception {
        when(jobService.listAllJobs()).thenReturn(createJobsDTO(createJobDTO("test-job", 123L), createJobDTO("test-job", 456L)));
        when(jobService.getJobByName("fake-job", true)).thenReturn(null);

        Long jobId = underTest.getJobId("fake-job");
        assertThat(jobId, nullValue());
    }


    private JobsDTO createJobsDTO(JobDTO... jobDTOs) {
        JobsDTO jobsDTO = new JobsDTO();
        jobsDTO.setJobs(jobDTOs);
        return jobsDTO;
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