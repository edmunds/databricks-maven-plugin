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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.edmunds.rest.databricks.DTO.RunNowDTO;
import com.edmunds.rest.databricks.DTO.RunsDTO;
import com.edmunds.rest.databricks.DTO.jobs.RunDTO;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link JobMojo}.
 */
public class JobMojoTest extends BaseDatabricksMojoTest {

    private JobMojo underTest = new JobMojo();

    @BeforeMethod
    public void init() throws Exception {
        super.init();

        underTest.setDatabricksServiceFactory(databricksServiceFactory);
        underTest.setStreamingOnly(false);
        underTest.setEnvironment("QA");
    }

    @Test
    public void testGetRunDTOs_empty() throws Exception {
        when(jobService.listRuns(eq(123L), eq(true), anyInt(), anyInt())).thenReturn(new RunsDTO());

        RunDTO[] runDTOs = underTest.getRunDTOs(123L);
        assertThat(runDTOs.length, is(0));
    }

    @Test
    public void testGetRunDTOs_single() throws Exception {
        when(jobService.listRuns(eq(123L), eq(true), anyInt(), anyInt())).thenReturn(createRunsDTO());

        RunDTO[] runDTOs = underTest.getRunDTOs(123L);
        assertThat(runDTOs.length, is(1));
        assertThat(runDTOs[0].getRunName(), is("test run, fire!"));
    }

    @Test
    public void testStartRun() throws Exception {
        when(jobService.listRuns(eq(123L), eq(true), anyInt(), anyInt())).thenReturn(new RunsDTO());
        when(jobService.runJobNow(eq(123L))).thenReturn(createRunNowDTO());

        RunNowDTO observed = underTest.startRun(123L);

        assertThat(observed.getNumberInJob(), is(1L));
    }

    @Test
    public void testStartRun_existing() throws Exception {
        //Regex no longer works with annotation
        when(jobService.listRuns(eq(123L), eq(true), anyInt(), anyInt())).thenReturn(createRunsDTO());
        try {
            underTest.startRun(123L);
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("Job: [123] already has an existing active run."));
            return;
        }
        fail();
    }

    @Test
    public void testStopRun_exists() throws Exception {
        when(jobService.listRuns(eq(123L), eq(true), anyInt(), anyInt())).thenReturn(createRunsDTO());

        underTest.stopActiveRuns(123L);

        verify(jobService, times(1)).cancelRun(456L);
    }

    @Test
    public void testStopRun_no_job() throws Exception {
        when(jobService.listRuns(eq(123L), eq(true), anyInt(), anyInt())).thenReturn(new RunsDTO());

        underTest.stopActiveRuns(123L);

        verify(jobService, never()).cancelRun(anyLong());
    }

    private RunNowDTO createRunNowDTO() {
        RunNowDTO runNowDTO = new RunNowDTO();
        runNowDTO.setNumberInJob(1);
        return runNowDTO;
    }

    private RunsDTO createRunsDTO() {
        RunsDTO runsDTO = new RunsDTO();

        RunDTO runDTO = new RunDTO();
        runDTO.setRunName("test run, fire!");
        runDTO.setRunId(456L);

        runsDTO.setRuns(new RunDTO[]{runDTO});
        return runsDTO;
    }
}