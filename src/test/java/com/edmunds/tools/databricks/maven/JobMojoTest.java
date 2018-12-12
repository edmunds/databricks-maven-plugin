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


import com.edmunds.rest.databricks.DTO.RunDTO;
import com.edmunds.rest.databricks.DTO.RunNowDTO;
import com.edmunds.rest.databricks.DTO.RunsDTO;
import org.apache.maven.plugin.MojoExecutionException;

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

/**
 * Tests for {@link JobMojo}.
 */
public class JobMojoTest extends BaseDatabricksMojoTest {

    private JobMojo underTest = new JobMojo();

    public void beforeMethod() throws Exception {
        super.beforeMethod();
        underTest.setDatabricksServiceFactory(databricksServiceFactory);
        underTest.setStreamingOnly(false);
        underTest.setEnvironment("QA");
    }

    public void testGetRunDTOs_empty() throws Exception {
        beforeMethod();
        when(jobService.listRuns(eq(123l), eq(true), anyInt(), anyInt())).thenReturn(new RunsDTO());

        RunDTO[] runDTOs = underTest.getRunDTOs(123L);
        assertThat(runDTOs.length, is(0));
    }

    public void testGetRunDTOs_single() throws Exception {
        beforeMethod();
        when(jobService.listRuns(eq(123l), eq(true), anyInt(), anyInt())).thenReturn(createRunsDTO());

        RunDTO[] runDTOs = underTest.getRunDTOs(123L);
        assertThat(runDTOs.length, is(1));
        assertThat(runDTOs[0].getRunName(), is("test run, fire!"));
    }

    public void testStartRun() throws Exception {
        beforeMethod();
        when(jobService.listRuns(eq(123l), eq(true), anyInt(), anyInt())).thenReturn(new RunsDTO());
        when(jobService.runJobNow(eq(123l))).thenReturn(createRunNowDTO());

        RunNowDTO observed = underTest.startRun(123l);

        assertThat(observed.getNumberInJob(), is(1L));
    }

    public void testStartRun_existing() throws Exception {
        beforeMethod();
        //Regex no longer works with annotation
        when(jobService.listRuns(eq(123l), eq(true), anyInt(), anyInt())).thenReturn(createRunsDTO());
        try {
            underTest.startRun(123l);
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("Job: [123] already has an existing active run."));
            return;
        }
        fail();
    }

    public void testStopRun_exists() throws Exception {
        beforeMethod();
        when(jobService.listRuns(eq(123l), eq(true), anyInt(), anyInt())).thenReturn(createRunsDTO());

        underTest.stopActiveRuns(123l);

        verify(jobService, times(1)).cancelRun(456l);
    }

    public void testStopRun_no_job() throws Exception {
        beforeMethod();
        when(jobService.listRuns(eq(123l), eq(true), anyInt(), anyInt())).thenReturn(new RunsDTO());

        underTest.stopActiveRuns(123l);

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
        runDTO.setRunId(456l);

        runsDTO.setRuns(new RunDTO[]{runDTO});
        return runsDTO;
    }
}