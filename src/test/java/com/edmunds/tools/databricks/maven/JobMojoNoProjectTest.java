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


import static org.testng.Assert.assertEquals;

import com.edmunds.tools.databricks.maven.model.EnvironmentDTO;
import java.io.File;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link JobMojoNoProject}.
 */
public class JobMojoNoProjectTest extends BaseDatabricksMojoTest {

    private JobMojoNoProject underTest = new JobMojoNoProject();

    @BeforeMethod
    public void init() throws Exception {
        super.init();

        underTest.setDatabricksServiceFactory(databricksServiceFactory);
        underTest.setStreamingOnly(false);
        underTest.setEnvironment("PROD");
        underTest.environmentDTOFile = new File("src/test/resources/databricks-plugin/environment.json");
    }

    @Test
    public void testStopRun_no_job() throws Exception {
        EnvironmentDTO model = underTest.getEnvironmentDTOSupplier().get();

        assertEquals(model.getEnvironment(), "PROD");
    }

}