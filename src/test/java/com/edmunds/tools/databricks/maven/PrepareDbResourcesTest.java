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

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.List;

import static com.edmunds.tools.databricks.maven.BaseDatabricksJobMojo.DEFAULT_JOB_JSON;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class PrepareDbResourcesTest extends BaseDatabricksMojoTest {

    private PrepareDbResources underTest = new PrepareDbResources();

    private ClassLoader classLoader = PrepareDbResources.class.getClassLoader();

    private String outputBuildDir;

    public void beforeMethod() throws Exception {
        super.beforeMethod();
        underTest.setDatabricksServiceFactory(databricksServiceFactory);
        underTest.setProject(project);
        underTest.setFailOnDuplicateJobName(true);
        outputBuildDir = build.getOutputDirectory();
    }

    public void testGetJobSettingsDTOs() throws Exception {
        beforeMethod();
        File outputFile = new File(outputBuildDir + PrepareDbResources.MODEL_FILE_NAME);
        underTest.setDbJobFile(new File(classLoader.getResource(DEFAULT_JOB_JSON).getFile()));
        underTest.setJobTemplateModelFile(outputFile);
        underTest.prepareJobTemplateModel();
        String lines = FileUtils.readFileToString(outputFile);

        assertThat(lines, containsString("  \"groupId\" : \"com.edmunds.test\","));
        assertThat(lines, containsString("  \"artifactId\" : \"mycoolartifact\","));
        assertThat(lines, containsString("  \"version\" : \"1.0\","));
        assertThat(lines, containsString("  \"environment\" : null,"));
        assertThat(lines, containsString("  \"groupWithoutCompany\" : \"test\""));
    }
}