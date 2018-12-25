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

import com.edmunds.rest.databricks.DTO.ExportFormatDTO;
import com.edmunds.rest.databricks.DTO.LanguageDTO;
import com.edmunds.rest.databricks.request.ImportWorkspaceRequest;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Map;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;

public class ImportWorkspaceMojoTest extends DatabricksMavenPluginTestHarness {


    private final String GOAL = "import-workspace";

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void execute_whenDefaultPrefixIsSet_importsWorkspace() throws Exception {
        ImportWorkspaceMojo underTest = getNoOverridesMojo(GOAL);

        File workspacePath = new File(this.getClass().getResource("/notebooks")
                .getPath());

        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);

        underTest.execute();

        verify(workspaceService).importWorkspace(argThat(getMatcher("/test/mycoolartifact/test1/myFile", LanguageDTO.SQL, "select * from mytable")));
        verify(workspaceService).importWorkspace(argThat(getMatcher("/test/mycoolartifact/test2/myFile", LanguageDTO.SCALA, "println(\"scala is cool\")")));
        verify(workspaceService).importWorkspace(argThat(getMatcher("/test/mycoolartifact/test2/test3/myFile", LanguageDTO.SCALA, "println(\"scala rocks\")")));
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = "JOB NAME VALIDATION FAILED \\[ILLEGAL VALUE\\]:\n" +
            "Expected: \\[failed-test\\] but found: \\[test\\]")
    public void execute_whenDefaultPrefixIsSetAndDoesNotMatchGroupId_failsValidation() throws Exception {

        ImportWorkspaceMojo underTest = getNoOverridesMojo(GOAL, "_fails_validation");

        File workspacePath = new File(this.getClass().getResource("/notebooks")
                .getPath());
        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);

        underTest.execute();
    }

    @Test
    public void execute_whenDefaultPrefixIsSetAndMatchesGroupId_importsWorkspace() throws Exception {
        ImportWorkspaceMojo underTest = getOverridesMojo(GOAL);

        File workspacePath = new File(this.getClass().getResource("/notebooks")
                .getPath());

        underTest.validate = true;
        underTest.setSourceWorkspacePath(workspacePath);

        underTest.execute();

        verify(workspaceService).importWorkspace(argThat(getMatcher("/test/mycoolartifact/test1/myFile", LanguageDTO.SQL, "select * from mytable")));
        verify(workspaceService).importWorkspace(argThat(getMatcher("/test/mycoolartifact/test2/myFile", LanguageDTO.SCALA, "println(\"scala is cool\")")));
        verify(workspaceService).importWorkspace(argThat(getMatcher("/test/mycoolartifact/test2/test3/myFile", LanguageDTO.SCALA, "println(\"scala rocks\")")));
    }

    private ImportWorkspaceRequestMatcher getMatcher(String path, LanguageDTO languageDTO, String content) {
        ImportWorkspaceRequest importWorkspaceRequest = new ImportWorkspaceRequest.ImportWorkspaceRequestBuilder(path)
                .withFormat(ExportFormatDTO.SOURCE)
                .withLanguage(languageDTO)
                .withOverwrite(true)
                .withContent(content.getBytes())
                .build();
        return new ImportWorkspaceRequestMatcher(importWorkspaceRequest);
    }

    /**
     * This is needed, because ImportWorkspaceRequest does not implement the equals method.
     */
    private class ImportWorkspaceRequestMatcher extends ArgumentMatcher<ImportWorkspaceRequest> {
        private static final String CONTENT = "content";
        private final ImportWorkspaceRequest expected;

        public ImportWorkspaceRequestMatcher(ImportWorkspaceRequest expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object obj) {
            if (!(obj instanceof ImportWorkspaceRequest)) {
                return false;
            }
            ImportWorkspaceRequest actual = (ImportWorkspaceRequest) obj;

            // byte arrays need to be converted to string for comparison
            transformByteArrayToString(actual.getData());
            transformByteArrayToString(expected.getData());

            return Maps.difference(actual.getData(), expected.getData()).areEqual();
        }

        private void transformByteArrayToString(Map<String, Object> data) {
            Object content = data.get(CONTENT);
            if (content instanceof byte[]) {
                data.put(CONTENT, new String((byte[]) content));
            }
        }
    }

}