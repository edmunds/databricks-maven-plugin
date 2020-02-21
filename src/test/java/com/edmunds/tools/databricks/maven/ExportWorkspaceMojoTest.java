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

import com.edmunds.rest.databricks.DTO.workspace.ExportFormatDTO;
import com.edmunds.rest.databricks.DTO.workspace.LanguageDTO;
import com.edmunds.rest.databricks.DTO.workspace.ObjectInfoDTO;
import com.edmunds.rest.databricks.DTO.workspace.ObjectTypeDTO;
import com.edmunds.rest.databricks.request.ExportWorkspaceRequest;
import com.edmunds.rest.databricks.request.ImportWorkspaceRequest;
import com.google.common.collect.Maps;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;

public class ExportWorkspaceMojoTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "export-workspace";

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void execute_whenDefaultPrefixIsSet_exportsWorkspace() throws Exception {
        ExportWorkspaceMojo underTest = getNoOverridesMojo(GOAL);

        String basePath = "/test/mycoolartifact";
        underTest.setWorkspacePrefix("com.edmunds.test/mycoolartifact");
        File sourceWorkspace = new File("./target/src/main/notebooks");

        underTest.setSourceWorkspacePath(sourceWorkspace);
        underTest.setWorkspacePrefix("test/mycoolartifact");

        underTest.validate = true;

        ObjectInfoDTO[] objectInfoDTOS = new ObjectInfoDTO[2];
        objectInfoDTOS[0] = buildObjectInfoDTO(basePath, "myFile", ObjectTypeDTO.NOTEBOOK);
        objectInfoDTOS[1] = buildObjectInfoDTO(basePath, "myNested", ObjectTypeDTO.DIRECTORY);
        Mockito.when(workspaceService.listStatus(URLEncoder.encode(basePath, "UTF-8"))).thenReturn(objectInfoDTOS);
        Mockito.when(workspaceService.listStatus(URLEncoder.encode(basePath + "/myNested", "UTF-8"))).thenReturn(new
                ObjectInfoDTO[]{
                buildObjectInfoDTO(basePath + "/myNested", "myNestedFile", ObjectTypeDTO.NOTEBOOK)});
        Mockito.when(workspaceService.exportWorkspace(Mockito.any(ExportWorkspaceRequest.class))).thenReturn
                ("MyCode!".getBytes(StandardCharsets.UTF_8));

        underTest.execute();

        Mockito.verify(workspaceService, Mockito.times(2)).listStatus(Mockito.anyString());
        Mockito.verify(workspaceService, Mockito.times(2)).exportWorkspace(Mockito.any(ExportWorkspaceRequest.class));

        assert (Paths.get(sourceWorkspace.getPath(), "test/mycoolartifact/myFile.scala").toFile().exists());
        assert (Paths.get(sourceWorkspace.getPath(), "test/mycoolartifact/myNested/myNestedFile.scala").toFile()
                .exists());
    }

    private ExportWorkspaceRequestMatcher getMatcher(String path) {
        ExportWorkspaceRequest exportWorkspaceRequest = new ExportWorkspaceRequest.ExportWorkspaceRequestBuilder(path)
            .withFormat(ExportFormatDTO.SOURCE)
            .build();
        return new ExportWorkspaceRequestMatcher(exportWorkspaceRequest);
    }

    private static class ExportWorkspaceRequestMatcher extends ArgumentMatcher<ExportWorkspaceRequest> {

        private static final String CONTENT = "content";
        private final ExportWorkspaceRequest expected;

        public ExportWorkspaceRequestMatcher(ExportWorkspaceRequest expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object obj) {
            if (!(obj instanceof ExportWorkspaceRequest)) {
                return false;
            }
            ExportWorkspaceRequest actual = (ExportWorkspaceRequest) obj;

            // byte arrays need to be converted to string for comparison
            transformByteArrayToString(actual.getData());
            transformByteArrayToString(expected.getData());

            return Maps.difference(actual.getData(), expected.getData()).areEqual();
        }

        private void transformByteArrayToString(Map<String, Object> data) {
            Object content = data.get(CONTENT);
            if (content instanceof byte[]) {
                data.put(CONTENT, new String((byte[]) content, StandardCharsets.UTF_8));
            }
        }
    }

    private ObjectInfoDTO buildObjectInfoDTO(String basePath, String fileName, ObjectTypeDTO objectTypeDTO) {
        ObjectInfoDTO objectInfoDTO = new ObjectInfoDTO();
        objectInfoDTO.setPath(basePath + "/" + fileName);
        objectInfoDTO.setLanguage(LanguageDTO.SCALA);
        objectInfoDTO.setObjectType(objectTypeDTO);
        return objectInfoDTO;
    }
}