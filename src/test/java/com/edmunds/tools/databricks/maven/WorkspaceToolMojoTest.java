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


import com.edmunds.rest.databricks.DTO.LanguageDTO;
import com.edmunds.rest.databricks.DTO.ObjectInfoDTO;
import com.edmunds.rest.databricks.DTO.ObjectTypeDTO;
import com.edmunds.rest.databricks.request.ExportWorkspaceRequest;
import java.io.File;
import java.net.URLEncoder;
import java.nio.file.Paths;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;

public class WorkspaceToolMojoTest extends BaseDatabricksMojoTest {

    private WorkspaceToolMojo underTest = new WorkspaceToolMojo();

    @BeforeMethod
    public void init() throws Exception {
        super.init();

        underTest.setDatabricksServiceFactory(databricksServiceFactory);
        underTest.setEnvironment("QA");
    }

    //TODO currently does not work with maven...
    @Ignore
    public void testExportExecute() throws Exception {
        String basePath = "test/mycoolartifact";
        underTest.setDbWorkspacePath("com.edmunds.test/mycoolartifact");
        File sourceWorkspace = new File("./target/src/main/notebooks");

        underTest.setSourceWorkspacePath(sourceWorkspace);
        underTest.setWorkspaceCommand(WorkspaceToolMojo.WorkspaceCommand.EXPORT);
        underTest.setWorkspacePrefix("test/mycoolartifact");

        ObjectInfoDTO[] objectInfoDTOS = new ObjectInfoDTO[2];
        objectInfoDTOS[0] = buildObjectInfoDTO(basePath, "myFile", ObjectTypeDTO.NOTEBOOK);
        objectInfoDTOS[1] = buildObjectInfoDTO(basePath, "myNested", ObjectTypeDTO.DIRECTORY);
        Mockito.when(workspaceService.listStatus(URLEncoder.encode(basePath, "UTF-8"))).thenReturn(objectInfoDTOS);
        Mockito.when(workspaceService.listStatus(URLEncoder.encode(basePath + "/myNested", "UTF-8"))).thenReturn(new
                ObjectInfoDTO[]{
                buildObjectInfoDTO(basePath + "/myNested", "myNestedFile", ObjectTypeDTO.NOTEBOOK)});
        Mockito.when(workspaceService.exportWorkspace(Mockito.any(ExportWorkspaceRequest.class))).thenReturn
                ("MyCode!".getBytes("UTF-8"));

        underTest.execute();

        Mockito.verify(workspaceService, Mockito.times(2)).exportWorkspace(Mockito.any(ExportWorkspaceRequest.class));
        Mockito.verify(workspaceService, Mockito.times(2)).listStatus(Mockito.anyString());

        assert (Paths.get(sourceWorkspace.getPath(), "test/mycoolartifact/myFile.scala").toFile().exists());
        assert (Paths.get(sourceWorkspace.getPath(), "test/mycoolartifact/myNested/myNestedFile.scala").toFile()
                .exists());
    }

    private ObjectInfoDTO buildObjectInfoDTO(String basePath, String fileName, ObjectTypeDTO objectTypeDTO) {
        ObjectInfoDTO objectInfoDTO = new ObjectInfoDTO();
        objectInfoDTO.setPath(basePath + "/" + fileName);
        objectInfoDTO.setLanguage(LanguageDTO.SCALA);
        objectInfoDTO.setObjectType(objectTypeDTO);
        return objectInfoDTO;
    }
}