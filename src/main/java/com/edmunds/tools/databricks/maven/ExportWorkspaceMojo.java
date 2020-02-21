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

import static org.apache.commons.lang3.StringUtils.substringAfter;

import com.edmunds.rest.databricks.DTO.workspace.ExportFormatDTO;
import com.edmunds.rest.databricks.DTO.workspace.ObjectInfoDTO;
import com.edmunds.rest.databricks.DTO.workspace.ObjectTypeDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.request.ExportWorkspaceRequest;
import com.edmunds.rest.databricks.service.WorkspaceService;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;


/**
 * Exports a workspace folder into a local directory.
 * Useful when developing on databricks and then copying it down to your development environment.
 */
@Mojo(name = "export-workspace", requiresProject = true)
public class ExportWorkspaceMojo extends BaseWorkspaceMojo {

    /**
     * Execute ExportWorkspaceMojo.
     *
     * @throws MojoExecutionException exception
     */
    public void execute() throws MojoExecutionException {
        try {
            exportWorkspace();
        } catch (DatabricksRestException | IOException e) {
            throw new MojoExecutionException(String.format("Could not export workspace. Local Path: "
                    + "[%s] DB Path: [%s]", getSourceFullWorkspacePath(), getWorkspacePrefix()), e);
        }
    }

    private void exportWorkspace() throws IOException, DatabricksRestException {
        // We export databricks notebooks into our source code directory (sourceWorkspacePath).
        accept(getWorkspacePrefix(), objectInfoDTO -> {
            if (objectInfoDTO.getObjectType() == ObjectTypeDTO.NOTEBOOK) {
                String outputFilename = createOutputFilename(objectInfoDTO);

                getLog().info(String.format("exporting: [%s] to: [%s]", outputFilename, getSourceFullWorkspacePath()));

                FileUtils.writeStringToFile(new File(getSourceFullWorkspacePath(), outputFilename),
                        getSource(objectInfoDTO), StandardCharsets.UTF_8);
            }
        });
    }

    private String getSource(ObjectInfoDTO objectInfoDTO) throws IOException, DatabricksRestException {
        String path = URLEncoder.encode(objectInfoDTO.getPath(), "UTF-8");
        ExportWorkspaceRequest exportWorkspaceRequest = new ExportWorkspaceRequest.ExportWorkspaceRequestBuilder(path)
                .withFormat(ExportFormatDTO.SOURCE)
                .build();
        byte[] bytes = getWorkspaceService().exportWorkspace(exportWorkspaceRequest);
        return StringUtils.newStringUtf8(Base64.decodeBase64(new String(bytes, StandardCharsets.UTF_8)));
    }

    private String createOutputFilename(ObjectInfoDTO objectInfoDTO) {
        String extension = objectInfoDTO.getLanguage().name().toLowerCase();
        if (extension.equals("python")) {
            extension = "py";
        }

        return substringAfter(objectInfoDTO.getPath(), getWorkspacePrefix()) + "." + extension;
    }

    private void accept(String path, ObjectInfoVisitor visitor) throws IOException, DatabricksRestException {
        String encodedPath = URLEncoder.encode(path, "UTF-8");
        ObjectInfoDTO[] objectInfoDTOS = getWorkspaceService().listStatus(encodedPath);
        if (objectInfoDTOS == null) {
            getLog().warn("objectInfo was null for: " + path + ". Not downloading");
            return;
        }
        for (ObjectInfoDTO objectInfoDTO : objectInfoDTOS) {
            visitor.visit(objectInfoDTO);
            if (objectInfoDTO.getObjectType() == ObjectTypeDTO.DIRECTORY) {
                accept(objectInfoDTO.getPath(), visitor);
            }
        }
    }

    private WorkspaceService getWorkspaceService() {
        return getDatabricksServiceFactory().getWorkspaceService();
    }

    /**
     * Simple visitor pattern to visit ObjectInfoDTO elements.
     */
    @FunctionalInterface
    public interface ObjectInfoVisitor {

        void visit(ObjectInfoDTO objectInfoDTO) throws IOException, DatabricksRestException;
    }
}
