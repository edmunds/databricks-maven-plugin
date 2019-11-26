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
import com.edmunds.rest.databricks.DTO.workspace.ObjectInfoDTO;
import com.edmunds.rest.databricks.DTO.workspace.ObjectTypeDTO;
import com.edmunds.rest.databricks.DatabricksRestException;
import com.edmunds.rest.databricks.request.ExportWorkspaceRequest;
import com.edmunds.rest.databricks.service.WorkspaceService;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.apache.commons.lang3.StringUtils.substringAfter;

/**
 * Interacts with the databricks workspace api.
 */
@Mojo(name = "workspace-tool", requiresProject = true)
public class WorkspaceToolMojo extends BaseWorkspaceMojo {

    /**
     * Workspace command to execute.
     */
    public enum WorkspaceCommand {
        EXPORT, LIST
    }

    /**
     * The workspace command to execute.<br>
     * EXPORT - export a workspace path FROM databricks to your local machine.<br>
     * LIST - list the contents of a workspace path.<br>
     */
    @Parameter(property = "workspace.command", required = true)
    private WorkspaceCommand workspaceCommand;

    public void execute() throws MojoExecutionException {

        try {
            switch (workspaceCommand) {
                case EXPORT: {
                    exportWorkspace();
                    break;
                }
                case LIST: {
                    listWorkspace();
                    break;
                }
                default: {
                    throw new MojoExecutionException("this should not happen");
                }
            }
        } catch (DatabricksRestException | IOException e) {
            throw new MojoExecutionException(String.format("Could not execute workspace command: [%s]. Local Path: " +
                    "[%s] DB Path: [%s]", workspaceCommand, getSourceFullWorkspacePath(), getDbWorkspacePath()), e);
        }
    }

    private void listWorkspace() throws IOException, DatabricksRestException {
        getLog().info(String.format("List for: [%s]", getDbWorkspacePath()));
        accept(getDbWorkspacePath(), objectInfoDTO ->
                getLog().info(ReflectionToStringBuilder.toString(objectInfoDTO, ToStringStyle.JSON_STYLE)));
    }

    private void exportWorkspace() throws IOException, DatabricksRestException {
        // We export databricks notebooks into our source code directory (sourceWorkspacePath).
        accept(getDbWorkspacePath(), objectInfoDTO -> {
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

        return substringAfter(objectInfoDTO.getPath(), getDbWorkspacePath()) + "." + extension;
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

    public void setWorkspaceCommand(WorkspaceCommand workspaceCommand) {
        this.workspaceCommand = workspaceCommand;
    }

    /**
     * Simple visitor pattern to visit ObjectInfoDTO elements.
     */
    @FunctionalInterface
    public interface ObjectInfoVisitor {
        void visit(ObjectInfoDTO objectInfoDTO) throws IOException, DatabricksRestException;
    }
}
