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

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;


import static org.apache.commons.io.FilenameUtils.getBaseName;
import static org.apache.commons.lang3.StringUtils.substringAfter;

/**
 * A base class for workspace mojos.
 */
public abstract class BaseWorkspaceMojo extends BaseDatabricksMojo {

    /**
     * This is the base path where databricks notebooks live in your project.
     */
    @Parameter(property = "sourceWorkspacePath", required = true, defaultValue = "${project.basedir}/src/main/notebooks")
    protected File sourceWorkspacePath;

    /**
     * This is where the databricks notebooks are packaged as part of a build.
     * This should not require changing.
     */
    //TODO this property should probably not be changeable
    @Parameter(property = "packagedWorkspacePath", required = true, defaultValue = "${project.build.directory}/notebooks/")
    protected File packagedWorkspacePath;

    //TODO this prefix should not be changeable, and seems repeat of dbWorkspacePath
    @Parameter(property = "workspacePrefix", required = true, defaultValue = "${project.groupId}/${project.artifactId}")
    protected String workspacePrefix;

    /**
     * This is where the notebooks live in the databricks workspace.
     */
    @Parameter(property = "dbWorkspacePath", required = true, defaultValue = "/${project.groupId}/${project" +
            ".artifactId}")
    private String dbWorkspacePath;

    // Valid extensions for databricks notebooks
    public static final List<String> DATABRICKS_SOURCE_EXTENSIONS = Arrays.asList("scala", "py", "r", "sql");


    protected String getSourceFullWorkspacePath() {
        String strippedPrefix = stripPrefix(workspacePrefix);
        return Paths.get(sourceWorkspacePath.getPath(), strippedPrefix).toString();
    }

    protected String getRemoteFullWorkspacePath() {
        //Seperator should always be "/"
        String strippedPrefix = stripPrefix(workspacePrefix);
        return (packagedWorkspacePath.getPath() + strippedPrefix).replace("\\", "/");
    }

    /**
     * Validates a notebook path meets requirements for uploading to databricks.
     *
     * @param notebookPath the root notebook path containing all notebooks
     * @throws MojoExecutionException if validation fails
     */
    void validateNotebooks(File notebookPath) throws MojoExecutionException {
        if (!notebookPath.exists()) {
            getLog().info(String.format("No notebooks found at [%s]", notebookPath.getPath()));
            return;
        }
        Collection<File> files = FileUtils.listFiles(notebookPath,
                new SuffixFileFilter(DATABRICKS_SOURCE_EXTENSIONS),
                DirectoryFileFilter.DIRECTORY);
        for (File file : files) {
            // e.g. the path under the local root, not the full path to it
            String relativePath = substringAfter(file.getParentFile().getPath(), notebookPath.getPath());
            String remoteFilePath = relativePath + "/" + getBaseName(file.getName());
            getLog().info(String.format("Validating: [%s]", remoteFilePath));
            if (validate) {
                validatePath(remoteFilePath, project.getGroupId(), project.getArtifactId());
            }
        }
    }

    void setPackagedWorkspacePath(File packagedWorkspacePath) {
        this.packagedWorkspacePath = packagedWorkspacePath;
    }

    void setWorkspacePrefix(String workspacePrefix) {
        this.workspacePrefix = workspacePrefix;
    }

    public void setSourceWorkspacePath(File sourceWorkspacePath) {
        this.sourceWorkspacePath = sourceWorkspacePath;
    }

    public String getDbWorkspacePath() {
        return stripPrefix(dbWorkspacePath).replaceAll("\\.", "/");
    }

    public void setDbWorkspacePath(String dbWorkspacePath) {
        this.dbWorkspacePath = dbWorkspacePath;
    }
}
