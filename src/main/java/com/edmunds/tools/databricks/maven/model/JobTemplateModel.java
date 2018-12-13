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

package com.edmunds.tools.databricks.maven.model;

import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.defaultString;

/**
 * Simple POJO to pass properties to the job template.
 */
//TODO should be renamed "ProjectModel" and no longer be meant just for jobs.
public class JobTemplateModel {

    public static final String DEPLOY_VERSION = "deploy-version";
    public static final String COMPANY_GROUP_PREFIX = "com\\.edmunds\\.";

    private Properties projectProperties;
    private Properties systemProperties;
    private String groupId;
    private String artifactId;
    private String version;
    // TODO This property should not be persisted with the template model
    private String environment;
    private String groupWithoutCompany;
    // The rationale for persisting these properties is because a deployed artifact will already have been deployed
    // to a specific place. You cannot change that after the fact!
    private String databricksRepo;
    private String databricksRepoKey;

    /**
     * Don't use this - it's for jackson deserialization only!
     */
    public JobTemplateModel() {
    }

    public JobTemplateModel(MavenProject project,
                            String environment, String databricksRepo, String databricksRepoKey) {
        this.groupId = project.getGroupId();
        this.artifactId = project.getArtifactId();
        this.projectProperties = project.getProperties();
        this.systemProperties = System.getProperties();
        this.version = defaultString(systemProperties.getProperty(DEPLOY_VERSION), project.getVersion());
        this.groupWithoutCompany = stripCompanyPackage(project.getGroupId());
        this.databricksRepo = databricksRepo;
        this.databricksRepoKey = databricksRepoKey;
        this.environment = environment;
        //TODO NEED TO GET RID OF this once we are ready. This is for backwards compatibility
        projectProperties.setProperty("databricks.repo", databricksRepo);
        projectProperties.setProperty("databricks.repo.key", databricksRepoKey);
    }

    public static String stripCompanyPackage(String path) {
        return path.replaceAll(COMPANY_GROUP_PREFIX, "");
    }

    public Properties getProjectProperties() {
        return projectProperties;
    }

    public Properties getSystemProperties() {
        return systemProperties;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getVersion() {
        return version;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getGroupWithoutCompany() {
        return groupWithoutCompany;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public String getDatabricksRepo() {
        return databricksRepo;
    }

    public void setDatabricksRepo(String databricksRepo) {
        this.databricksRepo = databricksRepo;
    }

    public String getDatabricksRepoKey() {
        return databricksRepoKey;
    }

    public void setDatabricksRepoKey(String databricksRepoKey) {
        this.databricksRepoKey = databricksRepoKey;
    }

    public static JobTemplateModel loadJobTemplateModelFromFile(File jobTemplateModelFile) throws
                                                                                          MojoExecutionException {
        if (jobTemplateModelFile == null) {
            throw new MojoExecutionException("jobTemplateModelFile must be set!");
        }
        try {
            String jobTemplateModelJson = FileUtils.readFileToString(jobTemplateModelFile);
            return ObjectMapperUtils.deserialize(jobTemplateModelJson, JobTemplateModel.class);
        } catch (IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}
