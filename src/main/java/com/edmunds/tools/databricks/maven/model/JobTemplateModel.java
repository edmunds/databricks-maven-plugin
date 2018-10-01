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

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.maven.project.MavenProject;

import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.defaultString;

/**
 * Simple POJO to pass properties to the job template.
 */
public class JobTemplateModel {

    public static final String DEPLOY_VERSION = "deploy-version";
    public static final String COMPANY_GROUP_PREFIX = "com\\.edmunds\\.";

    private Properties projectProperties;
    private Properties systemProperties;
    private String groupId;
    private String artifactId;
    private String version;
    private String environment;
    private String groupWithoutCompany;

    /**
     * Don't use this - it's for jackson deserialization only!
     */
    public JobTemplateModel() {
    }

    public JobTemplateModel(MavenProject project) {
        this.groupId = project.getGroupId();
        this.artifactId = project.getArtifactId();
        this.projectProperties = project.getProperties();
        this.systemProperties = System.getProperties();
        this.version = defaultString(systemProperties.getProperty(DEPLOY_VERSION), project.getVersion());
        this.groupWithoutCompany = stripCompanyPackage(project.getGroupId());
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
}
