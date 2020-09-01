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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.edmunds.rest.databricks.DatabricksServiceFactory;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * The base databricks mojo.
 */
public abstract class BaseDatabricksMojo extends AbstractMojo {

    private static final List<String> ALLOWED_REPO_TYPES = Arrays.asList("s3", "dbfs");

    private static final String DB_USER = "DB_USER";
    private static final String DB_PASSWORD = "DB_PASSWORD";
    private static final String DB_URL = "DB_URL";
    private static final String DB_TOKEN = "DB_TOKEN";

    @Parameter(defaultValue = "${project}", readonly = true)
    protected MavenProject project;

    /**
     * The repo type.
     * Allowed "s3" and "dbfs" types.
     * In case when the parameter is s3:
     *  method createDeployedArtifactPath() will return path with prefix "s3://"
     * In case when the parameter is dbfs:
     *  method createDeployedArtifactPath() will return path with prefix "dbfs://"
     */
    @Parameter(name = "databricksRepoType", property = "databricks.repo.type", defaultValue = "s3")
    protected String databricksRepoType;

    //TODO validate even with required=true? How does that play with the env properties
    /**
     * The repo location on s3 that you want to upload your jar to.
     * At the very least this should be an s3 bucket name like "my-bucket"
     * BUT you can also specify a common prefix here in addition to a bucket,
     * for example:
     * "my-bucket/artifacts"
     * This property is not required due to the no project option.
     * If both project property and mojo configuration is set, mojo configuration wins.
     */
    @Parameter(name = "databricksRepo", property = "databricks.repo")
    protected String databricksRepo;

    /**
     * The prefix to load to. This is appended to the databricksRepo property.
     * This is an artifact specific key and will by default be the maven style qualifier:
     * groupId/artifactId/version/artifact-version.jar
     */
    @Parameter(name = "databricksRepoKey", property = "databricks.repo.key",
        defaultValue = "${project.groupId}/${project.artifactId}/${project.version}/${project.build.finalName}"
            + ".${project.packaging}")
    protected String databricksRepoKey;

    /**
     * The aws databricksRepoRegion that the bucket is located in.
     */
    @Parameter(property = "databricksRepoRegion", defaultValue = "us-east-1")
    protected String databricksRepoRegion;

    /**
     * The environment name. Is used in freemarker templating for conditional job settings.
     */
    @Parameter(name = "environment", property = "environment")
    protected String environment;

    /**
     * This property can be picked up via an environment property. DB_URL
     */
    @Parameter(name = "host", property = "host")
    protected String host;

    /**
     * This property can be picked up via an environment property. DB_TOKEN NOTE: user+password authentication will take
     * precedence over token based authentication if both are provided.
     */
    @Parameter(name = "token", property = "token")
    protected String token;

    /**
     * This property can be picked up via an environment property. DB_USER
     */
    @Parameter(name = "user", property = "user")
    protected String user;

    /**
     * This property can be picked up via an environment property. DB_PASSWORD
     */
    @Parameter(name = "password", property = "password")
    protected String password;

    /**
     * Whether or not you want to validate the databricks job settings file.
     */
    @Parameter(name = "validate", defaultValue = "true", property = "validate")
    protected boolean validate;

    /**
     * The value that will be stripped off of the groupId and used in path setting and job names.
     * For example: 'com.edmunds' or 'org.myproject'. Keep in mind, this value is a regex, so, '.' should be escaped.
     * With the default prefixToStrip, if we have a group of 'com.edmunds.tools' the resulting value expected for use in
     * workspace root and job ids will be 'tools'.
     */
    @Parameter(name = "prefixToStrip", property = "prefixToStrip", defaultValue = "com\\.edmunds\\.")
    protected String prefixToStrip;

    /**
     * Whether you want to upsert a single databricks job.
     */
    @Parameter(name = "singleJob", property = "singleJob")
    protected String singleJob;

    private DatabricksServiceFactory databricksServiceFactory;

    protected DatabricksServiceFactory getDatabricksServiceFactory() {

        if (databricksServiceFactory == null) {
            loadPropertiesFromSystemEnvironment();
            if (token != null) {
                return DatabricksServiceFactory
                    .Builder
                    .createTokenAuthentication(token, host)
                    .withSoTimeout(1000 * 60 * 30)
                    .build();
            } else {
                throw new IllegalArgumentException("Must either specify user/password or token!");
            }
        }
        return databricksServiceFactory;
    }

    /**
     * NOTE - only for unit testing.
     *
     * @param databricksServiceFactory - the mock factory to use
     */
    void setDatabricksServiceFactory(DatabricksServiceFactory databricksServiceFactory) {
        this.databricksServiceFactory = databricksServiceFactory;
    }

    private void loadPropertiesFromSystemEnvironment() {
        String envUrl = System.getenv(DB_URL);
        if (isBlank(host) && isNotBlank(envUrl)) {
            this.host = envUrl;
        }
        String envToken = System.getenv(DB_TOKEN);
        if (isBlank(token) && isNotBlank(envToken)) {
            this.token = envToken;
        }
    }

    /**
     * NOTE - only for unit testing.
     */
    void setEnvironment(String environment) {
        this.environment = environment;
    }

    /**
     * NOTE - only for unit testing.
     */
    void setSingleJob(String singleJob) {
        this.singleJob = singleJob;
    }

    /**
     * NOTE - only for unit testing.
     */
    void setProject(MavenProject project) {
        this.project = project;
    }

    protected void validateRepoProperties() throws MojoExecutionException {
        if (!ALLOWED_REPO_TYPES.contains(databricksRepoType)) {
            throw new MojoExecutionException("Corrupted parameter: ${databricksRepoType}");
        }
        if (StringUtils.isBlank(databricksRepo)) {
            throw new MojoExecutionException("Missing mandatory parameter: ${databricksRepo}");
        }
        if (StringUtils.isBlank(databricksRepoKey)) {
            throw new MojoExecutionException("Missing mandatory parameter: ${databricksRepoKey}");
        }
    }

    String createDeployedArtifactPath() throws MojoExecutionException {
        //TODO if we want databricksRepo to be specified via system properties, this is where it could happen.
        validateRepoProperties();
        String modifiedDatabricksRepoType = databricksRepoType + "://";
        String modifiedDatabricksRepo = databricksRepo;
        String modifiedDatabricksRepoKey = databricksRepoKey;
        if (databricksRepo.endsWith("/")) {
            modifiedDatabricksRepo = databricksRepo.substring(0, databricksRepo.length() - 1);
        }
        if (databricksRepoKey.startsWith("/")) {
            modifiedDatabricksRepoKey = databricksRepoKey.substring(1);
        }
        return String.format("%s%s/%s", modifiedDatabricksRepoType, modifiedDatabricksRepo, modifiedDatabricksRepoKey);
    }
}
