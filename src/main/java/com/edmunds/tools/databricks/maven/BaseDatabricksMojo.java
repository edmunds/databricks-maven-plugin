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

import com.edmunds.rest.databricks.DTO.NewClusterDTO;
import com.edmunds.rest.databricks.DatabricksServiceFactory;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;


import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The base databricks mojo.
 */
public abstract class BaseDatabricksMojo extends AbstractMojo {

    public static final String DEFAULT_DBFS_ROOT_FORMAT = "s3://";

    private static final String DB_USER = "DB_USER";
    private static final String DB_PASSWORD = "DB_PASSWORD";
    private static final String DB_URL = "DB_URL";
    private static final String DB_TOKEN = "DB_TOKEN";

    public static final String GROUP_ID = "groupId";
    public static final String ARTIFACT_ID = "artifactId";
    public static final String DELTA_ENABLED = "spark.databricks.delta.preview.enabled";

    @Parameter(defaultValue = "${project}", readonly = true)
    protected MavenProject project;

    @Parameter(name = "prefixToStrip", property = "prefixToStrip", defaultValue = "com\\.edmunds\\.")
    protected String prefixToStrip;

    //TODO validate even with required=true? How does that play with the env properties
    /**
     * The repo location on s3 that you want to upload your jar to.
     * At the very least this should be an s3 bucket name like "my-bucket"
     * BUT you can also specify a common prefix here in addition to a bucket,
     * for example:
     * "my-bucket/artifacts"
     * <p>
     * This property is not required due to the no project option.
     * <p>
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
            defaultValue = "${project.groupId}/${project.artifactId}/${project.version}/${project.build.finalName}" +
                    ".${project.packaging}")
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
     * This property can be picked up via an environment property!
     * DB_URL
     */
    @Parameter(name = "host", property = "host")
    protected String host;

    /**
     * This property can be picked up via an environment property!
     * DB_TOKEN
     * NOTE: user+password authentication will take precedence over token based authentication if both are provided.
     */
    @Parameter(name = "token", property = "token")
    protected String token;

    /**
     * This property can be picked up via an environment property!
     * DB_USER
     */
    @Parameter(name = "user", property = "user")
    protected String user;

    /**
     * This property can be picked up via an environment property!
     * DB_PASSWORD
     */
    @Parameter(name = "password", property = "password")
    protected String password;

    /**
     * Whether or not you want to validate the databricks job settings file.
     */
    @Parameter(name = "validate", defaultValue = "true", property = "validate")
    protected boolean validate;


    private DatabricksServiceFactory databricksServiceFactory;

    protected DatabricksServiceFactory getDatabricksServiceFactory() {

        if (databricksServiceFactory == null) {

            loadPropertiesFromSystemEnvironment();

            if (user != null && password != null) {
                return DatabricksServiceFactory
                        .Builder
                        .createUserPasswordAuthentication(user, password, host)
                        .build();
            } else if (token != null) {
                return DatabricksServiceFactory
                        .Builder
                        .createTokenAuthentication(token, host)
                        .build();
            } else {
                throw new IllegalArgumentException("Must either specify user/password or token!");
            }
        }
        return databricksServiceFactory;
    }

    private void loadPropertiesFromSystemEnvironment() {
        String envUser = System.getenv(DB_USER);
        if (isBlank(user) && isNotBlank(envUser)) {
            this.user = envUser;
        }
        String envPassword = System.getenv(DB_PASSWORD);
        if (isBlank(password) && isNotBlank(envPassword)) {
            this.password = envPassword;
        }
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
     * NOTE - only for unit testing!
     *
     * @param databricksServiceFactory - the mock factory to use
     */
    void setDatabricksServiceFactory(DatabricksServiceFactory databricksServiceFactory) {
        this.databricksServiceFactory = databricksServiceFactory;
    }

    /**
     * NOTE - only for unit testing!
     */
    void setEnvironment(String environment) {
        this.environment = environment;
    }

    /**
     * NOTE - only for unit testing!
     */
    void setProject(MavenProject project) {
        this.project = project;
    }

    /**
     * NOTE - only for unit testing!
     */
    void setPrefixToStrip(String prefixToStrip) {
        this.prefixToStrip = prefixToStrip;
    }

    protected void validateRepoProperties() throws MojoExecutionException {
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
        String modifiedDatabricksRepo = databricksRepo;
        String modifiedDatabricksRepoKey = databricksRepoKey;
        if (databricksRepo.endsWith("/")) {
            modifiedDatabricksRepo = databricksRepo.substring(0, databricksRepo.length() - 1);
        }
        if (databricksRepoKey.startsWith("/")) {
            modifiedDatabricksRepoKey = databricksRepoKey.substring(1, databricksRepoKey.length());
        }
        return String.format("%s%s/%s", DEFAULT_DBFS_ROOT_FORMAT, modifiedDatabricksRepo, modifiedDatabricksRepoKey);
    }

    public String stripPrefix(String path) {
        return path.replaceAll(prefixToStrip, "");
    }

    public String getStrippedGroupId(String groupId, String artifactId) {
        return stripPrefix(isMaven(artifactId) ? groupId : getValue(GROUP_ID));
    }

    public String getArtifactId(String artifactId) {
        return isMaven(artifactId) ? artifactId : getValue(ARTIFACT_ID);
    }

    private boolean isMaven(String artifactId) {
        return !artifactId.equals("standalone-pom");
    }

    private String getValue(String key) {
        return Objects.toString(System.getProperties().get(key), "");
    }

    /**
     * Validate the path that is passed in. Note, path can also be a job name.
     * <p>
     * What the validation does:
     * split it by /
     * part[0] == groupId
     * part[1] == artifactId
     * part[...] - don't check, doesn't matter, as user defined
     *
     * @param path       - the path to validate, can be a job name as well
     * @param groupId    - the projects groupId
     * @param artifactId - the projects artifactId
     * @throws MojoExecutionException - thrown when the job name does not conform
     */
    public void validatePath(String path, String groupId, String artifactId) throws MojoExecutionException {

        //workspace path starts at root, job names do not have '/' prefix
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        String[] jobNameParts = path.split("/");
        if (jobNameParts.length < 2) {
            throw new MojoExecutionException(String.format("JOB NAME VALIDATION FAILED [ILLEGAL FORMAT]:\n" +
                    "Expected: [groupId/artifactId/...] but found: [%s] parts.", jobNameParts.length));
        }

        validatePart(jobNameParts[0], getStrippedGroupId(groupId, artifactId), GROUP_ID);
        validatePart(jobNameParts[1], getArtifactId(artifactId), ARTIFACT_ID);
    }

    private void validatePart(String jobNamePart, String expectedValue, String keyName) throws MojoExecutionException {
        if (isBlank(expectedValue)) {
            throw new MojoExecutionException(
                    String.format(
                            "JOB NAME VALIDATION FAILED [REQUIRED PROPERTY]: '%s' is not set.\n" +
                                    "Please set it in your POM file.\n" +
                                    "ex: <%s>foo</%s>\n" +
                                    "Or, pass it as a program argument.\n" +
                                    "ex: -D%s=foo", keyName, keyName, keyName, keyName));
        }

        if (!StringUtils.equals(expectedValue, jobNamePart)) {
            throw new MojoExecutionException(String.format("JOB NAME VALIDATION FAILED [ILLEGAL VALUE]:\n" +
                    "Expected: [%s] but found: [%s]", expectedValue, jobNamePart));
        }
    }


    public boolean isDeltaEnabled(NewClusterDTO newClusterDTO) {
        Map<String, String> confMap = newClusterDTO.getSparkConf();
        if (confMap == null) {
            return false;
        }
        String deltaValue = confMap.get(DELTA_ENABLED);
        if (deltaValue == null || !deltaValue.equals("true")) {
            return false;
        }
        return true;
    }
}
