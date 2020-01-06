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

package com.edmunds.tools.databricks.maven.validation;

import static com.edmunds.tools.databricks.maven.model.EnvironmentDTO.stripCompanyPackage;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * The plugin validation utility - only needed for the common path validation method.
 */
public class ValidationUtil {

    static final String GROUP_ID = "groupId";
    static final String ARTIFACT_ID = "artifactId";

    /**
     * Validate the path that is passed in. Note, path can also be a job name.
     * What the validation does:
     * split it by /
     * part[0] == groupId
     * part[1] == artifactId
     * part[...] - don't check, doesn't matter, as user defined
     *
     * @param path - the path to validate, can be a job name as well
     * @param groupId - the projects groupId
     * @param artifactId - the projects artifactId
     * @throws MojoExecutionException - thrown when the job name does not conform
     */
    public static void validatePath(String path, String groupId, String artifactId, String prefixToStrip)
        throws MojoExecutionException {

        path = path.replace('\\', '/');
        //workspace path starts at root, job names do not have '/' prefix
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        String[] jobNameParts = path.split("/");
        if (jobNameParts.length < 2) {
            throw new MojoExecutionException(String.format("JOB NAME VALIDATION FAILED [ILLEGAL FORMAT]:%n"
                + "Expected: [groupId/artifactId/...] but found: [%s] parts.", jobNameParts.length));
        }

        validatePart(jobNameParts[0], getStrippedGroupId(groupId, artifactId, prefixToStrip), GROUP_ID);
        validatePart(jobNameParts[1], getArtifactId(artifactId), ARTIFACT_ID);
    }

    private static String getStrippedGroupId(String groupId, String artifactId, String prefixToStrip) {
        return stripCompanyPackage(prefixToStrip, isMaven(artifactId) ? groupId : getValue(GROUP_ID));
    }

    private static String getArtifactId(String artifactId) {
        return isMaven(artifactId) ? artifactId : getValue(ARTIFACT_ID);
    }

    private static boolean isMaven(String artifactId) {
        return !artifactId.equals("standalone-pom");
    }

    private static String getValue(String key) {
        return Objects.toString(System.getProperties().get(key), "");
    }

    private static void validatePart(String jobNamePart, String expectedValue, String keyName)
        throws MojoExecutionException {
        if (isBlank(expectedValue)) {
            throw new MojoExecutionException(
                String.format(
                    "JOB NAME VALIDATION FAILED [REQUIRED PROPERTY]: '%s' is not set.%n"
                        + "Please set it in your POM file.%n"
                        + "ex: <%s>foo</%s>%n"
                        + "Or, pass it as a program argument.%n"
                        + "ex: -D%s=foo", keyName, keyName, keyName, keyName));
        }

        if (!StringUtils.equals(expectedValue, jobNamePart)) {
            throw new MojoExecutionException(String.format("JOB NAME VALIDATION FAILED [ILLEGAL VALUE]:%n"
                + "Expected: [%s] but found: [%s]", expectedValue, jobNamePart));
        }
    }

}