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

import com.edmunds.rest.databricks.DatabricksServiceFactory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;


import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The base databricks mojo.
 */
public abstract class BaseDatabricksMojo extends AbstractMojo {

    private static final String DB_USER = "DB_USER";
    private static final String DB_PASSWORD = "DB_PASSWORD";
    private static final String DB_URL = "DB_URL";
    private static final String DB_TOKEN = "DB_TOKEN";

    @Parameter(defaultValue = "${project}", readonly = true)
    protected MavenProject project;

    /**
     * The environment name. Is used in freemarker templating for conditional job settings.
     */
    @Parameter(property = "environment")
    protected String environment;

    @Parameter(property = "host")
    protected String host;

    /**
     * NOTE: user+password authentication will take precedence over token based authentication if both are provided.
     */
    @Parameter(property = "token")
    protected String token;

    @Parameter(property = "user")
    protected String user;

    @Parameter(property = "password")
    protected String password;

    @Parameter(defaultValue = "true", property = "validate")
    protected boolean validate;


    private DatabricksServiceFactory databricksServiceFactory;

    protected DatabricksServiceFactory getDatabricksServiceFactory() {

        if (databricksServiceFactory == null) {

            loadPropertiesFromSystemEnvironment();

            if (user != null && password != null) {
                return DatabricksServiceFactory
                        .Builder
                        .createServiceFactoryWithUserPasswordAuthentication(user, password, host)
                        .build();
            } else if (token != null) {
                return DatabricksServiceFactory
                        .Builder
                        .createServiceFactoryWithTokenAuthentication(token, host)
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

}
