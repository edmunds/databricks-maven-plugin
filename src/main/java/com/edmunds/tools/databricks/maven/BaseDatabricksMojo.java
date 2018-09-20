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
import com.edmunds.tools.databricks.maven.util.Environment;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * The base databricks mojo.
 */
public abstract class BaseDatabricksMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true)
    protected MavenProject project;

    @Parameter(defaultValue = "QA", property = "environment", required = true)
    protected Environment environment;

    @Parameter(defaultValue = "true", property = "validate")
    protected boolean validate;



    private DatabricksServiceFactory databricksServiceFactory;

    protected DatabricksServiceFactory getDatabricksServiceFactory() {

        getLog().debug(String.format("connecting to databricks environment: [%s]", environment));

        if (databricksServiceFactory == null) {
            databricksServiceFactory = environment.getEdmundsDatabricksServiceFactory();
        }
        return databricksServiceFactory;
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
    void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    /**
     * NOTE - only for unit testing!
     */
    void setProject(MavenProject project) {
        this.project = project;
    }

}
