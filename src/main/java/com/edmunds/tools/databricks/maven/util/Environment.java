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

package com.edmunds.tools.databricks.maven.util;

import com.edmunds.rest.databricks.DatabricksServiceFactory;


public enum Environment {

    PROD(System.getenv("DB_PROD_URL"), System.getenv("DB_PROD_USER"), System.getenv("DB_PROD_PASSWORD")),
    QA(System.getenv("DB_URL"), System.getenv("DB_USER"), System.getenv("DB_PASSWORD"));

    private final String host;
    private final String user;
    private final String password;

    private Environment(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
    }

    public DatabricksServiceFactory getEdmundsDatabricksServiceFactory() {
        return DatabricksServiceFactory
            .Builder
            .createServiceFactoryWithUserPasswordAuthentication(this.user, this.password, this.host).build();
    }

    public String getUniqueEnvironmentName() {
        return this.name();
    }

    public String getHost() {
        return this.host;
    }

    public String getUser() {
        return this.user;
    }

    public String getPassword() {
        return this.password;
    }
}
