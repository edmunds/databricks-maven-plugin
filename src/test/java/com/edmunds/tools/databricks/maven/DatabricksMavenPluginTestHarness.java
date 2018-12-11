/*
 *    Copyright 2018 Edmunds.com, Inc.
 *
 *        Licensed under the Apache License, Version 2.0 (the "License");
 *        you may not use this file except in compliance with the License.
 *        You may obtain a copy of the License at
 *
 *            http://www.apache.org/licenses/LICENSE-2.0
 *
 *        Unless required by applicable law or agreed to in writing, software
 *        distributed under the License is distributed on an "AS IS" BASIS,
 *        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *        See the License for the specific language governing permissions and
 *        limitations under the License.
 */

package com.edmunds.tools.databricks.maven;

import org.apache.maven.plugin.Mojo;
import org.testng.annotations.BeforeClass;

import java.io.File;

public abstract class DatabricksMavenPluginTestHarness extends BetterAbstractMojoTestCase {

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
    }

    public <T extends Mojo> T getNoOverridesMojo(String goal) throws Exception {
        File testPom = new File(getBasedir(),
            "src/test/resources/unit/basic-test/test-no-overrides-plugin-config" +
                ".xml");
        return (T) lookupConfiguredMojo(testPom, goal);
    }

    public <T extends Mojo> T getMissingMandatoryMojo(String goal) throws Exception {
        File testPom = new File(getBasedir(),
            "src/test/resources/unit/basic-test/test-missing-mandatory-plugin-config" +
                ".xml");
        return (T) lookupConfiguredMojo(testPom, goal);
    }

    public <T extends Mojo> T getOverridesMojo(String goal) throws Exception {
        File testPom = new File(getBasedir(),
            "src/test/resources/unit/basic-test/test-overrides-plugin-config" +
                ".xml");
        return (T) lookupConfiguredMojo(testPom, goal);
    }
}
