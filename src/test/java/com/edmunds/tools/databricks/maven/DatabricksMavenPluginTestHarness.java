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

import static org.powermock.api.mockito.PowerMockito.when;

import com.edmunds.rest.databricks.DatabricksServiceFactory;
import com.edmunds.rest.databricks.service.ClusterService;
import com.edmunds.rest.databricks.service.DbfsService;
import com.edmunds.rest.databricks.service.JobService;
import com.edmunds.rest.databricks.service.LibraryService;
import com.edmunds.rest.databricks.service.WorkspaceService;
import java.io.File;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


/**
 * Any test that extends this class requires that the plugin descriptor is available in order for this test to run.
 * If you do mvn clean install -DskipTests, you should then be able to run tests that depend on this class.
 * Most changes do not require rebuilding to test.
 * Unfortunately, some changes to MOJO will require that you regenerate this plugin descriptor.
 */
public abstract class DatabricksMavenPluginTestHarness extends BetterAbstractMojoTestCase {

    @Mock
    protected DatabricksServiceFactory databricksServiceFactory;
    @Mock
    protected ClusterService clusterService;
    @Mock
    protected LibraryService libraryService;
    @Mock
    protected WorkspaceService workspaceService;
    @Mock
    protected JobService jobService;
    @Mock
    protected DbfsService dbfsService;

    public void setUp() throws Exception {
        super.setUp();
    }

    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(databricksServiceFactory.getClusterService()).thenReturn(clusterService);
        when(databricksServiceFactory.getLibraryService()).thenReturn(libraryService);
        when(databricksServiceFactory.getWorkspaceService()).thenReturn(workspaceService);
        when(databricksServiceFactory.getJobService()).thenReturn(jobService);
        when(databricksServiceFactory.getDbfsService()).thenReturn(dbfsService);
    }

    public <T extends BaseDatabricksMojo> T getNoOverridesMojo(String goal) throws Exception {
        File testPom = new File(getBasedir(),
            String.format("src/test/resources/unit/basic-test/%s/test-no-overrides-plugin-config" +
                ".xml", goal));
        T ret = (T) lookupConfiguredMojo(testPom, goal);
        ret.setDatabricksServiceFactory(databricksServiceFactory);
        return ret;
    }

    public <T extends BaseDatabricksMojo> T getNoOverridesMojo(String goal, String variation) throws Exception {
        File testPom = new File(getBasedir(),
            String.format("src/test/resources/unit/basic-test/%s/test-no-overrides-plugin-config%s" +
                ".xml", goal, variation));
        T ret = (T) lookupConfiguredMojo(testPom, goal);
        ret.setDatabricksServiceFactory(databricksServiceFactory);
        return ret;
    }

    public <T extends BaseDatabricksMojo> T getMissingMandatoryMojo(String goal) throws Exception {
        File testPom = new File(getBasedir(),
            String.format("src/test/resources/unit/basic-test/%s/test-missing-mandatory-plugin-config" +
                ".xml", goal));

        T ret = (T) lookupConfiguredMojo(testPom, goal);
        ret.setDatabricksServiceFactory(databricksServiceFactory);
        return ret;
    }

    public <T extends BaseDatabricksMojo> T getOverridesMojo(String goal) throws Exception {
        File testPom = new File(getBasedir(),
            String.format("src/test/resources/unit/basic-test/%s/test-overrides-plugin-config" +
                ".xml", goal));
        T ret = (T) lookupConfiguredMojo(testPom, goal);
        ret.setDatabricksServiceFactory(databricksServiceFactory);
        return ret;
    }

    public <T extends BaseDatabricksMojo> T getOverridesMojo(String goal, String variation) throws Exception {
        File testPom = new File(getBasedir(),
            String.format("src/test/resources/unit/basic-test/%s/test-overrides-plugin-config%s" +
                ".xml", goal, variation));
        T ret = (T) lookupConfiguredMojo(testPom, goal);
        ret.setDatabricksServiceFactory(databricksServiceFactory);
        return ret;
    }
}
