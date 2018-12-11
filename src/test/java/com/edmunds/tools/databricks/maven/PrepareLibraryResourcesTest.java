package com.edmunds.tools.databricks.maven;

import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

//Requires building beforehand??
public class PrepareLibraryResourcesTest extends DatabricksMavenPluginTestHarness {

    private final String GOAL = "prepare-library-resources";

    @Test
    public void testCreateArtifactPath_default() throws Exception {
        PrepareLibraryResources underTest = (PrepareLibraryResources) getNoOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/artifacts/unit-test-group" +
                "/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar"));
        //TODO actually test the execute here
        underTest.execute();
    }

    @Test
    public void testCreateArtifactPath_failsWhenMissingMandatoryFields() throws Exception {
        PrepareLibraryResources underTest = (PrepareLibraryResources) getMissingMandatoryMojo(GOAL);
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            return;
        }
        fail();
    }

    @Test
    public void testCreateArtifactPath_succeedsWithOverrides() throws Exception {
        PrepareLibraryResources underTest = (PrepareLibraryResources) getOverridesMojo(GOAL);
        assertThat(underTest.createArtifactPath(), is("s3://my-bucket/my-destination"));
    }
}