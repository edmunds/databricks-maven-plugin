package com.edmunds.tools.databricks.maven;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class DeployToS3MojoTest extends DatabricksMavenPluginTestHarness {

    private static final String GOAL = "deploy-to-s3";
    @Mock
    AmazonS3Client s3Client;
    private DeployToS3Mojo underTest;

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void testDefaultExecute() throws Exception {
        underTest = getNoOverridesMojo(GOAL);
        underTest.s3Client = s3Client;
        underTest.execute();
    }

    @Test
    public void testMissingProperties() throws Exception {
        underTest = getMissingMandatoryMojo(GOAL);
        underTest.s3Client = s3Client;
        try {
            underTest.execute();
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), containsString("Missing mandatory parameter: ${databricksRepo}"));
            return;
        }
        fail();
    }

    @Test
    public void testOverridesExecute() throws Exception {
        underTest = getOverridesMojo(GOAL);
        underTest.s3Client = s3Client;
        underTest.execute();

        ArgumentCaptor<PutObjectRequest> putRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(s3Client).putObject(putRequestCaptor.capture());
        assertEquals("myBucket", putRequestCaptor.getValue().getBucketName());
        assertEquals("repo/unit-test-group/unit-test-artifact/1.0.0-SNAPSHOT/unit-test-artifact-1.0.0-SNAPSHOT.jar",
                putRequestCaptor.getValue().getKey());
        assertEquals("myFile.csv", putRequestCaptor.getValue().getFile().getName());
    }
}
