package com.edmunds.tools.databricks.maven;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PrepareLibraryResourcesTest extends BaseDatabricksMojoTest {

    private PrepareLibraryResources underTest = new PrepareLibraryResources();

    @BeforeMethod
    public void init() throws Exception {
        super.init();

        underTest.setDatabricksServiceFactory(databricksServiceFactory);
        underTest.setProject(project);
        underTest.setVersion("1.0");
        underTest.setDbfsRoot("s3://bucket-name");
    }

    @Test
    public void testCreateArtifactPath_default() throws Exception {
        assertThat(underTest.createArtifactPath(), is("s3://bucket-name/artifacts/com.edmunds" +
                ".test/mycoolartifact/1.0/mycoolartifact-1.0.jar"));
    }

    @Test
    public void testCreateArtifactPath_usingSpecifiedVersion() throws Exception {
        underTest.setVersion("2.3.4");
        assertThat(underTest.createArtifactPath(), is("s3://bucket-name/artifacts/com.edmunds" +
                ".test/mycoolartifact/2.3.4/mycoolartifact-2.3.4.jar"));
    }
}