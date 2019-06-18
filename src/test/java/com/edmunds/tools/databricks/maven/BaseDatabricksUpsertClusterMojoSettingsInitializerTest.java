package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.ClusterSettingsDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Tests for @{@link BaseDatabricksUpsertClusterMojoSettingsInitializer}.
 */
public class BaseDatabricksUpsertClusterMojoSettingsInitializerTest extends DatabricksMavenPluginTestHarness {

    private SettingsUtils<ClusterSettingsDTO> settingsUtils;
    private EnvironmentDTOSupplier environmentDTOSupplier;
    private SettingsInitializer<ClusterSettingsDTO> settingsInitializer;

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
        UpsertClusterMojo underTest = getNoOverridesMojo("upsert-cluster");
        settingsUtils = underTest.getSettingsUtils();
        environmentDTOSupplier = underTest.getEnvironmentDTOSupplier();
        settingsInitializer = underTest.getSettingsInitializer();
    }

    @Test
    public void testFillInDefaults() throws Exception {
        ClusterSettingsDTO defaultSettingsDTO = settingsUtils.defaultSettingsDTO();
        ClusterSettingsDTO targetDTO = new ClusterSettingsDTO();

        settingsInitializer.fillInDefaults(targetDTO, defaultSettingsDTO, environmentDTOSupplier.get());

        assertEquals(targetDTO.getClusterName(), "unit-test-group/unit-test-artifact");

        assertEquals(targetDTO.getNumWorkers(), defaultSettingsDTO.getNumWorkers());
        assertEquals(targetDTO.getArtifactPaths(), Collections.emptyList());
        assertEquals(targetDTO.getAutoTerminationMinutes(), defaultSettingsDTO.getAutoTerminationMinutes());
        assertEquals(targetDTO.getAwsAttributes(), defaultSettingsDTO.getAwsAttributes());
        assertNull(targetDTO.getClusterLogConf());
        assertNull(targetDTO.getCustomTags());
        assertEquals(targetDTO.getDriverNodeTypeId(), defaultSettingsDTO.getDriverNodeTypeId());
        assertEquals(targetDTO.getNodeTypeId(), defaultSettingsDTO.getNodeTypeId());
        assertEquals(targetDTO.getSparkConf(), defaultSettingsDTO.getSparkConf());
        assertEquals(targetDTO.getSparkEnvVars(), defaultSettingsDTO.getSparkEnvVars());
        assertEquals(targetDTO.getSparkVersion(), defaultSettingsDTO.getSparkVersion());
        assertNull(targetDTO.getSshPublicKeys());
    }

    @Test(expectedExceptions = MojoExecutionException.class,
            expectedExceptionsMessageRegExp = "REQUIRED FIELD \\[cluster_name\\] was empty. VALIDATION FAILED.")
    public void testValidate_whenNoClusterName_exception() throws Exception {
        ClusterSettingsDTO targetDTO = new ClusterSettingsDTO();

        settingsInitializer.validate(targetDTO, environmentDTOSupplier.get());
    }

    @Test
    public void testValidate_whenClusterNameFilled_noException() throws Exception {
        ClusterSettingsDTO targetDTO = new ClusterSettingsDTO();
        targetDTO.setClusterName("my-cluster-name");

        settingsInitializer.validate(targetDTO, environmentDTOSupplier.get());
    }

}