package com.edmunds.tools.databricks.maven;

import static com.edmunds.tools.databricks.maven.BaseDatabricksJobMojoSettingsInitializer.TEAM_TAG;

import com.edmunds.rest.databricks.DTO.jobs.JobEmailNotificationsDTO;
import com.edmunds.rest.databricks.DTO.jobs.JobSettingsDTO;
import com.edmunds.rest.databricks.DTO.jobs.NewClusterDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Tests for @{@link BaseDatabricksJobMojoSettingsInitializer}.
 */
public class BaseDatabricksJobMojoSettingsInitializerTest extends DatabricksMavenPluginTestHarness {

    private SettingsUtils<JobSettingsDTO> settingsUtils;
    private EnvironmentDTOSupplier environmentDTOSupplier;
    private SettingsInitializer<JobSettingsDTO> settingsInitializer;

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
        UpsertJobMojo underTest = getNoOverridesMojo("upsert-job");
        settingsUtils = underTest.getSettingsUtils();
        environmentDTOSupplier = underTest.getEnvironmentDTOSupplier();
        settingsInitializer = underTest.getSettingsInitializer();
    }

    @Test
    public void testDefaultIfNull_JobSettingsDTO() throws Exception {
        JobSettingsDTO defaultSettingsDTO = settingsUtils.defaultSettingsDTO();
        JobSettingsDTO targetDTO = new JobSettingsDTO();

        settingsInitializer.fillInDefaults(targetDTO, defaultSettingsDTO, environmentDTOSupplier.get());

        assertEquals(targetDTO.getName(), "unit-test-group/unit-test-artifact");

        NewClusterDTO defaultCluster = defaultSettingsDTO.getNewCluster();
        NewClusterDTO userCluster = targetDTO.getNewCluster();
        assertEquals(userCluster.getSparkVersion(), defaultCluster.getSparkVersion());
        assertEquals(userCluster.getNodeTypeId(), defaultCluster.getNodeTypeId());
        assertEquals(userCluster.getNumWorkers(), defaultCluster.getNumWorkers());
        assertEquals(userCluster.getAwsAttributes().getEbsVolumeSize(),
            defaultCluster.getAwsAttributes().getEbsVolumeSize());

        assertEquals(targetDTO.getLibraries()[0].getJar(), defaultSettingsDTO.getLibraries()[0].getJar());

        assertEquals(targetDTO.getMaxConcurrentRuns(), defaultSettingsDTO.getMaxConcurrentRuns());
        assertEquals(targetDTO.getMaxRetries(), defaultSettingsDTO.getMaxRetries());
        assertEquals(targetDTO.getTimeoutSeconds(), defaultSettingsDTO.getTimeoutSeconds());
    }

    @Test
    public void validateInstanceTags_whenNull_fillsInDefault() throws Exception {
        JobSettingsDTO targetDTO = createTestJobSettings(null);

        settingsInitializer.fillInDefaults(targetDTO, settingsUtils.defaultSettingsDTO(), environmentDTOSupplier.get());

        assertEquals(targetDTO.getNewCluster().getCustomTags().get(TEAM_TAG), "unit-test-group");
    }

    @Test
    public void validateInstanceTags_whenWrongTeamTag_fillsInDefault() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(TEAM_TAG, "overrideTeam");

        JobSettingsDTO targetDTO = createTestJobSettings(tags);

        settingsInitializer.fillInDefaults(targetDTO, settingsUtils.defaultSettingsDTO(), environmentDTOSupplier.get());

        assertEquals(targetDTO.getNewCluster().getCustomTags().get(TEAM_TAG), "overrideTeam");
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp =
        "REQUIRED FIELD \\[email_notifications.on_failure\\] was empty. VALIDATION FAILED.")
    public void testValidate_whenNoEmailOnFailure_exception() throws Exception {
        JobSettingsDTO targetDTO = createTestJobSettings(Maps.newHashMap());

        settingsInitializer.validate(targetDTO, environmentDTOSupplier.get());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp =
        "JOB NAME VALIDATION FAILED \\[ILLEGAL FORMAT\\]:.*" +
            "Expected: \\[groupId/artifactId/...\\] but found: \\[1\\] parts.")
    public void testValidate_whenIncorrectJobName_exception() throws Exception {
        JobSettingsDTO targetDTO = createTestJobSettings(Maps.newHashMap());
        JobEmailNotificationsDTO jobEmailNotificationsDTO = new JobEmailNotificationsDTO();
        jobEmailNotificationsDTO.setOnFailure(ArrayUtils.toArray("some@email.com"));
        targetDTO.setEmailNotifications(jobEmailNotificationsDTO);
        targetDTO.setName("job-name");

        settingsInitializer.validate(targetDTO, environmentDTOSupplier.get());
    }

    @Test
    public void testValidate_whenEmailAndJobNameFilled_noException() throws MojoExecutionException {
        JobSettingsDTO targetDTO = createTestJobSettings(Maps.newHashMap());
        JobEmailNotificationsDTO jobEmailNotificationsDTO = new JobEmailNotificationsDTO();
        jobEmailNotificationsDTO.setOnFailure(ArrayUtils.toArray("some@email.com"));
        targetDTO.setEmailNotifications(jobEmailNotificationsDTO);
        targetDTO.setName("unit-test-group/unit-test-artifact/job-name");

        settingsInitializer.validate(targetDTO, environmentDTOSupplier.get());
    }

    private JobSettingsDTO createTestJobSettings(Map<String, String> tags) {
        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        NewClusterDTO newClusterDTO = new NewClusterDTO();
        newClusterDTO.setCustomTags(tags);
        jobSettingsDTO.setNewCluster(newClusterDTO);
        return jobSettingsDTO;
    }

}