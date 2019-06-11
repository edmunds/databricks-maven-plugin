package com.edmunds.tools.databricks.maven;

import com.edmunds.rest.databricks.DTO.JobEmailNotificationsDTO;
import com.edmunds.rest.databricks.DTO.JobSettingsDTO;
import com.edmunds.rest.databricks.DTO.NewClusterDTO;
import com.edmunds.tools.databricks.maven.model.JobEnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.SettingsInitializer;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import com.edmunds.tools.databricks.maven.validation.ValidationUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.util.Map;

import static com.edmunds.tools.databricks.maven.BaseDatabricksJobMojoSettingsInitializer.DELTA_TAG;
import static com.edmunds.tools.databricks.maven.BaseDatabricksJobMojoSettingsInitializer.TEAM_TAG;

/**
 * Tests for @{@link BaseDatabricksJobMojoSettingsInitializer}.
 */
public class BaseDatabricksJobMojoSettingsInitializerTest extends DatabricksMavenPluginTestHarness {

    private SettingsUtils<JobEnvironmentDTO, JobSettingsDTO> settingsUtils;
    private SettingsInitializer<JobEnvironmentDTO, JobSettingsDTO> settingsInitializer;

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
        UpsertJobMojo underTest = getNoOverridesMojo("upsert-job");
        settingsUtils = underTest.getSettingsUtils();
        settingsInitializer = underTest.getSettingsInitializer();
    }

    @Test
    public void testDefaultIfNull_JobSettingsDTO() throws Exception {
        JobSettingsDTO defaultSettingsDTO = settingsUtils.defaultSettingsDTO();
        JobSettingsDTO targetDTO = new JobSettingsDTO();

        settingsInitializer.fillInDefaults(targetDTO, defaultSettingsDTO, settingsUtils.getEnvironmentDTO());

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

        settingsInitializer.fillInDefaults(targetDTO, settingsUtils.defaultSettingsDTO(), settingsUtils.getEnvironmentDTO());

        assertEquals(targetDTO.getNewCluster().getCustomTags().get(TEAM_TAG), "unit-test-group");
    }

    @Test
    public void validateInstanceTags_whenWrongTeamTag_fillsInDefault() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(TEAM_TAG, "overrideTeam");

        JobSettingsDTO targetDTO = createTestJobSettings(tags);

        settingsInitializer.fillInDefaults(targetDTO, settingsUtils.defaultSettingsDTO(), settingsUtils.getEnvironmentDTO());

        assertEquals(targetDTO.getNewCluster().getCustomTags().get(TEAM_TAG), "overrideTeam");
    }

    @Test
    public void validateInstanceTags_whenMissingDeltaTag_fillsInDefault() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(TEAM_TAG, "myteam");

        JobSettingsDTO targetDTO = makeDeltaEnabled(createTestJobSettings(tags));

        settingsInitializer.fillInDefaults(targetDTO, settingsUtils.defaultSettingsDTO(), settingsUtils.getEnvironmentDTO());

        assertEquals(targetDTO.getNewCluster().getCustomTags().get(DELTA_TAG), "true");
    }

    @Test
    public void validateInstanceTags_whenDeltaTag_noException() throws Exception {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(TEAM_TAG, "myteam");
        tags.put(DELTA_TAG, "true");

        JobSettingsDTO targetDTO = makeDeltaEnabled(createTestJobSettings(tags));

        settingsInitializer.fillInDefaults(targetDTO, settingsUtils.defaultSettingsDTO(), settingsUtils.getEnvironmentDTO());

        assertEquals(targetDTO.getNewCluster().getCustomTags().get(DELTA_TAG), "true");
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp =
            "REQUIRED FIELD \\[email_notifications.on_failure\\] was empty. VALIDATION FAILED.")
    public void testValidate_whenNoEmailOnFailure_exception() throws Exception {
        JobSettingsDTO targetDTO = createTestJobSettings(Maps.newHashMap());

        settingsInitializer.validate(targetDTO, settingsUtils.getEnvironmentDTO());
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp =
            "JOB NAME VALIDATION FAILED \\[ILLEGAL FORMAT\\]:\n" +
                    "Expected: \\[groupId/artifactId/...\\] but found: \\[1\\] parts.")
    public void testValidate_whenIncorrectJobName_exception() throws Exception {
        JobSettingsDTO targetDTO = createTestJobSettings(Maps.newHashMap());
        JobEmailNotificationsDTO jobEmailNotificationsDTO = new JobEmailNotificationsDTO();
        jobEmailNotificationsDTO.setOnFailure(ArrayUtils.toArray("some@email.com"));
        targetDTO.setEmailNotifications(jobEmailNotificationsDTO);
        targetDTO.setName("job-name");

        settingsInitializer.validate(targetDTO, settingsUtils.getEnvironmentDTO());
    }

    @Test
    public void testValidate_whenEmailAndJobNameFilled_noException() throws MojoExecutionException {
        JobSettingsDTO targetDTO = createTestJobSettings(Maps.newHashMap());
        JobEmailNotificationsDTO jobEmailNotificationsDTO = new JobEmailNotificationsDTO();
        jobEmailNotificationsDTO.setOnFailure(ArrayUtils.toArray("some@email.com"));
        targetDTO.setEmailNotifications(jobEmailNotificationsDTO);
        targetDTO.setName("unit-test-group/unit-test-artifact/job-name");

        settingsInitializer.validate(targetDTO, settingsUtils.getEnvironmentDTO());
    }

    private JobSettingsDTO createTestJobSettings(Map<String, String> tags) {
        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        NewClusterDTO newClusterDTO = new NewClusterDTO();
        newClusterDTO.setCustomTags(tags);
        jobSettingsDTO.setNewCluster(newClusterDTO);
        return jobSettingsDTO;
    }

    private JobSettingsDTO makeDeltaEnabled(JobSettingsDTO jobSettingsDTO) {
        Map<String, String> sparkConf = Maps.newHashMap();
        sparkConf.put(ValidationUtil.DELTA_ENABLED, "true");
        jobSettingsDTO.getNewCluster().setSparkConf(sparkConf);
        return jobSettingsDTO;
    }

}