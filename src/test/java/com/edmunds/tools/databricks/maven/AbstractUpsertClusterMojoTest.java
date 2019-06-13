package com.edmunds.tools.databricks.maven;

import com.edmunds.rest.databricks.DTO.AutoScaleDTO;
import com.edmunds.rest.databricks.DTO.AwsAttributesDTO;
import com.edmunds.rest.databricks.DTO.ClusterInfoDTO;
import com.edmunds.rest.databricks.DTO.ClusterLibraryStatusesDTO;
import com.edmunds.rest.databricks.DTO.ClusterLogConfDTO;
import com.edmunds.rest.databricks.DTO.ClusterStateDTO;
import com.edmunds.rest.databricks.DTO.ClusterTagDTO;
import com.edmunds.rest.databricks.DTO.LibraryDTO;
import com.edmunds.rest.databricks.DTO.LibraryFullStatusDTO;
import com.edmunds.rest.databricks.request.CreateClusterRequest;
import com.edmunds.rest.databricks.request.EditClusterRequest;
import com.edmunds.tools.databricks.maven.model.ClusterSettingsDTO;
import com.google.common.collect.Sets;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.edmunds.rest.databricks.DTO.AwsAvailabilityDTO.SPOT_WITH_FALLBACK;
import static com.edmunds.rest.databricks.DTO.EbsVolumeTypeDTO.GENERAL_PURPOSE_SSD;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractUpsertClusterMojoTest<T extends UpsertClusterMojo> extends DatabricksMavenPluginTestHarness {

    protected String GOAL;

    protected T underTest;

    protected abstract void setGoal();

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
        setGoal();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    @Test
    public void test_executeWithDefault_createNewCluster() throws Exception {
        underTest = getNoOverridesMojo(GOAL);
        String clusterId = "clusterId";

        when(clusterService.list()).thenReturn(new ClusterInfoDTO[]{});
        ArgumentCaptor<CreateClusterRequest> reqCaptor = ArgumentCaptor.forClass(CreateClusterRequest.class);
        when(clusterService.create(reqCaptor.capture())).thenReturn(clusterId);

        ArgumentCaptor<LibraryDTO[]> libCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);
        doNothing().when(libraryService).install(eq(clusterId), libCaptor.capture());

        underTest.execute();
        Thread.sleep(100);

        verify(clusterService, times(2)).create(reqCaptor.capture());
        assertCreateClusterRequestEquality(reqCaptor, Arrays.asList("my-cluster", "my-cluster-2"));
        verify(clusterService, times(0)).edit(any(EditClusterRequest.class));
        verify(clusterService, times(0)).restart(clusterId);
        verify(clusterService, times(0)).getInfo(clusterId);

        verify(libraryService, times(0)).uninstall(eq(clusterId), any(LibraryDTO[].class));
        verify(libraryService).install(eq(clusterId), libCaptor.capture());
        Set<String> jarPaths = Sets.newHashSet("dbfs:/Libs/jars/app_sdk_0_1_2-345.jar",
                "s3://bucket-name/artifacts/com.company.project/my-artifact-name/1.0.132/my-artifact-name-1.0.132.jar");
        LibraryDTO[] libs = libCaptor.getValue();
        assertTrue(jarPaths.contains(libs[0].getJar()));
        assertTrue(jarPaths.contains(libs[1].getJar()));
    }

    @Test
    public void test_executeWithOverride_upsertCluster() throws Exception {
        underTest = getOverridesMojo(GOAL);
        String clusterId = "clusterId";

        when(clusterService.list()).thenReturn(new ClusterInfoDTO[]{createClusterInfoDTO()});
        ArgumentCaptor<EditClusterRequest> reqCaptor = ArgumentCaptor.forClass(EditClusterRequest.class);
        when(clusterService.getInfo(clusterId)).thenReturn(createClusterInfoDTO());
        when(libraryService.clusterStatus(clusterId)).thenReturn(createClusterLibraryStatusesDTO());
        ArgumentCaptor<LibraryDTO[]> libCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);

        underTest.execute();
        Thread.sleep(100);

        verify(clusterService).edit(reqCaptor.capture());
        verify(clusterService).getInfo(clusterId);

        verify(libraryService).uninstall(eq(clusterId), any(LibraryDTO[].class));
        verify(libraryService).install(eq(clusterId), libCaptor.capture());
    }

    @Test
    public void test_CreateArtifactPath_succeedsWithOverrides() throws Exception {
        underTest = getOverridesMojo(GOAL);
        assertTrue(getPath().endsWith("databricks-cluster-settings-override.json"));
    }

    @Test(expectedExceptions = MojoExecutionException.class,
            expectedExceptionsMessageRegExp = "Failed to unmarshal Settings DTO.*")
    public void test_executeWithOverride_malformedConfigException() throws Exception {
        underTest = getOverridesMojo(GOAL, "-malformed");
        assertTrue(getPath().endsWith("databricks-cluster-settings-malformed.json"));

        underTest.execute();
    }

    @Test
    public void test_getClusterSettings_returnsNoFile() throws Exception {
        underTest = getOverridesMojo(GOAL, "-missing");
        assertTrue(getPath().endsWith("databricks-cluster-settings-missing.json"));

        underTest.execute();
        List<ClusterSettingsDTO> clusterSettingsDTOs = underTest.getSettingsUtils().buildSettingsDTOsWithDefaults();

        assertEquals(0, clusterSettingsDTOs.size());
    }

    protected abstract String getPath();

    private void assertCreateClusterRequestEquality(ArgumentCaptor<CreateClusterRequest> reqCaptor, Collection<String> clusterNames) {
        // mandatory params
        Map<String, Object> reqData = reqCaptor.getValue().getData();
        AutoScaleDTO autoscale = (AutoScaleDTO) reqData.get("autoscale");
        if (autoscale == null) {
            assertEquals(1, reqData.get("num_workers"));
        } else {
            // num_workers param should be ignored when autoscale specified
            assertNull(reqData.get("num_workers"));
            assertEquals(1, autoscale.getMinWorkers());
            assertEquals(2, autoscale.getMaxWorkers());
        }
        assertTrue(clusterNames.contains(reqData.get("cluster_name")));
        assertEquals("5.2.x-scala2.11", reqData.get("spark_version"));
        assertEquals("m4.large", reqData.get("node_type_id"));
        AwsAttributesDTO awsAttributes = (AwsAttributesDTO) reqData.get("aws_attributes");
        assertEquals(1, awsAttributes.getFirstOnDemand());
        assertEquals(SPOT_WITH_FALLBACK, awsAttributes.getAvailability());
        assertEquals("us-east-1c", awsAttributes.getZoneId());
        assertEquals("yourArn", awsAttributes.getInstanceProfileArn());
        assertEquals(50, awsAttributes.getSpotBidPricePercent());
        assertEquals(GENERAL_PURPOSE_SSD, awsAttributes.getEbsVolumeType());
        assertEquals(1, awsAttributes.getEbsVolumeCount());
        assertEquals(100, awsAttributes.getEbsVolumeSize());
        Map<String, String> sparkEnvVars = (Map<String, String>) reqData.get("spark_env_vars");
        assertEquals("/databricks/python3/bin/python3", sparkEnvVars.get("PYSPARK_PYTHON"));
        assertEquals(10, reqData.get("autotermination_minutes"));
        // optional params
        assertEquals("m4.large", reqData.get("driver_node_type_id"));
        Map<String, String> sparkConf = (Map<String, String>) reqData.get("spark_conf");
        assertEquals("2g", sparkConf.get("spark.driver.maxResultSize"));
        ClusterLogConfDTO clusterLogConf = (ClusterLogConfDTO) reqData.get("cluster_log_conf");
        assertNull(clusterLogConf.getDbfs());
        assertNull(clusterLogConf.getS3());
        ClusterTagDTO[] customTags = (ClusterTagDTO[]) reqData.get("custom_tags");
        assertEquals("tag", customTags[0].getKey());
        assertEquals("value", customTags[0].getValue());
        String[] sshPublicKeys = (String[]) reqData.get("ssh_public_keys");
        assertEquals("ssh_key1", sshPublicKeys[0]);
        assertEquals("ssh_key2", sshPublicKeys[1]);
    }

    private ClusterInfoDTO createClusterInfoDTO() {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.setClusterName("my-cluster");
        clusterInfoDTO.setClusterId("clusterId");
        clusterInfoDTO.setState(ClusterStateDTO.RUNNING);
        return clusterInfoDTO;
    }

    private ClusterLibraryStatusesDTO createClusterLibraryStatusesDTO() {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.setJar("dbfs:/Libs/jars/app_sdk_0_1_2-345.jar");
        LibraryFullStatusDTO libraryFullStatusDTO = new LibraryFullStatusDTO();
        libraryFullStatusDTO.setLibrary(libraryDTO);
        ClusterLibraryStatusesDTO libraryStatusesDTO = new ClusterLibraryStatusesDTO();
        libraryStatusesDTO.setLibraryFullStatuses(
                new LibraryFullStatusDTO[]{libraryFullStatusDTO}
        );
        return libraryStatusesDTO;
    }

}