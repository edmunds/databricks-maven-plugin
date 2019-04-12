package com.edmunds.tools.databricks.maven;

import com.edmunds.rest.databricks.DTO.AwsAttributesDTO;
import com.edmunds.rest.databricks.DTO.ClusterInfoDTO;
import com.edmunds.rest.databricks.DTO.LibraryDTO;
import com.edmunds.rest.databricks.request.CreateClusterRequest;
import org.apache.maven.plugin.MojoExecutionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static com.edmunds.rest.databricks.DTO.AwsAvailabilityDTO.SPOT_WITH_FALLBACK;
import static com.edmunds.rest.databricks.DTO.EbsVolumeTypeDTO.GENERAL_PURPOSE_SSD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for @{@link UpsertClusterMojo}.
 */
public class UpsertClusterMojoTest extends DatabricksMavenPluginTestHarness {

    private final static String GOAL = "upsert-cluster";

    private UpsertClusterMojo underTest;

    @BeforeClass
    public void initClass() throws Exception {
        super.setUp();
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

        verify(clusterService, times(2)).create(reqCaptor.capture());
        assertCreateClusterRequestEquality(reqCaptor, Arrays.asList("my-cluster", "my-cluster-2"));

        verify(libraryService).install(eq(clusterId), libCaptor.capture());
        LibraryDTO[] libs = libCaptor.getValue();
        assertLibraryDTOEquality(libs[0], "s3://bucket-name/artifacts/com.company.project/my-artifact-name/1.0.132/my-artifact-name-1.0.132.jar");
        assertLibraryDTOEquality(libs[1], "dbfs:/Libs/jars/app_sdk_0_1_2-345.jar");
    }

    @Test(expectedExceptions = MojoExecutionException.class, expectedExceptionsMessageRegExp = ".*Cluster already exists")
    public void test_executeWithDefault_clusterAlreadyExistsException() throws Exception {
        underTest = getNoOverridesMojo(GOAL);
        when(clusterService.list()).thenReturn(new ClusterInfoDTO[]{createClusterInfoDTO()});

        underTest.execute();
    }

    @Test
    public void test_executeWithOverride_upsertCluster() throws Exception {
        underTest = getOverridesMojo(GOAL);
        String clusterId = "newClusterId";

        when(clusterService.list()).thenReturn(new ClusterInfoDTO[]{createClusterInfoDTO()});
        ArgumentCaptor<CreateClusterRequest> reqCaptor = ArgumentCaptor.forClass(CreateClusterRequest.class);
        when(clusterService.create(reqCaptor.capture())).thenReturn(clusterId);
        ArgumentCaptor<LibraryDTO[]> libCaptor = ArgumentCaptor.forClass(LibraryDTO[].class);
        doNothing().when(libraryService).install(eq(clusterId), libCaptor.capture());

        underTest.execute();

        verify(clusterService).create(reqCaptor.capture());
        assertCreateClusterRequestEquality(reqCaptor, Collections.singletonList("my-cluster"));

        verify(libraryService, times(0)).install(eq(clusterId), libCaptor.capture());
    }

    @Test
    public void testCreateArtifactPath_succeedsWithOverrides() throws Exception {
        underTest = getOverridesMojo(GOAL);
        assertTrue(underTest.dbClusterFile.getPath().endsWith("databricks-cluster-settings-override.json"));
        assertThat(underTest.failOnClusterExists, is(false));
    }

    @Test(expectedExceptions = MojoExecutionException.class,
            expectedExceptionsMessageRegExp = "Failed to parse config.*\"cluster_name\": \"my-cluster-malformed\".*")
    public void test_executeWithOverride_malformedConfigException() throws Exception {
        underTest = getOverridesMojo(GOAL, "-malformed");
        assertTrue(underTest.dbClusterFile.getPath().endsWith("databricks-cluster-settings-malformed.json"));
        assertThat(underTest.failOnClusterExists, is(false));

        underTest.execute();
    }

    @Test(expectedExceptions = MojoExecutionException.class,
            expectedExceptionsMessageRegExp = "Failed to parse config: databricks-cluster-settings-missing.json")
    public void test_executeWithOverride_badConfigPathException() throws Exception {
        underTest = getOverridesMojo(GOAL, "-missing");
        assertTrue(underTest.dbClusterFile.getPath().endsWith("databricks-cluster-settings-missing.json"));
        assertThat(underTest.failOnClusterExists, is(false));

        underTest.execute();
    }

    private ClusterInfoDTO createClusterInfoDTO() {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.setClusterName("my-cluster");
        clusterInfoDTO.setClusterId("clusterId");
        return clusterInfoDTO;
    }

    private void assertCreateClusterRequestEquality(ArgumentCaptor<CreateClusterRequest> reqCaptor, Collection<String> clusterNames) {
        Map<String, Object> reqData = reqCaptor.getValue().getData();
        assertEquals(1, reqData.get("num_workers"));
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
    }

    private void assertLibraryDTOEquality(LibraryDTO libDTO, String jar) {
        assertEquals(jar, libDTO.getJar());
        assertNull(libDTO.getEgg());
        assertNull(libDTO.getCran());
        assertNull(libDTO.getMaven());
        assertNull(libDTO.getPypi());
    }

}