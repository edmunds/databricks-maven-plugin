/*
 *  Copyright 2018 Edmunds.com, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package com.edmunds.tools.databricks.maven.util;

import com.edmunds.rest.databricks.DTO.ClusterInfoDTO;
import com.edmunds.tools.databricks.maven.BaseDatabricksMojoTest;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

public class ClusterUtilsTest extends BaseDatabricksMojoTest {

    @BeforeMethod
    public void init() throws Exception {
        super.init();
    }

    @Test
    public void testConvertClusterNamesToIds() throws Exception {
        when(clusterService.list()).thenReturn(new ClusterInfoDTO[]{getClusterInfoDTO("test1", "123"), getClusterInfoDTO("test2", "456")});

        List<String> clusterIds = ClusterUtils.convertClusterNamesToIds(clusterService, Arrays.asList("test2"));

        assertThat(clusterIds, hasItem("456"));
        assertThat(clusterIds.size(), is(1));
    }

    private ClusterInfoDTO getClusterInfoDTO(String clusterName, String clusterId) {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO.setClusterName(clusterName);
        clusterInfoDTO.setClusterId(clusterId);
        return clusterInfoDTO;
    }
}