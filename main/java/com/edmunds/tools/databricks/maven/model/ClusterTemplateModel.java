/*
 *  Copyright 2019 Edmunds.com, Inc.
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

package com.edmunds.tools.databricks.maven.model;

import com.edmunds.rest.databricks.DTO.AwsAttributesDTO;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

/**
 * Simple POJO to pass properties to the cluster template.
 */
public class ClusterTemplateModel {

    @JsonProperty("num_workers")
    private Integer numWorkers;
    @JsonProperty("cluster_name")
    private String clusterName;
    @JsonProperty("spark_version")
    private String sparkVersion;
    @JsonProperty("aws_attributes")
    private AwsAttributesDTO awsAttributes;
    @JsonProperty("node_type_id")
    private String nodeTypeId;
    @JsonProperty("autotermination_minutes")
    private Integer autoterminationMinutes;
    @JsonProperty("artifact_paths")
    private Collection<String> artifactPaths;

    /**
     * Don't use this - it's for jackson deserialization only!
     */
    public ClusterTemplateModel() {
    }

    public Integer getNumWorkers() {
        return numWorkers;
    }

    public void setNumWorkers(Integer numWorkers) {
        this.numWorkers = numWorkers;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getSparkVersion() {
        return sparkVersion;
    }

    public void setSparkVersion(String sparkVersion) {
        this.sparkVersion = sparkVersion;
    }

    public AwsAttributesDTO getAwsAttributes() {
        return awsAttributes;
    }

    public void setAwsAttributes(AwsAttributesDTO awsAttributes) {
        this.awsAttributes = awsAttributes;
    }

    public String getNodeTypeId() {
        return nodeTypeId;
    }

    public void setNodeTypeId(String nodeTypeId) {
        this.nodeTypeId = nodeTypeId;
    }

    public Integer getAutoterminationMinutes() {
        return autoterminationMinutes;
    }

    public void setAutoterminationMinutes(Integer autoterminationMinutes) {
        this.autoterminationMinutes = autoterminationMinutes;
    }

    public Collection<String> getArtifactPaths() {
        return artifactPaths;
    }

    public void setArtifactPaths(Collection<String> artifactPaths) {
        this.artifactPaths = artifactPaths;
    }
}
