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

import com.edmunds.rest.databricks.DTO.ClusterAttributesDTO;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

/**
 * Simple POJO to pass properties to the cluster template.
 */
public class ClusterTemplateModel extends ClusterAttributesDTO {

    @JsonProperty("num_workers")
    private int numWorkers;
    @JsonProperty("artifact_paths")
    private Collection<String> artifactPaths;

    /**
     * Don't use this - it's for jackson deserialization only!
     */
    public ClusterTemplateModel() {
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public void setNumWorkers(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    public Collection<String> getArtifactPaths() {
        return artifactPaths;
    }

    public void setArtifactPaths(Collection<String> artifactPaths) {
        this.artifactPaths = artifactPaths;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            ClusterTemplateModel other = (ClusterTemplateModel) o;

            if (this.getNumWorkers() != other.getNumWorkers()) {
                return false;
            }

            Object this$artifactPaths = this.getArtifactPaths();
            Object other$artifactPaths = other.getArtifactPaths();
            if (this$artifactPaths == null) {
                return other$artifactPaths == null;
            } else {
                return this$artifactPaths.equals(other$artifactPaths);
            }
        }
        return false;
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof ClusterTemplateModel;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = result * 59 + this.getNumWorkers();
        Object $artifactPaths = this.getArtifactPaths();
        result = result * 59 + ($artifactPaths == null ? 43 : $artifactPaths.hashCode());
        return result;
    }

    @Override
    public String toString() {
        String result = super.toString();
        return result.substring(0, result.length() - 1) +
                ", numWorkers=" + this.getNumWorkers() + ", artifactPaths=" + this.getArtifactPaths() + ")";
    }
}
