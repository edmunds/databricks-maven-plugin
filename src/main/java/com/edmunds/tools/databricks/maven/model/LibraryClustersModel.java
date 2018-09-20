package com.edmunds.tools.databricks.maven.model;

import java.util.Collection;

/**
 * Used to serialize artifact and cluster data, for library attachment.
 */
public class LibraryClustersModel {

    private String artifactPath;
    private Collection<String> clusterNames;

    /**
     * Don't use this - it's for jackson deserialization only!
     */
    public LibraryClustersModel() {
    }

    public LibraryClustersModel(String artifactPath, Collection<String> clusterNames) {
        this.artifactPath = artifactPath;
        this.clusterNames = clusterNames;
    }

    public String getArtifactPath() {
        return artifactPath;
    }

    public Collection<String> getClusterNames() {
        return clusterNames;
    }
}
