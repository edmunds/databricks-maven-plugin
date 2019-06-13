package com.edmunds.tools.databricks.maven.model;

import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * Used to serialize artifact and cluster data, for library attachment.
 * <p>
 * //TODO there is no reason to have a separate LibraryClustersModel and JobTemplatesModel.
 * // We should have file where we serialize all information pertaining to a project for None-Project invocation.
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

    public static LibraryClustersModel loadFromFile(File libraryClusterModelFile) throws MojoExecutionException {
        if (libraryClusterModelFile == null || !libraryClusterModelFile.exists()) {
            return null;
        }
        try {
            String libraryMappingModelJson = FileUtils.readFileToString(libraryClusterModelFile, StandardCharsets.UTF_8);
            return ObjectMapperUtils.deserialize(libraryMappingModelJson, LibraryClustersModel.class);
        } catch (IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}
