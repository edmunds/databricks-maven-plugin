package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.ClusterEnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

/**
 * Cluster mojo, to perform databricks cluster upsert (create or update through recreation).
 * <p>
 * NoProject mojo has been extracted into a separate class
 * so we have both mojos for non-project execution and for multi-module projects.
 */
@Mojo(name = "upsert-cluster-np", requiresProject = false)
public class UpsertClusterMojoNoProject extends UpsertClusterMojo {

    /**
     * The serialized cluster environment dto is required to be passed in a NoProject scenario.
     */
    @Parameter(name = "clusterEnvironmentDTOFile", property = "clusterEnvironmentDTOFile", required = true)
    private File clusterEnvironmentDTOFile;

    @Override
    protected EnvironmentDTOSupplier<ClusterEnvironmentDTO> createEnvironmentDTOSupplier() {
        return new EnvironmentDTOSupplier<ClusterEnvironmentDTO>() {
            @Override
            public ClusterEnvironmentDTO get() throws MojoExecutionException {
                ClusterEnvironmentDTO serializedClusterEnvironment = ClusterEnvironmentDTO.loadClusterEnvironmentDTOFromFile(clusterEnvironmentDTOFile);
                //We now set properties that are based on runtime and not buildtime. Ideally this would be enforced.
                //I consider this code ugly
                if (environment != null) {
                    serializedClusterEnvironment.setEnvironment(environment);
                }
                return serializedClusterEnvironment;
            }

            @Override
            public File getEnvironmentDTOFile() {
                return UpsertClusterMojoNoProject.super.dbClusterFile;
            }
        };
    }

}
