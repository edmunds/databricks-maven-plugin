package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.EnvironmentDTO;
import com.edmunds.tools.databricks.maven.util.EnvironmentDTOSupplier;
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
     * The serialized environment dto is required to be passed in a NoProject scenario.
     */
    @Parameter(name = "environmentDTOFile", property = "environmentDTOFile", required = true)
    private File environmentDTOFile;

    @Override
    protected EnvironmentDTOSupplier getEnvironmentDTOSupplier() {
        return () -> {
            EnvironmentDTO serializedEnvironment = EnvironmentDTO.loadEnvironmentDTOFromFile(environmentDTOFile);
            //We now set properties that are based on runtime and not buildtime. Ideally this would be enforced.
            //I consider this code ugly
            if (environment != null) {
                serializedEnvironment.setEnvironment(environment);
            }
            return serializedEnvironment;
        };
    }

}
