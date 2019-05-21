package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.ClusterTemplateModel;
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
     * The databricks cluster json file that contains all of the information for how to create databricks cluster.
     * Must be specified in case of non-project run.
     */
    @Parameter(name = "dbClusterFile", property = "dbClusterFile", required = true)
    protected File dbClusterFile;

    @Override
    protected ClusterTemplateModel[] getClusterTemplateModels() throws MojoExecutionException {
        return loadClusterTemplateModelsFromFile(dbClusterFile);
    }

}
