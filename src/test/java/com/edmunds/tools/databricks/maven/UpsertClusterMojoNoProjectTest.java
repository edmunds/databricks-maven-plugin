package com.edmunds.tools.databricks.maven;

/**
 * Tests for @{@link UpsertClusterMojoNoProject}.
 */
public class UpsertClusterMojoNoProjectTest extends AbstractUpsertClusterMojoTest<UpsertClusterMojoNoProject> {

    @Override
    protected void setGoal() {
        GOAL = "upsert-cluster-np";
    }

    @Override
    protected String getPath() {
        return underTest.dbClusterFile.getPath();
    }

}