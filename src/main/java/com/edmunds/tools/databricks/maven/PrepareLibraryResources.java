package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Prepares the library-mapping.json file such that we can run library attachment, sans project later (e.g. during a build).
 */
@Mojo(name = "prepare-library-resources", requiresProject = true, defaultPhase = LifecyclePhase.PREPARE_PACKAGE)
public class PrepareLibraryResources extends BaseLibraryMojo {

    private static final String LIBRARY_MAPPING_FILE_NAME = "library-mapping.json";

    @Parameter(property = "libaryMappingFile", defaultValue = "${project.build.directory}/databricks-plugin/" + LIBRARY_MAPPING_FILE_NAME)
    protected File libaryMappingFileOutput;

    @Override
    public void execute() throws MojoExecutionException {
        prepareLibraryResources();
    }

    void prepareLibraryResources() throws MojoExecutionException {
        if (project.getArtifact().getType().equals(JAR)) {
            if (ArrayUtils.isNotEmpty(clusters)) {
                try {
                    FileUtils.writeStringToFile(libaryMappingFileOutput,
                            ObjectMapperUtils.serialize(getLibraryClustersModel()), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new MojoExecutionException(e.getMessage(), e);
                }
            } else {
                getLog().warn("no clusters configured for library prepare, skipping");
            }
        } else {
            getLog().warn("non jar artifact found, skipping");
        }
    }
}
