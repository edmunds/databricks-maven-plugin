package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.LibraryClustersModel;
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;


import static org.apache.commons.lang3.StringUtils.defaultString;

/**
 * Prepares the library-mapping.json file such that we can run library attachment, sans project later (e.g. during a build).
 */
@Mojo(name = "prepare-library-resources", requiresProject = true, defaultPhase = LifecyclePhase.PREPARE_PACKAGE)
public class PrepareLibraryResources extends BaseWorkspaceMojo {

    public static final String DEFAULT_DBFS_ROOT_FORMAT = "s3://%s";

    public static final String JAR = "jar";

    public static final String LIBRARY_MAPPING_FILE_NAME = "library-mapping.json";


    //TODO we actually are repeating ourselves from upload mojo here. Eventually we need to combine these.
    /**
     * The s3 bucket to upload your jar to.
     */
    @Parameter(property = "bucketName", required = true)
    private String bucketName;

    /**
     * The prefix to load to.
     *
     * @parameter default-value "artifacts/${project.groupId}/${project.artifactId}/${project.version}/${project.build.finalName}.${project.packaging}"
     */
    @Parameter(property = "key", defaultValue = "artifacts/${project.groupId}/${project.artifactId}/${project.version}/${project.build.finalName}.${project.packaging}")
    private String key;

    @Parameter(property = "libaryMappingFile", defaultValue = "${project.build.directory}/databricks-plugin/" + LIBRARY_MAPPING_FILE_NAME)
    protected File libaryMappingFile;

    /**
     * This should be a list of cluster names to install to.
     */
    @Parameter(property = "clusters")
    protected String[] clusters;

    /**
     * The version of the jar to attach to the clusters. Defaults to the current project version.
     */
    @Parameter(property = "version", defaultValue = "${project.version}")
    protected String version;

    @Override
    public void execute() throws MojoExecutionException {
        prepareLibraryResources();
    }

    void prepareLibraryResources() throws MojoExecutionException {
        if (StringUtils.isBlank(bucketName)) {
            //This alternative property source is for the integration test.
            bucketName = System.getProperty("DB_REPO");
            if (StringUtils.isBlank(bucketName)) {
                throw new MojoExecutionException("Missing mandatory parameter: ${bucketName}");
            }
        }
        if (project.getArtifact().getType().equals(JAR)) {
            if (ArrayUtils.isNotEmpty(clusters)) {
                try {
                    FileUtils.writeStringToFile(libaryMappingFile, ObjectMapperUtils.serialize(getLibraryClustersModel()));
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

    protected LibraryClustersModel getLibraryClustersModel() throws MojoExecutionException {
        try {
            LibraryClustersModel libraryClustersModel;
            if (libaryMappingFile.exists()) {
                String libraryMappingModelJson = FileUtils.readFileToString(libaryMappingFile);
                libraryClustersModel = ObjectMapperUtils.deserialize(libraryMappingModelJson, LibraryClustersModel.class);
            } else {
                if (isLocalBuild) {
                    libraryClustersModel = new LibraryClustersModel(createArtifactPath(), Arrays.asList(clusters));
                } else {
                    throw new MojoExecutionException(String.format("[%s] file was not found in the build. Please ensure prepare-package was ran during build.", LIBRARY_MAPPING_FILE_NAME));
                }
            }
            return libraryClustersModel;
        } catch (IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    String createArtifactPath() {
        return String.format("%s/%s", bucketName, key);
    }

}
