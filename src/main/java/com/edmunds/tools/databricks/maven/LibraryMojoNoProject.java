/*
 *    Copyright 2018 Edmunds.com, Inc.
 *
 *        Licensed under the Apache License, Version 2.0 (the "License");
 *        you may not use this file except in compliance with the License.
 *        You may obtain a copy of the License at
 *
 *            http://www.apache.org/licenses/LICENSE-2.0
 *
 *        Unless required by applicable law or agreed to in writing, software
 *        distributed under the License is distributed on an "AS IS" BASIS,
 *        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *        See the License for the specific language governing permissions and
 *        limitations under the License.
 */

package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.LibraryClustersModel;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

/**
 * This is the No Project version of the library Mojo.
 * Used to deploy a specific artifact to a databricks cluster when you are not working with the source code.
 */
@Mojo(name = "library-np", requiresProject = false)
public class LibraryMojoNoProject extends LibraryMojo {

    @Parameter(property = "libraryMappingFile", required = true)
    protected File libraryMappingFile;

    @Override
    protected LibraryClustersModel getLibraryClustersModel() throws MojoExecutionException {
        LibraryClustersModel libraryClustersModel = LibraryClustersModel.loadFromFile(libraryMappingFile);
        if (libraryClustersModel == null) {
            getLog().info("No library mapping file found at " + libraryMappingFile);
        }
        return libraryClustersModel;
    }
}
