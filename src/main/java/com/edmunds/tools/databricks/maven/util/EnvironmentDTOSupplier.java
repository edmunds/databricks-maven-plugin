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

package com.edmunds.tools.databricks.maven.util;

import org.apache.maven.plugin.MojoExecutionException;

import java.io.File;

/**
 * A class implementing this interface should return EnvironmentDTO & EnvironmentDTO File for a concrete Mojo.
 *
 * @param <E> Environment DTO
 */
public interface EnvironmentDTOSupplier<E> {

    E get() throws MojoExecutionException;

    File getEnvironmentDTOFile();

}
