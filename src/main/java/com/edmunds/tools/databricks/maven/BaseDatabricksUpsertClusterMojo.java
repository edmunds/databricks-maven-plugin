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

package com.edmunds.tools.databricks.maven;

import com.edmunds.tools.databricks.maven.model.ClusterTemplateDTO;
import com.edmunds.tools.databricks.maven.model.ClusterTemplateModel;
import com.edmunds.tools.databricks.maven.util.ObjectMapperUtils;
import com.edmunds.tools.databricks.maven.util.SettingsUtils;
import com.edmunds.tools.databricks.maven.validation.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import freemarker.cache.StringTemplateLoader;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class BaseDatabricksUpsertClusterMojo extends BaseDatabricksMojo {

    /**
     * The databricks cluster json file that contains all of the information for how to create databricks cluster.
     */
    @Parameter(defaultValue = "${project.build.resources[0].directory}/databricks-plugin/databricks-cluster-settings.json", property = "dbClusterFile")
    protected File dbClusterFile;

    protected ClusterTemplateDTO[] getClusterTemplateDTOs() throws MojoExecutionException {
        return loadClusterTemplateDTOsFromFile(dbClusterFile);
    }

    protected ClusterTemplateDTO[] loadClusterTemplateDTOsFromFile(File clustersConfig) throws MojoExecutionException {
        if (!clustersConfig.exists()) {
            getLog().info("No clusters config file exists");
            return new ClusterTemplateDTO[]{};
        }
        ClusterTemplateDTO[] cts;
        try {
            cts = ObjectMapperUtils.deserialize(clustersConfig, ClusterTemplateDTO[].class);
        } catch (IOException e) {
            String config = clustersConfig.getName();
            try {
                config = new String(Files.readAllBytes(Paths.get(clustersConfig.toURI())));
            } catch (IOException ex) {
                // Exception while trying to read configuration file content. No need to log it
            }
            throw new MojoExecutionException("Failed to parse config: " + config, e);
        }
        return cts;
    }

    protected ClusterTemplateModel getClusterTemplateModel() throws MojoExecutionException {
        if (StringUtils.isBlank(databricksRepo)) {
            throw new MojoExecutionException("databricksRepo property is missing");
        }
        return new ClusterTemplateModel(project, environment, databricksRepo, databricksRepoKey, prefixToStrip);
    }

    ClusterTemplateDTO[] buildClusterTemplateDTOsWithDefault() throws MojoExecutionException {
        ClusterTemplateModel clusterTemplateModel = getClusterTemplateModel();
        String clusterSettings = SettingsUtils.getJobSettingsFromTemplate("clusterSettings", dbClusterFile, clusterTemplateModel);
        if (clusterSettings == null) {
            return new ClusterTemplateDTO[]{};
        }

        ClusterTemplateDTO defaultClusterTemplateDTO = defaultClusterTemplateDTO();

        ClusterTemplateDTO[] clusterTemplateDTOs = deserializeClusterTemplateDTOs(clusterSettings, readDefaultCluster());
        for (ClusterTemplateDTO clusterTemplateDTO : clusterTemplateDTOs) {
            try {
                SettingsUtils.fillInDefaultClusterSettings(clusterTemplateDTO, defaultClusterTemplateDTO, clusterTemplateModel);
            } catch (JsonProcessingException e) {
                throw new MojoExecutionException("Fail to fill empty-value with default", e);
            }

            // Validate all job settings. If any fail terminate.
            if (validate) {
                validateClusterTemplate(clusterTemplateDTO, clusterTemplateModel);
            }
        }

        return clusterTemplateDTOs;
    }

    private void validateClusterTemplate(ClusterTemplateDTO clusterTemplateDTO, ClusterTemplateModel clusterTemplateModel) throws MojoExecutionException {
        int numWorkers = clusterTemplateDTO.getNumWorkers();
        if (numWorkers == 0) {
            throw new MojoExecutionException("REQUIRED FIELD [num_workers] was empty. VALIDATION FAILED.");
        }
    }

    /**
     * FIXME - it is possible for the example to be invalid, and the cluster file being valid. This should be fixed.
     *
     * <p>
     * Default ClusterTemplateDTO is used to fill the value when user cluster has missing value.
     *
     * @return
     * @throws MojoExecutionException
     */
    public ClusterTemplateDTO defaultClusterTemplateDTO() throws MojoExecutionException {
        return deserializeClusterTemplateDTOs(getClusterSettingsFromTemplate(readDefaultCluster(), getClusterTemplateModel()), readDefaultCluster())[0];
    }

    private static ClusterTemplateDTO[] deserializeClusterTemplateDTOs(String settingsJson, String defaultSettingsJson) throws MojoExecutionException {
        try {
            return ObjectMapperUtils.deserialize(settingsJson, ClusterTemplateDTO[].class);
        } catch (IOException e) {
            throw new MojoExecutionException(String.format("Failed to unmarshal cluster templates to object:\n[%s]\nHere is an example, of what it should look like:\n[%s]\n",
                    settingsJson,
                    defaultSettingsJson), e);
        }
    }

    String getClusterSettingsFromTemplate(String templateText, ClusterTemplateModel jobTemplateModel) throws MojoExecutionException {
        StringWriter stringWriter = new StringWriter();
        try {
            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate("defaultTemplate", templateText);

            Template temp = SettingsUtils.getFreemarkerConfiguration(templateLoader).getTemplate("defaultTemplate");
            temp.process(jobTemplateModel, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process job template: [%s]\nFreemarker message:\n%s", templateText, e.getMessage()), e);
        }

        return stringWriter.toString();
    }


    private static String readDefaultCluster() {
        try {
            return IOUtils.toString(BaseDatabricksJobMojo.class.getResourceAsStream("/default-cluster.json"), Charset.defaultCharset());
        } catch (Exception e) {
            return ExceptionUtils.getStackTrace(e);
        }
    }

}
