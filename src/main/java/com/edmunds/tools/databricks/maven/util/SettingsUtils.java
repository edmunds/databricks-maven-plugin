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

import com.edmunds.tools.databricks.maven.BaseDatabricksMojo;
import com.edmunds.tools.databricks.maven.model.BaseEnvironmentDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import freemarker.cache.FileTemplateLoader;
import freemarker.cache.StringTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for Databricks Mojo settings initialization process.
 * Conjuncts Mojo-specific and common part of logic.
 *
 * @param <M> Databricks Mojo Databricks Mojo class.
 * @param <E> Environment DTO POJO with Project and Environment properties.
 * @param <S> Settings DTO POJO that contains Mojo settings.
 */
public class SettingsUtils<M extends BaseDatabricksMojo, E extends BaseEnvironmentDTO, S> {

    public static final ObjectMapper OBJECT_MAPPER = ObjectMapperUtils.getObjectMapper();
    private static final Log log = new SystemStreamLog();

    private final Class<M> mojoClass;
    private final Class<S[]> settingsDtoArrayClass;
    private final File environmentDTOFile;
    private final String defaultSettingsFileName;
    private final EnvironmentDTOSupplier<E> environmentDTOSupplier;
    private final SettingsInitializer<E, S> settingsInitializer;

    public SettingsUtils(Class<M> mojoClass, Class<S[]> settingsDtoArrayClass, File environmentDTOFile, String defaultSettingsFileName,
                         EnvironmentDTOSupplier<E> environmentDTOSupplier, SettingsInitializer<E, S> settingsInitializer) {
        this.mojoClass = mojoClass;
        this.settingsDtoArrayClass = settingsDtoArrayClass;
        this.environmentDTOFile = environmentDTOFile;
        this.defaultSettingsFileName = defaultSettingsFileName;
        this.environmentDTOSupplier = environmentDTOSupplier;
        this.settingsInitializer = settingsInitializer;
    }

    // TODO this method exists only because of unit tests
    public SettingsInitializer<E, S> getSettingsInitializer() {
        return settingsInitializer;
    }

    /**
     * FIXME - it is possible for the example to be invalid, and the user settings file being valid. This should be fixed.
     *
     * <p>
     * Default SettingsDTO is used to fill the value when user settings has missing value.
     *
     * @return
     * @throws MojoExecutionException
     */
    public S defaultSettingsDTO() throws MojoExecutionException {
        String defaultSettingsJson = readDefaultSettingsJson();
        return deserializeSettings(getSettingsJsonFromEnvironment(defaultSettingsJson, getEnvironmentDTO()), defaultSettingsJson)[0];
    }

    public E getEnvironmentDTO() throws MojoExecutionException {
        return environmentDTOSupplier.get();
    }

    public List<S> buildSettingsDTOsWithDefaults() throws MojoExecutionException {
        E environmentDTO = getEnvironmentDTO();
        String settingsJson = getSettingsJsonFromEnvironment(environmentDTO);
        if (settingsJson == null) {
            return Collections.emptyList();
        }

        S defaultSettingDTO = defaultSettingsDTO();
        List<S> settingsDTOs = Arrays.asList(deserializeSettings(settingsJson, readDefaultSettingsJson()));
        for (S settingsDTO : settingsDTOs) {
            try {
                settingsInitializer.fillInDefaults(settingsDTO, defaultSettingDTO, environmentDTO);
            } catch (JsonProcessingException e) {
                throw new MojoExecutionException("Fail to fill empty-value with default", e);
            }
            // Validate all mojo settings. If any fail terminate.
            settingsInitializer.validate(settingsDTO, environmentDTO);
        }

        return settingsDTOs;
    }

    String getSettingsJsonFromEnvironment(E environmentDTO) throws MojoExecutionException {
        if (!environmentDTOFile.exists()) {
            log.info("No Environment DTO File exists");
            return null;
        }
        StringWriter stringWriter = new StringWriter();
        try {
            TemplateLoader templateLoader = new FileTemplateLoader(environmentDTOFile.getParentFile());
            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate(environmentDTOFile.getName());
            temp.process(environmentDTO, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process Environment DTO File: [%s]\nFreemarker message:\n%s", environmentDTOFile.getAbsolutePath(), e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    private String getSettingsJsonFromEnvironment(String defaultSettingsJson, E environmentDTO) throws MojoExecutionException {
        StringWriter stringWriter = new StringWriter();
        try {
            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate("defaultSettings", defaultSettingsJson);
            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate("defaultSettings");
            temp.process(environmentDTO, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process Environment DTO: [%s]\nFreemarker message:\n%s", defaultSettingsJson, e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    private Configuration getFreemarkerConfiguration(TemplateLoader templateLoader) throws IOException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setTemplateLoader(templateLoader);
        cfg.setDefaultEncoding(Charset.defaultCharset().name());
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        return cfg;
    }

    private S[] deserializeSettings(String settingsDTOJson, String defaultSettingsDTOJson) throws MojoExecutionException {
        try {
            return ObjectMapperUtils.deserialize(settingsDTOJson, settingsDtoArrayClass);
        } catch (IOException e) {
            throw new MojoExecutionException(String.format("Failed to unmarshal Settings DTO:\n[%s]\nHere is an example, of what it should look like:\n[%s]\n",
                    settingsDTOJson,
                    defaultSettingsDTOJson), e);
        }
    }

    private String readDefaultSettingsJson() {
        try {
            return IOUtils.toString(mojoClass.getResourceAsStream(defaultSettingsFileName), Charset.defaultCharset());
        } catch (Exception e) {
            return ExceptionUtils.getStackTrace(e);
        }
    }

}
