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

import com.edmunds.tools.databricks.maven.model.BaseEnvironmentDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
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
 * @param <E> Environment DTO POJO with Project and Environment properties.
 * @param <S> Settings DTO POJO that contains Mojo settings.
 */
public class SettingsUtils<E extends BaseEnvironmentDTO, S> {

    private static final Log log = new SystemStreamLog();

    private final Class<S[]> settingsDtoArrayClass;
    private final String defaultSettingsJson;
    private final File userSettingsFile;
    private final E environmentDTO;
    private final SettingsInitializer<E, S> settingsInitializer;

    /**
     * SettingsUtils constructor. Mojos instantiate this class and use it for building up Settings DTOs.
     *
     * @param settingsDtoArrayClass   Settings DTO array class, required because of generics type erasure.
     * @param defaultSettingsFileName Default Settings DTO file name.
     * @param userSettingsFile        User Settings DTO file.
     * @param environmentDTOSupplier  Project and Environment properties DTO supplier.
     * @param settingsInitializer     Settings DTO initializer for a concrete Mojo.
     */
    public SettingsUtils(Class<S[]> settingsDtoArrayClass, String defaultSettingsFileName, File userSettingsFile,
                         EnvironmentDTOSupplier<E> environmentDTOSupplier, SettingsInitializer<E, S> settingsInitializer)
            throws MojoExecutionException {
        this.settingsDtoArrayClass = settingsDtoArrayClass;
        this.defaultSettingsJson = readDefaultSettingsJson(defaultSettingsFileName);
        this.userSettingsFile = userSettingsFile;
        this.environmentDTO = environmentDTOSupplier.get();
        this.settingsInitializer = settingsInitializer;
    }

    /**
     * FIXME - it is possible for the example to be invalid, and the user settings file being valid. This should be fixed.
     *
     * <p>
     * Default SettingsDTO is used to fill the value when user settings has missing value.
     *
     * @return Default Mojo Settings DTO.
     * @throws MojoExecutionException
     */
    public S defaultSettingsDTO() throws MojoExecutionException {
        return deserializeSettings(enrichDefaultSettingsWithEnvironment())[0];
    }

    /**
     * Constructs Settings DTO from user specified settings, default settings, project and environment properties.
     *
     * @return Mojo Settings DTO.
     * @throws MojoExecutionException
     */
    public List<S> buildSettingsDTOsWithDefaults() throws MojoExecutionException {
        String userSettingsJson = getUserSettingsJson();
        if (userSettingsJson == null) {
            return Collections.emptyList();
        }

        S defaultSettingDTO = defaultSettingsDTO();
        List<S> settingsDTOs = Arrays.asList(deserializeSettings(userSettingsJson));
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

    /**
     * Produces Mojo Settings DTO json from User Settings file and Environment DTO.
     *
     * @return Mojo Settings DTO json.
     * @throws MojoExecutionException
     */
    String getUserSettingsJson() throws MojoExecutionException {
        String userSettingsPath = userSettingsFile.getAbsolutePath();
        if (!userSettingsFile.exists()) {
            log.info(String.format("Specified User Settings file not found (%s)", userSettingsPath));
            return null;
        }
        StringWriter stringWriter = new StringWriter();
        try {
            TemplateLoader templateLoader = new FileTemplateLoader(userSettingsFile.getParentFile());
            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate(userSettingsFile.getName());
            temp.process(environmentDTO, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process User Settings file: [%s]%nFreemarker message:%n%s", userSettingsPath, e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    /**
     * Produces Mojo Settings DTO json from Default Settings json and Environment DTO.
     *
     * @return Mojo Settings DTO json.
     * @throws MojoExecutionException
     */
    private String enrichDefaultSettingsWithEnvironment() throws MojoExecutionException {
        StringWriter stringWriter = new StringWriter();
        try {
            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate("defaultSettings", defaultSettingsJson);
            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate("defaultSettings");
            temp.process(environmentDTO, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process Default Settings json: [%s]%nFreemarker message:%n%s", defaultSettingsJson, e.getMessage()), e);
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

    private S[] deserializeSettings(String settingsDTOJson) throws MojoExecutionException {
        try {
            return ObjectMapperUtils.deserialize(settingsDTOJson, settingsDtoArrayClass);
        } catch (IOException e) {
            throw new MojoExecutionException(String.format("Failed to unmarshal Settings DTO:%n[%s]%nHere is an example, of what it should look like:%n[%s]%n",
                    settingsDTOJson,
                    defaultSettingsJson), e);
        }
    }

    private String readDefaultSettingsJson(String defaultSettingsFileName) {
        try {
            return IOUtils.toString(this.getClass().getResourceAsStream(defaultSettingsFileName), Charset.defaultCharset());
        } catch (Exception e) {
            return ExceptionUtils.getStackTrace(e);
        }
    }

}
