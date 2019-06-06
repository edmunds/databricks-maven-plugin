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

import com.edmunds.tools.databricks.maven.model.BaseModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import freemarker.cache.FileTemplateLoader;
import freemarker.cache.StringTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;

public class SettingsUtils<T extends BaseModel> {

    public static final ObjectMapper OBJECT_MAPPER = ObjectMapperUtils.getObjectMapper();

    private static Log log;

    public static Log getLog() {
        if (log == null) {
            log = new SystemStreamLog();
        }
        return log;
    }

    public String getSettingsFromTemplate(String settingsName, File settingsFile, T templateModel) throws MojoExecutionException {
        if (!settingsFile.exists()) {
            getLog().info(String.format("No %s file exists", settingsName));
            return null;
        }
        StringWriter stringWriter = new StringWriter();
        try {
            TemplateLoader templateLoader = new FileTemplateLoader(settingsFile.getParentFile());
            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate(settingsFile.getName());
            temp.process(templateModel, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process %s file as template: [%s]\nFreemarker message:\n%s", settingsName, settingsFile.getAbsolutePath(), e.getMessage()), e);
        }

        return stringWriter.toString();
    }

    public Configuration getFreemarkerConfiguration(TemplateLoader templateLoader) throws IOException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setTemplateLoader(templateLoader);
        cfg.setDefaultEncoding(Charset.defaultCharset().name());
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        return cfg;
    }

    public String getModelFromTemplate(String templateText, T templateModel) throws MojoExecutionException {
        StringWriter stringWriter = new StringWriter();
        try {
            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate("defaultTemplate", templateText);

            Template temp = getFreemarkerConfiguration(templateLoader).getTemplate("defaultTemplate");
            temp.process(templateModel, stringWriter);
        } catch (IOException | TemplateException e) {
            throw new MojoExecutionException(String.format("Failed to process template model: [%s]\nFreemarker message:\n%s", templateText, e.getMessage()), e);
        }

        return stringWriter.toString();
    }

}
