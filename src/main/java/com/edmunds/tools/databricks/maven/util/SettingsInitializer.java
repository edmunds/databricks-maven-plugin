package com.edmunds.tools.databricks.maven.util;

import com.edmunds.tools.databricks.maven.model.BaseModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.File;

public interface SettingsInitializer<T extends BaseModel, D> {

    File getSettingsFile();

    void fillInDefaults(D settings, D defaultSettings, T templateModel) throws JsonProcessingException;

    void validate(D settings, T templateModel) throws MojoExecutionException;

}
