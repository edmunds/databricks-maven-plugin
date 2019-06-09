package com.edmunds.tools.databricks.maven.util;

import com.edmunds.tools.databricks.maven.model.BaseEnvironmentDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * A class implementing this interface should bring Settings DTO fields initialization & validation logic.
 *
 * @param <E> Environment DTO
 * @param <S> Settings DTO
 */
public interface SettingsInitializer<E extends BaseEnvironmentDTO, S> {

    void fillInDefaults(S settingsDTO, S defaultSettingsDTO, E environmentDTO) throws JsonProcessingException;

    void validate(S settingsDTO, E environmentDTO) throws MojoExecutionException;

}
