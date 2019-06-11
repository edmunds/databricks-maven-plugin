package com.edmunds.tools.databricks.maven.util;

import com.edmunds.tools.databricks.maven.model.BaseEnvironmentDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * A class implementing this interface should bring Settings DTO fields initialization & validation logic.
 *
 * @param <E> Environment DTO.
 * @param <S> Settings DTO A POJO that contains Mojo settings.
 */
public interface SettingsInitializer<E extends BaseEnvironmentDTO, S> {

    /**
     * This method enriches User Settings DTO (first parameter) with Default Settings
     * and Project/Environment Properties  if any mandatory fields are empty.
     *
     * @param settingsDTO        User settings.
     * @param defaultSettingsDTO Default settings.
     * @param environmentDTO     Project and Environment properties.
     * @throws JsonProcessingException
     */
    void fillInDefaults(S settingsDTO, S defaultSettingsDTO, E environmentDTO) throws JsonProcessingException;

    /**
     * Checks whether Settings DTO properties valid or not.
     * Validation logic varies for different Mojo DTOs.
     *
     * @param settingsDTO    Settings DTO.
     * @param environmentDTO Project and Environment properties.
     * @throws MojoExecutionException
     */
    void validate(S settingsDTO, E environmentDTO) throws MojoExecutionException;

}
