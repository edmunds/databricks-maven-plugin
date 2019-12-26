package com.edmunds.tools.databricks.maven.util;

import com.edmunds.tools.databricks.maven.model.EnvironmentDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * A class implementing this interface should fulfill Settings DTO fields initialization & validation logic.
 *
 * @param <S> Settings DTO A POJO that contains Mojo settings.
 */
public interface SettingsInitializer<S> {

    /**
     * This method enriches User Settings DTO (first parameter) with Default Settings and Project/Environment Properties
     * if any mandatory fields are empty.
     *
     * @param settingsDTO User settings.
     * @param defaultSettingsDTO Default settings.
     * @param environmentDTO Project and Environment properties.
     * @throws JsonProcessingException exception
     */
    void fillInDefaults(S settingsDTO, S defaultSettingsDTO, EnvironmentDTO environmentDTO)
        throws JsonProcessingException;

    /**
     * Checks whether Settings DTO properties valid or not. Validation logic varies for different Mojo DTOs.
     *
     * @param settingsDTO Settings DTO.
     * @param environmentDTO Project and Environment properties.
     * @throws MojoExecutionException exception
     */
    void validate(S settingsDTO, EnvironmentDTO environmentDTO) throws MojoExecutionException;

}
