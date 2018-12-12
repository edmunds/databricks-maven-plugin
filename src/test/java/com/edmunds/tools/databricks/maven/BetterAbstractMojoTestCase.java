/*
 *        Taken from https://github.com/ahgittin/license-audit-maven-plugin
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

import org.apache.maven.DefaultMaven;
import org.apache.maven.Maven;
import org.apache.maven.execution.*;
import org.apache.maven.plugin.Mojo;
import org.apache.maven.plugin.testing.AbstractMojoTestCase;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuilder;
import org.apache.maven.project.ProjectBuildingRequest;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.internal.impl.SimpleLocalRepositoryManagerFactory;
import org.eclipse.aether.repository.LocalRepository;

import java.io.File;
import java.util.Arrays;

/**
 * Borrowed from <strong>ahgittin</strong> to provide a working Maven project as part of unit testing.
 *
 * @author ahgittin (https://github.com/ahgittin/license-audit-maven-plugin)
 */
public abstract class BetterAbstractMojoTestCase extends AbstractMojoTestCase {

    protected MavenSession newMavenSession() {
        try {
            MavenExecutionRequest request = new DefaultMavenExecutionRequest();
            MavenExecutionResult result = new DefaultMavenExecutionResult();

            // populate sensible defaults, including repository basedir and remote repos
            MavenExecutionRequestPopulator populator;
            populator = getContainer().lookup(MavenExecutionRequestPopulator.class);
            populator.populateDefaults(request);

            // this is needed to allow java profiles to get resolved; i.e. avoid during project builds:
            // [ERROR] Failed to determine Java version for profile java-1.5-detected @ org.apache.commons:commons-parent:22, /Users/alex/.m2/repository/org/apache/commons/commons-parent/22/commons-parent-22.pom, line 909, column 14
            request.setSystemProperties(System.getProperties());

            // and this is needed so that the repo session in the maven session
            // has a repo manager, and it points at the local repo
            // (cf MavenRepositorySystemUtils.newSession() which is what is otherwise done)
            DefaultMaven maven = (DefaultMaven) getContainer().lookup(Maven.class);
            DefaultRepositorySystemSession repoSession =
                (DefaultRepositorySystemSession) maven.newRepositorySession(request);
            repoSession.setLocalRepositoryManager(
                new SimpleLocalRepositoryManagerFactory().newInstance(repoSession,
                    new LocalRepository(request.getLocalRepository().getBasedir())));
            @SuppressWarnings("deprecation")
            MavenSession session = new MavenSession(getContainer(),
                repoSession,
                request, result);
            return session;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extends the super to use the new {@link #newMavenSession()} introduced here
     * which sets the defaults one expects from maven; the standard test case leaves a lot of things blank
     */
    @Override
    protected MavenSession newMavenSession(MavenProject project) {
        MavenSession session = newMavenSession();
        session.setCurrentProject(project);
        session.setProjects(Arrays.asList(project));
        return session;
    }

    /**
     * As {@link #lookupConfiguredMojo(MavenProject, String)} but taking the pom file
     * and creating the {@link MavenProject}.
     */
    protected Mojo lookupConfiguredMojo(File pom, String goal) throws Exception {
        assertNotNull(pom);
        assertTrue(pom.exists());

        ProjectBuildingRequest buildingRequest = newMavenSession().getProjectBuildingRequest();
        ProjectBuilder projectBuilder = lookup(ProjectBuilder.class);
        MavenProject project = projectBuilder.build(pom, buildingRequest).getProject();

        return lookupConfiguredMojo(project, goal);
    }

}