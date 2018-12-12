/*
 *  Copyright 2018 Edmunds.com, Inc.
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

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

/**
 * Uploads an artifact to s3.
 */
@Mojo(name = "upload-to-s3", defaultPhase = LifecyclePhase.DEPLOY)
public class UploadMojo extends BaseDatabricksMojo {

    /**
     * The local file to upload.
     */
    @Parameter(property = "file", required = true, defaultValue = "${project.build.directory}/${project.build.finalName}.${project.packaging}")
    private File file;

    protected AmazonS3 s3Client;

    @Override
    public void execute() throws MojoExecutionException {
        if (file.exists()) {
            AmazonS3URI uri = new AmazonS3URI(createArtifactPath());
            String bucket = uri.getBucket();
            String key = uri.getKey();
            try {
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, file);
                putObjectRequest.setGeneralProgressListener(new LoggingProgressListener(getLog(), file.length()));

                getLog().info(String.format("Starting upload for bucket: [%s] key: [%s], file: [%s]", bucket,
                    key, file
                    .getPath()));

                getS3Client().putObject(putObjectRequest);
            } catch (SdkClientException e) {
                throw new MojoExecutionException(String.format("Could not upload file: [%s] to bucket: [%s] with " +
                    "remote prefix: [%s]", file.getPath(), bucket, key), e);
            }
        } else {
            getLog().warn(String.format("Target upload file does not exist, skipping: [%s]", file.getPath()));
        }
    }

    protected AmazonS3 getS3Client() {
        if (s3Client == null) {
            AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
            s3Client = AmazonS3ClientBuilder
                .standard()
                .withRegion(databricksRepoRegion)
                .withCredentials(credentialsProvider)
                .build();

        }
        return s3Client;
    }

    private static class LoggingProgressListener implements ProgressListener {

        private final Log log;
        private final long size;
        private double progress = 0;

        private long startTime = System.currentTimeMillis();
        private long lastTimeLogged = System.currentTimeMillis();

        public LoggingProgressListener(Log log, long size) {
            this.log = log;
            this.size = size;
        }

        @Override
        public void progressChanged(ProgressEvent progressEvent) {
            progress += progressEvent.getBytesTransferred();

            double percent = (progress / size) * 100;
            long now = System.currentTimeMillis();
            if (now - lastTimeLogged > 2000) {
                log.info(String.format("Transferred %.2f%% to s3, total run time: %ss.", percent, (now - startTime) / 1000));
                lastTimeLogged = now;
            }

        }
    }

}
