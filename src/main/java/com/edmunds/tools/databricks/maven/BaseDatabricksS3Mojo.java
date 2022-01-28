package com.edmunds.tools.databricks.maven;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.File;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

public abstract class BaseDatabricksS3Mojo extends BaseDatabricksMojo {
    protected AmazonS3 s3Client;

    protected abstract File getSourceFile();

    protected String createSourceFilePath() throws MojoExecutionException {
        return createDeployedArtifactPath();
    }

    @Override
    public void execute() throws MojoExecutionException {
        if (getSourceFile().exists()) {
            AmazonS3URI uri = new AmazonS3URI(createSourceFilePath());
            String bucket = uri.getBucket();
            String key = uri.getKey();
            try {
                PutObjectRequest putObjectRequest =
                        new PutObjectRequest(bucket, key, getSourceFile())
                                .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl);
                putObjectRequest.setGeneralProgressListener(
                        new LoggingProgressListener(getLog(), getSourceFile().length()));

                getLog().info(String.format("Starting upload for bucket: [%s] key: [%s], file: [%s]",
                        bucket, key, getSourceFile().getPath()));

                getS3Client().putObject(putObjectRequest);
            } catch (SdkClientException e) {
                throw new MojoExecutionException(String.format("Could not upload file: [%s] to bucket: [%s] with "
                        + "remote prefix: [%s]", getSourceFile().getPath(), bucket, key), e);
            }
        } else {
            getLog().warn(
                    String.format("Target upload file does not exist, skipping: [%s]", getSourceFile().getPath()));
        }
    }

    protected AmazonS3 getS3Client() {
        if (s3Client == null) {
            AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
            s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(getDatabricksRepoRegion())
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
                log.info(String.format("Transferred %.2f%% to s3, total run time: %ss.",
                        percent, (now - startTime) / 1000));
                lastTimeLogged = now;
            }

        }
    }
}
