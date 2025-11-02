    package com.zephyrus.S3;

    import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
    import software.amazon.awssdk.regions.Region;
    import software.amazon.awssdk.services.s3.S3Client;

    public class S3Config {
        public static S3Client getS3Client() {
            return S3Client.builder()
                    .region(Region.US_EAST_1)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
        }
    }
