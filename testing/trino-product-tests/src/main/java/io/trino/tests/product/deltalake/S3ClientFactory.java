/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.deltalake;

import io.trino.testing.minio.MinioClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.minio.MinioClient.DEFAULT_MINIO_ACCESS_KEY;
import static io.trino.testing.minio.MinioClient.DEFAULT_MINIO_SECRET_KEY;

final class S3ClientFactory
{
    public static final String AWS_S3_SERVER_TYPE = "aws";
    public static final String MINIO_S3_SERVER_TYPE = "minio";

    private S3ClientFactory() {}

    public static S3Client createS3Client(String serverType)
    {
        return switch (serverType) {
            case AWS_S3_SERVER_TYPE -> createAwsS3Client();
            case MINIO_S3_SERVER_TYPE -> createMinioS3Client();
            default -> throw new IllegalArgumentException("Invalid value '" + serverType + "' for the s3 server type");
        };
    }

    private static S3Client createAwsS3Client()
    {
        String region = requireEnv("AWS_REGION");
        return S3Client.builder().region(Region.of(region)).build();
    }

    private static S3Client createMinioS3Client()
    {
        AwsCredentials credentials = AwsBasicCredentials.create(DEFAULT_MINIO_ACCESS_KEY, DEFAULT_MINIO_SECRET_KEY);
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

        return S3Client.builder()
                .endpointOverride(URI.create(MinioClient.DEFAULT_MINIO_ENDPOINT))
                .region(Region.US_EAST_2)
                .forcePathStyle(true)
                .credentialsProvider(credentialsProvider)
                .build();
    }
}
