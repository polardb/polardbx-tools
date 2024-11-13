/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
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

package store;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.comm.Protocol;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import exception.S3Exception;

import java.util.List;

public class FileStorageUtil {

    static {
        init();
    }

    private static void init() {
        System.setProperty("aws.java.v1.disableDeprecationAnnouncement", "true");
    }

    public static OSSClient getS3ClientFromEnv() {
        String accessKeyId = System.getenv("S3_ACCESS_KEY_ID");
        if (accessKeyId == null) {
            throw new IllegalArgumentException("S3_ACCESS_KEY_ID must be set");
        }
        String accessKeySecret = System.getenv("S3_ACCESS_KEY_SECRET");
        if (accessKeySecret == null) {
            throw new IllegalArgumentException("S3_ACCESS_KEY_SECRET must be set");
        }
        String endPoint = System.getenv("S3_ENDPOINT");
        if (endPoint == null) {
            throw new IllegalArgumentException("S3_ENDPOINT must be set");
        }

        ClientBuilderConfiguration clientConf = new ClientBuilderConfiguration();
        clientConf.setMaxConnections(1024);
        clientConf.setProtocol(Protocol.HTTPS);
        clientConf.setMaxErrorRetry(3);
        clientConf.setConnectionTimeout(3000);
        clientConf.setSocketTimeout(30000);

        OSSClient ossClient =
            (OSSClient) new OSSClientBuilder().build(endPoint, accessKeyId, accessKeySecret, null, clientConf);
        return ossClient;
    }

    public static AmazonS3 getS3AwsFileSystem(String accessKeyId, String accessKeySecret,
                                              String endPoint, String bucketName) {

        ClientConfiguration config = new ClientConfiguration();
        AwsClientBuilder.EndpointConfiguration endpointConfig =
            new AwsClientBuilder.EndpointConfiguration(endPoint, null);

        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, accessKeySecret);
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);

        AmazonS3 s3 = AmazonS3Client.builder()
            .withEndpointConfiguration(endpointConfig)
            .withClientConfiguration(config)
            .withCredentials(awsCredentialsProvider)
            .disableChunkedEncoding()
            .withPathStyleAccessEnabled(false)
            .build();

        // check bucket exists
        if (!s3.doesBucketExistV2(bucketName)) {
            throw new S3Exception("Bucket " + bucketName + " does not exist");
        }
        return s3;
    }

    public static AmazonS3 getS3FileSystemFromEnv() {
        String accessKeyId = System.getenv("S3_ACCESS_KEY_ID");
        if (accessKeyId == null) {
            throw new IllegalArgumentException("S3_ACCESS_KEY_ID must be set");
        }
        String accessKeySecret = System.getenv("S3_ACCESS_KEY_SECRET");
        if (accessKeySecret == null) {
            throw new IllegalArgumentException("S3_ACCESS_KEY_SECRET must be set");
        }
        String bucket = System.getenv("S3_BUCKET");
        if (bucket == null) {
            throw new IllegalArgumentException("S3_BUCKET must be set");
        }
        String endPoint = System.getenv("S3_ENDPOINT");
        if (endPoint == null) {
            throw new IllegalArgumentException("S3_ENDPOINT must be set");
        }
        return getS3AwsFileSystem(accessKeyId, accessKeySecret, endPoint, bucket);
    }

    public static List<String> listFiles(FileStorage fileStorage, String prefix) {
        List<String> filenames = fileStorage.listFiles(prefix);
        return filenames;
    }
}
