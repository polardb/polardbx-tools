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

    public static final String S3_AK_ID = "S3_ACCESS_KEY_ID";
    public static final String S3_AK_SECRET = "S3_ACCESS_KEY_SECRET";
    public static final String S3_ENDPOINT = "S3_ENDPOINT";
    public static final String S3_BUCKET = "S3_BUCKET";

    public static final String OSS_AK_ID = "OSS_ACCESS_KEY_ID";
    public static final String OSS_AK_SECRET = "OSS_ACCESS_KEY_SECRET";
    public static final String OSS_ENDPOINT = "OSS_ENDPOINT";
    public static final String OSS_BUCKET = "OSS_BUCKET";

    static {
        init();
    }

    private static void init() {
        System.setProperty("aws.java.v1.disableDeprecationAnnouncement", "true");
    }

    public static OSSClient getOssClientFromEnv() {
        String accessKeyId = System.getenv(OSS_AK_ID);
        if (accessKeyId == null) {
            notSetException(OSS_AK_ID);
        }
        String accessKeySecret = System.getenv(OSS_AK_SECRET);
        if (accessKeySecret == null) {
            notSetException(OSS_AK_SECRET);
        }
        String endPoint = System.getenv(OSS_ENDPOINT);
        if (endPoint == null) {
            notSetException(OSS_ENDPOINT);
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

    private static void notSetException(String varName) {
        throw new IllegalArgumentException(varName + " must be set");
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
        String accessKeyId = System.getenv(S3_AK_ID);
        if (accessKeyId == null) {
            notSetException(S3_AK_ID);
        }
        String accessKeySecret = System.getenv(S3_AK_SECRET);
        if (accessKeySecret == null) {
            notSetException(S3_AK_SECRET);
        }
        String endPoint = System.getenv(S3_ENDPOINT);
        if (endPoint == null) {
            notSetException(S3_ENDPOINT);
        }
        String bucket = System.getenv(S3_BUCKET);
        if (bucket == null) {
            notSetException(S3_BUCKET);
        }
        return getS3AwsFileSystem(accessKeyId, accessKeySecret, endPoint, bucket);
    }

    public static List<String> listFiles(FileStorage fileStorage, String prefix) {
        List<String> filenames = fileStorage.listFiles(prefix);
        return filenames;
    }
}
