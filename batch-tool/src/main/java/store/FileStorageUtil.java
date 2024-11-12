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
import exception.S3Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.net.URI;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.S3_ACCESS_KEY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.S3_SECRET_KEY;

public class FileStorageUtil {

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

    public static FileSystem getS3AwsFileSystem(String accessKeyId, String accessKeySecret,
                                                String endPoint, String fileUri) {
        S3AFileSystem fileSystem = new S3AFileSystem();
        Configuration fsConf = new Configuration();
        fsConf.set(S3_ACCESS_KEY, accessKeyId);
        fsConf.set(S3_SECRET_KEY, accessKeySecret);
        fsConf.set("fs.s3a.endpoint", endPoint);
        try {
            URI s3FileUri = URI.create(fileUri);
            fileSystem.initialize(s3FileUri, fsConf);
            Path workingDirectory =
                new Path(URI.create(fileUri + "/"));
            fileSystem.setWorkingDirectory(workingDirectory);
        } catch (Throwable t) {
            try {
                fileSystem.close();
            } catch (Throwable t1) {
                // ignore
            }
            t.printStackTrace();
            throw new S3Exception("bad fileUri = " + fileUri, t);
        }
        return fileSystem;
    }

    public static FileSystem getS3FileSystemFromEnv() {
        String accessKeyId = System.getenv("S3_ACCESS_KEY_ID");
        if (accessKeyId == null) {
            throw new IllegalArgumentException("S3_ACCESS_KEY_ID must be set");
        }
        String accessKeySecret = System.getenv("S3_ACCESS_KEY_SECRET");
        if (accessKeySecret == null) {
            throw new IllegalArgumentException("S3_ACCESS_KEY_SECRET must be set");
        }
        String fileUri = System.getenv("S3_URI");
        if (fileUri == null) {
            throw new IllegalArgumentException("S3_URI must be set");
        }
        String endPoint = System.getenv("S3_ENDPOINT");
        if (endPoint == null) {
            throw new IllegalArgumentException("S3_ENDPOINT must be set");
        }
        return getS3AwsFileSystem(accessKeyId, accessKeySecret, endPoint, fileUri);
    }

}
