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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import exception.S3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class S3AwsFileStorage implements FileStorage {

    private static final Logger logger = LoggerFactory.getLogger(S3AwsFileStorage.class);

    private final AmazonS3 s3Client;
    private final String bucketName;

    public S3AwsFileStorage(AmazonS3 s3Client) {
        this.s3Client = s3Client;
        String bucketName = System.getenv("S3_BUCKET");
        if (bucketName == null) {
            throw new IllegalArgumentException("S3_BUCKET must be set");
        }
        this.bucketName = bucketName;
    }

    @Override
    public void put(String localFile, String targetPath) {
        try {
            logger.info("开始上传文件 {} 至 {}/{}", localFile, bucketName, targetPath);
            PutObjectResult putObjectResult = s3Client.putObject(bucketName, targetPath, new File(localFile));
            logger.info("文件 {} 上传成功", localFile);
        } catch (Exception e) {
            e.printStackTrace();
            throw new S3Exception(e);
        }
    }

    @Override
    public void get(String targetFile, String localPath) {
        try {
            logger.info("开始加载文件 {}/{} 至 {}", bucketName, targetFile, localPath);
            s3Client.getObject(new GetObjectRequest(bucketName, targetFile), new File(localPath));
            logger.info("文件 {} 缓冲完成", localPath);
        } catch (Exception e) {
            e.printStackTrace();
            throw new S3Exception(e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public List<String> listFiles(String prefix) {
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix(prefix);

        ObjectListing objects = s3Client.listObjects(request);
        if (objects.isTruncated()) {
            throw new UnsupportedOperationException("The number of files exceeds " + objects.getMaxKeys());
        }
        List<String> filenames = objects.getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey)
            .collect(java.util.stream.Collectors.toList());
        return filenames;
    }
}
