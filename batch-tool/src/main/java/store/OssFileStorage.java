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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.google.common.base.Preconditions;
import exception.S3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class OssFileStorage implements FileStorage {

    private static final Logger logger = LoggerFactory.getLogger(OssFileStorage.class);

    private final OSSClient ossClient;
    private final String bucketName;

    public OssFileStorage(OSSClient ossClient) {
        Preconditions.checkNotNull(ossClient);
        this.ossClient = ossClient;
        String bucketName = System.getenv(FileStorageUtil.OSS_BUCKET);
        if (bucketName == null) {
            throw new IllegalArgumentException("S3_BUCKET must be set");
        }
        this.bucketName = bucketName;
    }

    @Override
    public void put(String localFile, String targetPath) {
        try {
            logger.info("开始上传文件 {} 至 {}/{}", localFile, bucketName, targetPath);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, targetPath, new File(localFile));
            PutObjectResult result = ossClient.putObject(putObjectRequest);
            logger.info("文件 {} 上传成功", localFile);
        } catch (Exception e) {
            e.printStackTrace();
            throw new S3Exception(e);
        }
    }

    @Override
    public void get(String targetFile, String localPath) {
        try {
            logger.info("开始下载文件 {}/{} 至 {}", bucketName, targetFile, localPath);
            ossClient.getObject(new GetObjectRequest(bucketName, targetFile), new File(localPath));
            logger.info("文件 {}/{} 下载至 {} 完成", bucketName, targetFile, localPath);
        }  catch (Exception e) {
            e.printStackTrace();
            throw new S3Exception(e);
        }
    }

    @Override
    public void close() {
        try {
            ossClient.shutdown();
        } catch (Exception e) {
            // ignore
            e.printStackTrace();
        }
    }

    @Override
    public List<String> listFiles(String prefix) {
        String nextMarker = null;
        ObjectListing objectListing;
        List<String> filenames = new ArrayList<>();

        do {
            objectListing = ossClient.listObjects(new ListObjectsRequest(bucketName).withPrefix(prefix)
                .withMarker(nextMarker).withMaxKeys(1000));

            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary sum : sums) {
                filenames.add(sum.getKey());
            }
            nextMarker = objectListing.getNextMarker();

        } while (objectListing.isTruncated());

        return filenames;
    }
}
