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

import exception.S3Exception;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class S3AwsFileStorage implements FileStorage {

    private static final Logger logger = LoggerFactory.getLogger(S3AwsFileStorage.class);

    private final FileSystem fileSystem;

    public S3AwsFileStorage(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public void put(String localFile, String targetPath) {
        Path path = new Path(targetPath);
        logger.info("Start uploading file {} to {}", localFile, targetPath);

        try (OutputStream outputStream = fileSystem.create(path);
            InputStream inputStream = Files.newInputStream(Paths.get(localFile))) {

            IOUtils.copy(inputStream, outputStream);
            logger.info("File {} is uploaded to {} successfully", localFile, path);
        } catch (IOException e) {
            throw new S3Exception(e);
        }
    }

    @Override
    public void get(String targetFile, String localPath) {
        Path path = new Path(targetFile);
        logger.info("Start fetching file {} to {}", targetFile, localPath);

        try (InputStream inputStream = fileSystem.open(path);
            OutputStream outputStream = Files.newOutputStream(Paths.get(localPath))) {

            IOUtils.copy(inputStream, outputStream);
            logger.info("Fetching file {} to {} is done", targetFile, localPath);
        } catch (IOException e) {
            throw new S3Exception(e);
        }
    }

    @Override
    public void close() {

    }
}
