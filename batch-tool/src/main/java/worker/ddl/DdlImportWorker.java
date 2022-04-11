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

package worker.ddl;

import model.config.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;
import util.IOUtil;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 直接通过source导入库表
 */
public class DdlImportWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DdlExportWorker.class);
    private final List<String> filepaths = new ArrayList<>();;
    private final DataSource dataSource;

    public DdlImportWorker(String filename, DataSource dataSource) {
        this.dataSource = dataSource;
        File file = new File(filename);
        if (!file.exists() || !file.isFile()) {
            throw new IllegalStateException("File " + filename + " does not exist");
        }
        this.filepaths.add(file.getAbsolutePath());
    }

    public DdlImportWorker(List<String> tableNames, DataSource dataSource) {
        this.dataSource = dataSource;
        for (String name : tableNames) {
            String filename = name + ConfigConstant.DDL_FILE_SUFFIX;
            String fileAbsPath = FileUtil.getFileAbsPath(filename);
            this.filepaths.add(fileAbsPath);
        }
    }

    @Override
    public void run() {
        BufferedReader reader = null;
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {

            StringBuilder sqlStringBuilder = new StringBuilder(100);
            String line = null;
            for (String filepath : filepaths) {
                reader = new BufferedReader(new FileReader(filepath));
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("--")) {
                        continue;
                    }
                    if (!line.endsWith(";")) {
                        sqlStringBuilder.append(line).append("\n");
                    } else {
                        sqlStringBuilder.append(line);
                        String sql = sqlStringBuilder.toString();
                        logger.info("Executing DDL sql: " + sql);
                        stmt.execute(sql);
                        sqlStringBuilder.setLength(0);
                    }
                }
                sqlStringBuilder.setLength(0);
                IOUtil.close(reader);
            }
        } catch (IOException | SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            IOUtil.close(reader);
        }
        logger.info("DDL语句导入完毕");
    }
}
