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
import model.config.FileLineRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;
import worker.MyThreadPool;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static model.config.GlobalVar.DDL_PARALLELISM;
import static model.config.GlobalVar.DDL_RETRY_COUNT;

/**
 * 直接通过读取SQL导入库表
 */
public class DdlImportWorker {

    private static final Logger logger = LoggerFactory.getLogger(DdlExportWorker.class);
    private static final int MAX_SQL_SAMPLE_LEN = 50;

    private final List<String> filepaths = new ArrayList<>();
    private final DataSource dataSource;
    private final ExecutorService ddlThreadPool =
        MyThreadPool.createUnboundedFixedExecutor("DDL-importer", DDL_PARALLELISM);
    private final AtomicInteger taskCount = new AtomicInteger(0);
    private volatile String useDbSql = null;

    public static DdlImportWorker fromFiles(List<FileLineRecord> fileRecords, DataSource dataSource) {
        DdlImportWorker worker = new DdlImportWorker(dataSource);
        for (FileLineRecord fileRecord : fileRecords) {
            String filePath = fileRecord.getFilePath();
            File file = new File(fileRecord.getFilePath());
            if (!file.exists() || !file.isFile()) {
                throw new IllegalStateException("File " + filePath + " does not exist");
            }
            worker.filepaths.add(file.getAbsolutePath());
        }
        return worker;
    }

    public static DdlImportWorker fromTables(List<String> tableNames, DataSource dataSource) {
        DdlImportWorker worker = new DdlImportWorker(dataSource);
        for (String name : tableNames) {
            String filename = name + ConfigConstant.DDL_FILE_SUFFIX;
            String fileAbsPath = FileUtil.getFileAbsPath(filename);
            worker.filepaths.add(fileAbsPath);
        }
        return worker;
    }

    DdlImportWorker(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 异步导入DDL建表语句
     */
    public synchronized void doImportSync() {
        logger.info("Importing ddl with parallelism: {}, retry count: {}", DDL_PARALLELISM, DDL_RETRY_COUNT);

        if (ddlThreadPool.isShutdown()) {
            throw new IllegalStateException("ddl thread pool has been shutdown");
        }
        StringBuilder sqlStringBuilder = new StringBuilder(100);

        try {
            for (String filepath : filepaths) {
                try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
                    processLines(reader, sqlStringBuilder);
                    sqlStringBuilder.setLength(0);
                }
            }
            sqlStringBuilder.setLength(0);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        ddlThreadPool.shutdown();
        int waitCount = 0;
        while ((waitCount = taskCount.get()) != 0) {
            logger.info("等待DDL导入结束，剩余任务数：{}", waitCount);
            int sleepTimeMillis = (waitCount / 50 + 1) * 3000;
            try {
                Thread.sleep(sleepTimeMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("DDL语句导入完毕");
    }

    private void processLines(BufferedReader reader, StringBuilder sqlStringBuilder) throws IOException {
        String line = null;
        String lastSql = null;
        boolean firstLine = true;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("--") || line.isEmpty()) {
                continue;
            }
            if (!line.endsWith(";")) {
                sqlStringBuilder.append(line).append("\n");
            } else {
                // 拼接完整sql
                sqlStringBuilder.append(line);
                String sql = sqlStringBuilder.toString();

                if (firstLine && (sql.contains("DATABASE") || sql.contains("database"))) {
                    importDDL(sql);
                } else if (useDbSql == null && (sql.startsWith("use"))) {
                    useDbSql = sql;
                } else if (StringUtils.startsWithIgnoreCase(sql, "drop ") && lastSql == null) {
                    // drop 语句放在 create 语句之前执行
                    lastSql = sql;
                } else {
                    if (lastSql != null) {
                        submitDDL(lastSql, sql);
                        lastSql = null;
                    } else {
                        submitDDL(sql);
                    }
                }
                firstLine = false;
                sqlStringBuilder.setLength(0);
            }
        }

    }

    private void importDDL(String sql) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            logger.info("正在执行 DDL 语句: {}", sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void submitDDL(String... sqls) {
        taskCount.incrementAndGet();
        ddlThreadPool.submit(() -> {
            String sql = null, ddlSample = null;
            try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
                for (String s : sqls) {
                    sql = s;
                    ddlSample = sql.substring(0, Math.min(MAX_SQL_SAMPLE_LEN, sql.length()));
                    logger.info("正在执行 DDL 语句: {} ...", ddlSample);
                    if (useDbSql != null) {
                        stmt.execute(useDbSql);
                    }
                    stmt.execute(sql);
                }
            } catch (SQLException e) {
                String msg = e.getMessage();
                try (Connection conn = dataSource.getConnection();
                    Statement stmt = conn.createStatement()) {
                    if (!StringUtils.containsIgnoreCase(msg, "already exists")) {
                        int retryCount = 0;
                        for (; retryCount < DDL_RETRY_COUNT; retryCount++) {
                            try {
                                logger.info("正在重试 DDL 语句: {} ...", ddlSample);
                                stmt.execute(sql);
                                break;
                            } catch (SQLException e2) {
                                e2.printStackTrace();
                            }
                        }
                        if (retryCount < DDL_RETRY_COUNT) {
                            // 重试成功
                            return;
                        }
                    }
                } catch (Exception e1) {
                    logger.error("Failed to retry due to [{}]", e1.getMessage());
                }
                logger.error("Failed to import DDL: [{}] due to [{}]", sql, msg);
            } finally {
                taskCount.decrementAndGet();
            }
        });
    }
}
