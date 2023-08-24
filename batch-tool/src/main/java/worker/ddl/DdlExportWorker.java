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

import exception.DatabaseException;
import model.config.ExportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import util.IOUtil;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static model.config.ConfigConstant.DDL_FILE_SUFFIX;

public class DdlExportWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DdlExportWorker.class);
    private final String filename;
    private BufferedWriter bufferedWriter = null;

    private final DataSource druid;
    private final String dbName;
    private final List<String> tableNames;
    /**
     * 是否导出整个数据库与其中的所有表
     */
    private final boolean isExportWholeDb;
    private final ExportConfig config;

    private static final Pattern DB_MODE_PATTERN = Pattern.compile("/\\* MODE = '(.*)' \\*/$");

    public DdlExportWorker(DataSource druid, String dbName, ExportConfig config) {
        this.druid = druid;
        this.dbName = dbName;
        this.config = config;
        try (Connection conn = druid.getConnection()) {
            this.tableNames = DbUtil.getAllTablesInDb(conn, dbName);
        } catch (SQLException | DatabaseException e) {
            throw new RuntimeException(e);
        }
        this.isExportWholeDb = true;
        this.filename = dbName + DDL_FILE_SUFFIX;
    }

    public DdlExportWorker(DataSource druid, String dbName, List<String> tableNames, ExportConfig config) {
        this.druid = druid;
        this.dbName = dbName;
        this.config = config;
        this.tableNames = tableNames;
        this.isExportWholeDb = false;
        this.filename = dbName + DDL_FILE_SUFFIX;
    }

    @Override
    public void run() {
        try {
            beforeRun();
            exportDdl();
        } catch (Throwable t) {
            t.printStackTrace();
            logger.error(t.getMessage());
        } finally {
            afterRun();
        }
    }

    private void exportDdl() throws Throwable {
        try (Connection conn = druid.getConnection()) {
            if (isExportWholeDb) {
                logger.info("库：{} 开始导出库结构", dbName);
                exportDatabaseStructure(conn, dbName);
            }
            for (String tableName : tableNames) {
                exportTableStructure(conn, tableName);
            }
        }
    }

    private void exportDatabaseStructure(Connection conn, String dbName) throws IOException, DatabaseException {
        writeCommentForDatabase(dbName);
        String dbDdl = DbUtil.getShowCreateDatabase(conn, dbName);
        Matcher matcher = DB_MODE_PATTERN.matcher(dbDdl);
        if (matcher.find()) {
            String mode = matcher.group(1);
            dbDdl = String.format("%s mode = '%s'", dbDdl, mode);
        }
        writeLine(dbDdl + ";");
        writeLine("");
        writeLine(String.format("use %s;", dbName));
        writeLine("");
    }

    private void exportTableStructure(Connection conn, String tableName) throws IOException, DatabaseException {
        logger.info("表：{} 开始导出表结构", tableName);

        beforeCreateTable(tableName);
        String tableDdl = DbUtil.getShowCreateTable(conn, tableName);
        writeLine(tableDdl + ";");
        writeLine("");
    }

    private void beforeCreateTable(String tableName) throws IOException {
        writeCommentForTable(tableName);
        writeDropTableIfExists(tableName);
    }

    private void writeCommentForDatabase(String dbName) throws IOException {
        writeLine("--");
        writeLine("-- Database structure for database `" + dbName + "`");
        writeLine("--");
    }

    private void writeCommentForTable(String tableName) throws IOException {
        writeLine("--");
        writeLine("-- Table structure for table `" + tableName + "`");
        writeLine("--");
    }

    private void writeDropTableIfExists(String tableName) throws IOException {
        if (!config.isDropTableIfExists()) {
            return;
        }
        writeLine(String.format("DROP TABLE IF EXISTS `" + tableName + "`;"));
    }

    private void writeLine(String line) throws IOException {
        bufferedWriter.write(line);
        bufferedWriter.newLine();
    }

    private void beforeRun() throws Exception {
        bufferedWriter = new BufferedWriter(new FileWriter(getFilepath()));
    }

    private String getFilepath() {
        return config.getPath() + filename;
    }

    private void afterRun() {
        IOUtil.close(bufferedWriter);
    }
}
