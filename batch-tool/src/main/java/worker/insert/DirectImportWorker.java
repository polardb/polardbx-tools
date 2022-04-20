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

package worker.insert;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import model.config.FileLineRecord;
import model.config.GlobalVar;
import model.db.FieldMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import util.IOUtil;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * 按行读取并且按行
 * 支持断点记录与恢复
 * 性能较差
 */
public class DirectImportWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DirectImportWorker.class);

    private final DataSource dataSource;
    private final char sep;
    private final List<FileLineRecord> fileRecords;
    private final String tableName;
    private final Charset charset;
    private final List<FieldMetaInfo> fieldMetaInfoList;
    private final int maxErrorCount;

    private final int reportLine;

    public DirectImportWorker(DataSource dataSource, String sep,
                              Charset charset,
                              List<FileLineRecord> fileRecords, String tableName,
                              List<FieldMetaInfo> fieldMetaInfoList,
                              int maxErrorCount) {
        if (sep.length() != 1) {
            throw new RuntimeException("Only allows single char separator in safe mode");
        }
        this.sep = sep.charAt(0);
        this.fileRecords = fileRecords;
        this.dataSource = dataSource;
        this.charset = charset;
        this.fieldMetaInfoList = fieldMetaInfoList;
        this.tableName = tableName;
        this.maxErrorCount = maxErrorCount;
        this.reportLine = GlobalVar.EMIT_BATCH_SIZE * 10;
    }

    @Override
    public void run() {
        CSVParser parser = new CSVParserBuilder().withSeparator(sep).build();
        int curLine = 0;
        int curErrorCount = 0;
        String curFile = null;
        String prepareSql = DbUtil.getPrepareInsertSql(tableName, fieldMetaInfoList.size(), true);
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(prepareSql)) {
            for (FileLineRecord fileRecord : fileRecords) {
                String filePath = fileRecord.getFilePath();
                int startLine = fileRecord.getStartLine();
                curFile = filePath;
                curLine = startLine;
                int importedLines = 0;
                CSVReader reader = new CSVReaderBuilder(new InputStreamReader(
                    new FileInputStream(filePath), charset))
                    .withCSVParser(parser).build();
                reader.skip(startLine - 1);
                for (String[] values; (values = reader.readNext()) != null; ) {
                    for (int i1 = 0; i1 < values.length; i1++) {
                        stmt.setObject(i1 + 1, values[i1]);
                    }
                    try {
                        // Batch prepare 导入
                        stmt.addBatch();
                        importedLines++;
                        if (importedLines % GlobalVar.EMIT_BATCH_SIZE == 0) {
                            stmt.executeBatch();
                            stmt.clearBatch();
                        }
                        if (importedLines % reportLine == 0) {
                            logger.info("文件 {} 已导入 {} 行", curFile, importedLines);
                        }
                    } catch (SQLException e) {
                        curErrorCount++;
                        logger.error("Failed in file {} at line {}, current error times: {}, error msg: {}",
                            curFile, curLine, curErrorCount, e.getMessage());
                        if (curErrorCount > maxErrorCount) {
                            throw e;
                        }
                    }
                    curLine++;
                }
                IOUtil.close(reader);
                logger.info("文件 {} 导入成功!", curFile);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
