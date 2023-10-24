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
import com.opencsv.exceptions.CsvValidationException;
import exception.DatabaseException;
import model.ConsumerExecutionContext;
import model.ProducerExecutionContext;
import model.config.FileLineRecord;
import model.config.GlobalVar;
import model.db.FieldMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IOUtil;
import worker.util.ImportUtil;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * 按行读取并且逐行导入
 * 支持断点记录与恢复
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
    private final boolean sqlEscapeEnabled;
    private final boolean emptyStrAsNull;

    public DirectImportWorker(DataSource dataSource,
                              String tableName,
                              ProducerExecutionContext producerContext,
                              ConsumerExecutionContext consumerContext) {
        if (producerContext.getSeparator().length() != 1) {
            throw new RuntimeException("Only allows single char separator in safe mode");
        }
        this.sep = producerContext.getSeparator().charAt(0);
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.fileRecords = producerContext.getDataFileLineRecordList();
        this.charset = producerContext.getCharset();
        this.fieldMetaInfoList = consumerContext.getTableFieldMetaInfo(tableName)
            .getFieldMetaInfoList();
        this.maxErrorCount = producerContext.getMaxErrorCount();
        this.reportLine = GlobalVar.EMIT_BATCH_SIZE * 10;
        this.sqlEscapeEnabled = consumerContext.isSqlEscapeEnabled();
        this.emptyStrAsNull = consumerContext.isEmptyStrAsNull();
    }

    @Override
    public void run() {
        CSVParser parser = new CSVParserBuilder().withSeparator(sep).build();
        int curLine = 0;
        int curErrorCount = 0;
        String curFile = null;
        int importedLines = 0;

        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            StringBuilder insertSqlBuilder = new StringBuilder(256);

            for (FileLineRecord fileRecord : fileRecords) {
                insertSqlBuilder.setLength(0);
                String filePath = fileRecord.getFilePath();
                int startLine = fileRecord.getStartLine();
                curFile = filePath;
                curLine = startLine;
                CSVReader reader = new CSVReaderBuilder(new InputStreamReader(
                    Files.newInputStream(Paths.get(filePath)), charset))
                    .withCSVParser(parser).build();
                reader.skip(startLine - 1);
                for (String[] values; (values = reader.readNext()) != null; ) {
                    try {
                        ImportUtil.getDirectImportSql(insertSqlBuilder, tableName,
                            fieldMetaInfoList, Arrays.asList(values), sqlEscapeEnabled, emptyStrAsNull);

                        stmt.execute(insertSqlBuilder.toString());
                        importedLines++;
                        if (importedLines % reportLine == 0) {
                            logger.info("文件 {} 已导入 {} 行", curFile, importedLines);
                        }
                    } catch (SQLException | DatabaseException e) {
                        curErrorCount++;
                        logger.error("Failed in file {} at line {}, current error times: {}, error msg: {}",
                            curFile, curLine, curErrorCount, e.getMessage());
                        if (curErrorCount > maxErrorCount) {
                            IOUtil.close(reader);
                            throw e;
                        }
                    }
                    curLine++;
                    insertSqlBuilder.setLength(0);
                }
                IOUtil.close(reader);
                logger.info("文件 {} 导入成功!", curFile);
            }
        } catch (SQLException | DatabaseException e) {
            logger.error("Import worker exited with exception, current imported lines: {}", importedLines);
            throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            logger.error("CSV format invalid {} at line: {}", e.getMessage(), curLine);
            throw new RuntimeException(e);
        }
    }
}
