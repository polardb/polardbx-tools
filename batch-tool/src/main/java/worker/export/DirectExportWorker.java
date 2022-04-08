/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package worker.export;

import com.alibaba.druid.util.JdbcUtils;
import model.config.CompressMode;
import model.config.EncryptionMode;
import model.config.FileFormat;
import model.config.GlobalVar;
import model.config.QuoteEncloseMode;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import model.encrypt.Cipher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DataSourceUtil;
import util.DbUtil;
import util.FileUtil;
import worker.common.IFileWriter;
import worker.common.NioFileWriter;
import worker.common.XlsxFileWriter;
import worker.util.ExportUtil;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

/**
 * 直接拿到数据库数据并写入文件的工作线程
 * 没有输出文件数量的限制
 */
public class DirectExportWorker extends BaseExportWorker {
    private static final Logger logger = LoggerFactory.getLogger(DirectExportWorker.class);
    private static final int NO_FILE_SEQ = -1;

    /**
     * 原始指定文件名
     */
    private final String filename;
    private final IFileWriter fileWriter;
    private Cipher cipher = null;

    /**
     * 单个文件最大行数
     */
    private final int maxLine;
    /**
     * 当前写入文件的行数
     */
    private int curLineNum = 0;
    /**
     * 当前文件序号
     */
    private int curFileSeq;

    private String whereCondition;

    /**
     * 首行是否为字段名header
     * 注意与压缩模式的兼容性
     */
    private final boolean isWithHeader;
    private CountDownLatch countDownLatch;
    private Semaphore permitted;

    public DirectExportWorker(DataSource druid,
                              TableTopology topology,
                              TableFieldMetaInfo tableFieldMetaInfo,
                              String filename,
                              String separator,
                              boolean isWithHeader,
                              QuoteEncloseMode quoteEncloseMode,
                              CompressMode compressMode,
                              FileFormat fileFormat,
                              Charset charset) {
        this(druid, topology,  tableFieldMetaInfo, 0,
            filename, separator, isWithHeader, quoteEncloseMode, compressMode, fileFormat, charset);
    }

    /**
     * @param maxLine 单个文件最大行数
     */
    public DirectExportWorker(DataSource druid, TableTopology topology,
                              TableFieldMetaInfo tableFieldMetaInfo,
                              int maxLine,
                              String filename,
                              String separator,
                              boolean isWithHeader,
                              QuoteEncloseMode quoteEncloseMode,
                              CompressMode compressMode,
                              FileFormat fileFormat,
                              Charset charset) {
        super(druid, topology, tableFieldMetaInfo, separator, quoteEncloseMode, compressMode, fileFormat);
        this.maxLine = maxLine;
        this.filename = filename;
        this.isWithHeader = isWithHeader;
        if (isLimitLine()) {
            this.curFileSeq = 0;
            if (maxLine < GlobalVar.EMIT_BATCH_SIZE) {
                throw new IllegalArgumentException("Max line should be greater than batch size: "
                    + GlobalVar.EMIT_BATCH_SIZE);
            }
        } else {
            this.curFileSeq = NO_FILE_SEQ;
        }
        switch (fileFormat) {
        case XLSX:
            this.fileWriter = new XlsxFileWriter();
            break;
        default:
            this.fileWriter = new NioFileWriter(compressMode, charset);
        }
        createNewFile();
    }

    /**
     * 创建一个新的空文件
     * 会覆盖同名文件
     * 并根据开关 向文件写入字段名
     */
    private void createNewFile() {
        String tmpFileName = getTmpFileName();
        fileWriter.nextFile(tmpFileName);
        if (isWithHeader) {
            appendHeader();
        }
    }

    /**
     * 获取写入当前文件名
     */
    private String getTmpFileName() {
        if (this.curFileSeq == -1 && this.compressMode == CompressMode.NONE
            && this.fileFormat == FileFormat.NONE) {
            return this.filename;
        }
        StringBuilder fileNameBuilder = new StringBuilder(this.filename.length() + 6);
        fileNameBuilder.append(this.filename);
        if (curFileSeq != -1) {
            fileNameBuilder.append('-').append(curFileSeq);
        }
        if (this.fileFormat != FileFormat.NONE) {
            fileNameBuilder.append(fileFormat.getSuffix());
        }
        if (this.compressMode == CompressMode.GZIP) {
            fileNameBuilder.append(".gz");
        }
        return fileNameBuilder.toString();
    }

    private void appendHeader() {
        byte[] header = FileUtil.getHeaderBytes(tableFieldMetaInfo.getFieldMetaInfoList(), separator);
        fileWriter.write(header);
    }

    @Override
    public void run() {
        beforeRun();
        try {
            if (produceByLine()) {
                produceDataByLine();
            } else {
                produceData();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            afterRun();
        }
    }

    private boolean produceByLine() {
        if (fileFormat == FileFormat.XLSX) {
            return true;
        }
        if (this.cipher == null) {
            return false;
        }
        EncryptionMode encryptionMode = this.cipher.getEncryptionConfig()
            .getEncryptionMode();
        if (encryptionMode != EncryptionMode.NONE && encryptionMode != EncryptionMode.CAESAR) {
            return true;
        }
        return false;
    }

    private void beforeRun() {
        if (permitted != null) {
            permitted.acquireUninterruptibly();
        }
    }

    private void afterRun() {
        countDownLatch.countDown();
        if (permitted != null) {
            permitted.release();
        }
        fileWriter.close();
    }

    private void produceData() {
        Statement stmt = null;
        ResultSet resultSet = null;
        Connection conn = null;
        try {
            logger.info("{} 开始导出", topology);
            String sql = ExportUtil.getDirectSqlWithFormattedDate(topology,
                tableFieldMetaInfo.getFieldMetaInfoList(), whereCondition);
            // 字段数
            int colNum;
            // 已经缓存的行数
            int bufferedRowNum = 0;
            byte[] value;
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            conn = druid.getConnection();
            stmt = DataSourceUtil.createStreamingStatement(conn);
            resultSet = stmt.executeQuery(sql);
            colNum = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i < colNum; i++) {
                    value = resultSet.getBytes(i);
                    writeFieldValue(os, value, isStringTypeList.get(i - 1));
                    // 附加分隔符
                    os.write(separator);
                }
                value = resultSet.getBytes(colNum);
                writeFieldValue(os, value, isStringTypeList.get(colNum - 1));
                // 附加换行符
                os.write(FileUtil.SYS_NEW_LINE_BYTE);

                bufferedRowNum++;
                if (bufferedRowNum == GlobalVar.EMIT_BATCH_SIZE) {
                    if (isLimitLine() && curLineNum + bufferedRowNum > maxLine) {
                        // 超过了行数
                        // 新建文件
                        createNewPartFile();
                    }
                    writeToFile(os);
                    curLineNum += bufferedRowNum;
                    bufferedRowNum = 0;
                }
            }
            if (bufferedRowNum != 0) {
                // 最后剩余的元组
                if (isLimitLine() && curLineNum + bufferedRowNum > maxLine) {
                    // 超过了行数
                    // 新建文件
                    createNewPartFile();
                }
                writeToFile(os);
                bufferedRowNum = 0;
            }
            logger.info("{} 导出完成", topology);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    private void writeToFile(ByteArrayOutputStream os) {
        byte[] data = os.toByteArray();
        if (cipher != null) {
            try {
                data = cipher.encrypt(data);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
        fileWriter.write(data);
        os.reset();
    }

    /**
     * 按行从数据库中读取并写入文件
     */
    private void produceDataByLine() {
        String sql = ExportUtil.getDirectSqlWithFormattedDate(topology,
            tableFieldMetaInfo.getFieldMetaInfoList(), whereCondition);
        try (Connection conn = druid.getConnection();
            Statement stmt = DataSourceUtil.createStreamingStatement(conn);
            ResultSet rs = stmt.executeQuery(sql)) {



        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    private boolean isLimitLine() {
        return maxLine != 0;
    }

    private void createNewPartFile() {
        curFileSeq++;
        createNewFile();
        curLineNum = 0;
    }

    public String getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void setPermitted(Semaphore permitted) {
        this.permitted = permitted;
    }

    public void setCipher(Cipher cipher) {
        this.cipher = cipher;
    }
}
