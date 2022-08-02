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

import model.config.CompressMode;
import model.config.FileFormat;
import model.config.GlobalVar;
import model.config.QuoteEncloseMode;
import model.db.FieldMetaInfo;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import model.encrypt.BaseCipher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DataSourceUtil;
import util.FileUtil;
import worker.common.writer.CipherLineFileWriter;
import worker.common.writer.IFileWriter;
import worker.common.writer.NioFileWriter;
import worker.common.writer.XlsxFileWriter;
import worker.util.ExportUtil;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
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
    private final BaseCipher cipher;

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

    protected String whereCondition;

    /**
     * 首行是否为字段名header
     * 注意与压缩模式的兼容性
     */
    private final boolean isWithHeader;
    private CountDownLatch countDownLatch;
    private Semaphore permitted;

    public DirectExportWorker(DataSource dataSource,
                              TableTopology topology,
                              TableFieldMetaInfo tableFieldMetaInfo,
                              String filename,
                              String separator,
                              boolean isWithHeader,
                              QuoteEncloseMode quoteEncloseMode,
                              CompressMode compressMode,
                              FileFormat fileFormat,
                              Charset charset,
                              BaseCipher cipher) {
        this(dataSource, topology,  tableFieldMetaInfo, 0,
            filename, separator, isWithHeader, quoteEncloseMode,
            compressMode, fileFormat, charset, cipher);
    }

    /**
     * @param maxLine 单个文件最大行数
     */
    public DirectExportWorker(DataSource dataSource, TableTopology topology,
                              TableFieldMetaInfo tableFieldMetaInfo,
                              int maxLine,
                              String filename,
                              String separator,
                              boolean isWithHeader,
                              QuoteEncloseMode quoteEncloseMode,
                              CompressMode compressMode,
                              FileFormat fileFormat,
                              Charset charset,
                              BaseCipher cipher) {
        super(dataSource, topology, tableFieldMetaInfo, separator, quoteEncloseMode, compressMode, fileFormat);
        this.maxLine = maxLine;
        this.filename = filename;
        this.isWithHeader = isWithHeader;
        this.cipher = cipher;
        initFileSeq();
        this.fileWriter = initFileWriter(charset);
        createNewFile();
    }

    private void initFileSeq() {
        if (isLimitLine()) {
            this.curFileSeq = 0;
            if (maxLine < GlobalVar.EMIT_BATCH_SIZE) {
                throw new IllegalArgumentException("Max line should be greater than batch size: "
                    + GlobalVar.EMIT_BATCH_SIZE);
            }
        } else {
            this.curFileSeq = NO_FILE_SEQ;
        }
    }

    private IFileWriter initFileWriter(Charset charset) {
        switch (fileFormat) {
        case XLSX:
        case XLS:
        case ET:
            return new XlsxFileWriter();
        }
        if (cipher == null || cipher.supportBlock()) {
            return new NioFileWriter(compressMode, charset);
        }
        return new CipherLineFileWriter(cipher, separator, quoteEncloseMode);
    }

    /**
     * 创建一个新的空文件
     * 会覆盖同名文件
     * 并根据开关 向文件写入字段名
     */
    private void createNewFile() {
        String tmpFileName = getTmpFilename();
        fileWriter.nextFile(tmpFileName);
        if (isWithHeader) {
            appendHeader();
        }
    }

    /**
     * 获取写入当前文件名
     */
    private String getTmpFilename() {
        if (this.curFileSeq == -1 && this.compressMode == CompressMode.NONE
            && this.fileFormat == FileFormat.NONE) {
            return this.filename;
        }
        StringBuilder filenameBuilder = new StringBuilder(this.filename.length() + 6);
        filenameBuilder.append(this.filename);
        if (curFileSeq != -1) {
            filenameBuilder.append('-').append(curFileSeq);
        }
        if (this.fileFormat != FileFormat.NONE) {
            filenameBuilder.append(fileFormat.getSuffix());
        }
        if (this.compressMode == CompressMode.GZIP) {
            filenameBuilder.append(".gz");
        }
        return filenameBuilder.toString();
    }

    private void appendHeader() {
        if (this.fileWriter.produceByBlock()) {
            byte[] header = FileUtil.getHeaderBytes(tableFieldMetaInfo.getFieldMetaInfoList(), separator);
            fileWriter.write(header);
        } else {
            String[] headerValues = tableFieldMetaInfo.getFieldMetaInfoList().stream()
                    .map(FieldMetaInfo::getName).toArray(String[]::new);
            fileWriter.writeLine(headerValues);
        }
    }

    @Override
    public void run() {
        beforeRun();
        try {
            if (this.fileWriter.produceByBlock()) {
                produceData();
            } else {
                produceDataByLine();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            afterRun();
        }
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

    @Override
    protected void emitBatchData() {
        if (isLimitLine() && curLineNum + bufferedRowNum > maxLine) {
            // 超过了行数
            // 新建文件
            createNewPartFile();
        }
        writeToFile(os);
        curLineNum += bufferedRowNum;
    }

    @Override
    protected void dealWithRemainData() {
        if (isLimitLine() && curLineNum + bufferedRowNum > maxLine) {
            // 超过了行数
            // 新建文件
            createNewPartFile();
        }
        writeToFile(os);
        bufferedRowNum = 0;
    }

    @Override
    protected String getExportSql() {
        return ExportUtil.getDirectSql(topology,
            tableFieldMetaInfo.getFieldMetaInfoList(), whereCondition);
    }

    @Override
    protected void afterProduceData() {
        logger.info("{} 导出完成", topology);
    }

    private void writeToFile(ByteArrayOutputStream os) {
        byte[] data = os.toByteArray();
        if (cipher != null) {
            try {
                data = cipher.encrypt(data);
            } catch (Exception e) {
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
        fileWriter.write(data);
    }

    /**
     * 按行从数据库中读取并写入文件
     */
    private void produceDataByLine() {
        String sql = getExportSql();
        try (Connection conn = druid.getConnection();
            Statement stmt = DataSourceUtil.createStreamingStatement(conn);
            ResultSet rs = stmt.executeQuery(sql)) {
            int colNum = rs.getMetaData().getColumnCount();
            int line = 0;
            while (rs.next()) {
                line++;
                String[] values = new String[colNum];
                for (int i = 1; i < colNum + 1; i++) {
                    String value = rs.getString(i);
                    values[i - 1] = value != null ? value : FileUtil.NULL_ESC_STR;
                }
                fileWriter.writeLine(values);
                if (line % 1000 == 0) {
                    logger.info("{} 当前已写入行数: {} ", filename, line);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
}
