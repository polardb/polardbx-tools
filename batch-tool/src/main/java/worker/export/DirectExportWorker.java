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
import model.config.GlobalVar;
import model.config.QuoteEncloseMode;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DataSourceUtil;
import util.FileUtil;
import worker.util.ExportUtil;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private final String filename;
    private FileChannel appendChannel = null;
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
    private int curFileSeq = 0;

    private String whereCondition;

    private final boolean isWithHeader;
    private CountDownLatch countDownLatch;

    private Semaphore permitted;

    public DirectExportWorker(DataSource druid,
                              TableTopology topology,
                              TableFieldMetaInfo tableFieldMetaInfo,
                              String filename,
                              String separator,
                              boolean isWithHeader,
                              QuoteEncloseMode quoteEncloseMode) {
        super(druid, topology, tableFieldMetaInfo, separator, quoteEncloseMode);
        this.filename = filename;
        this.maxLine = 0;
        this.isWithHeader = isWithHeader;
        try {
            createNewFile(filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DirectExportWorker(DataSource druid, TableTopology topology,
                              TableFieldMetaInfo tableFieldMetaInfo,
                              int maxLine,
                              String filename,
                              String separator,
                              boolean isWithHeader,
                              QuoteEncloseMode quoteEncloseMode) {
        super(druid, topology, tableFieldMetaInfo, separator, quoteEncloseMode);
        if (maxLine < GlobalVar.EMIT_BATCH_SIZE) {
            throw new IllegalArgumentException("Max line should be greater than batch size: "
                + GlobalVar.EMIT_BATCH_SIZE);
        }
        this.filename = filename;
        this.maxLine = maxLine;
        this.isWithHeader = isWithHeader;
        String tmpFileName = filename + "-" + curFileSeq;
        try {
            createNewFile(tmpFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个新的空文件
     * 会覆盖同名文件
     * 并根据开关 向文件写入字段名
     */
    private void createNewFile(String tmpFileName) throws IOException {
        this.appendChannel = FileUtil.createEmptyFileAndOpenChannel(tmpFileName);
        if (isWithHeader) {
            appendHeader();
        }
    }

    private void appendHeader() throws IOException {
        byte[] header = FileUtil.getHeaderBytes(tableFieldMetaInfo.getFieldMetaInfoList(), separator);
        writeNio(header);
    }

    @Override
    public void run() {
        beforeRun();
        try {
            produceData();
        } finally {
            afterRun();
        }
    }

    private void produceData() {
        Statement stmt = null;
        ResultSet resultSet = null;
        Connection conn = null;
        try {
            logger.info("{} 开始导出", topology);
            String sql = ExportUtil.getSqlWithFormattedDate(topology,
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
                    writeNio(os.toByteArray());
                    os.reset();
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
                writeNio(os.toByteArray());
                os.reset();
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
    }

    private boolean isLimitLine() {
        return maxLine != 0;
    }

    private void createNewPartFile() {
        curFileSeq++;
        String tmpFileName = filename + "-" + curFileSeq;
        try {
            appendChannel.close();
            createNewFile(tmpFileName);
            curLineNum = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeNio(byte[] data) {
        try {
            ByteBuffer src = ByteBuffer.wrap(data);
            int length = appendChannel.write(src);
            while (length != 0) {
                length = appendChannel.write(src);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
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
