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

package worker.export.order;

import com.alibaba.druid.util.JdbcUtils;
import model.config.ConfigConstant;
import model.config.GlobalVar;
import model.db.TableFieldMetaInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DataSourceUtil;
import util.FileUtil;
import worker.util.ExportUtil;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static model.config.GlobalVar.EMIT_BATCH_SIZE;

public class DirectOrderByExportWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DirectOrderByExportWorker.class);
    private final String filename;
    private FileChannel appendChannel = null;
    private String whereCondition;
    /**
     * 单个文件最大行数
     */
    private final int maxLine;
    private final DataSource druid;
    private final String tableName;
    private final TableFieldMetaInfo tableFieldMetaInfo;
    private final List<String> orderByColumnName;
    private final byte[] separator;
    /**
     * 当前写入文件的行数
     */
    private int curLineNum = 0;
    /**
     * 当前文件序号
     */
    private int curFileSeq = 0;
    /**
     * 默认升序
     */
    private final boolean isAscending;

    private ByteArrayOutputStream os = new ByteArrayOutputStream();
    /**
     * 已经缓存的行数
     */
    private int bufferedRowNum = 0;

    public DirectOrderByExportWorker(DataSource druid, String filename, TableFieldMetaInfo tableFieldMetaInfo,
                                     String tableName, List<String> orderByColumnName,
                                     int maxLine, byte[] separator, boolean isAscending) {
        this.filename = filename;
        this.tableFieldMetaInfo = tableFieldMetaInfo;
        this.tableName = tableName;
        this.druid = druid;

        this.orderByColumnName = orderByColumnName;
        this.maxLine = maxLine;
        this.separator = separator;
        this.isAscending = isAscending;
        String tmpFileName = filename + curFileSeq;
        File file = new File(tmpFileName);
        try {
            // 若文件存在则覆盖
            FileUtils.deleteQuietly(file);
            file.createNewFile();
            appendChannel = FileChannel.open(Paths.get(tmpFileName),
                StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void produceData() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String sql = ExportUtil.getDirectOrderBySql(tableName, tableFieldMetaInfo.getFieldMetaInfoList(),
            orderByColumnName, whereCondition, isAscending);
        long startTime = System.currentTimeMillis();
        os = new ByteArrayOutputStream();
        try {
            conn = druid.getConnection();
            stmt = DataSourceUtil.createStreamingStatement(conn);
            resultSet = stmt.executeQuery(sql);
            byte[] value;

            bufferedRowNum = 0;
            int colNum = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i < colNum; i++) {
                    value = resultSet.getBytes(i);
                    FileUtil.writeToByteArrayStream(os, value);
                    // 附加分隔符
                    os.write(separator);
                }
                value = resultSet.getBytes(colNum);
                FileUtil.writeToByteArrayStream(os, value);
                // 附加换行符
                os.write(FileUtil.SYS_NEW_LINE_BYTE);

                bufferedRowNum++;
                curLineNum++;
                if (bufferedRowNum == EMIT_BATCH_SIZE) {
                    writeNio(os.toByteArray());
                    os.reset();
                    bufferedRowNum = 0;
                }
                if (isLimitLine() && curLineNum == maxLine) {
                    // 开启新文件
                    createNewPartFile();
                }
            }
            if (bufferedRowNum != 0) {
                // 最后剩余的元组
                writeNio(os.toByteArray());
                os.reset();
            }
            long endTime = System.currentTimeMillis();
            logger.debug("order by 发送完成，耗时 {} s", (endTime - startTime) / 1000F);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            clearEmptyFile();
            JdbcUtils.close(resultSet);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    private void clearEmptyFile() {
        try {
            appendChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (curLineNum == 0) {
            File file = new File(filename + curFileSeq);
            FileUtils.deleteQuietly(file);
        }
    }

    private boolean isLimitLine() {
        return maxLine != 0;
    }

    private void createNewPartFile() {
        if (bufferedRowNum != 0) {
            writeNio(os.toByteArray());
            os.reset();
            bufferedRowNum = 0;
        }
        curFileSeq++;
        String tmpFileName = filename + curFileSeq;
        File file = new File(tmpFileName);
        try {
            appendChannel.close();
            FileUtils.deleteQuietly(file);
            file.createNewFile();
            appendChannel = FileChannel.open(Paths.get(tmpFileName),
                StandardOpenOption.APPEND);
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

    @Override
    public void run() {
        produceData();
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }
}
