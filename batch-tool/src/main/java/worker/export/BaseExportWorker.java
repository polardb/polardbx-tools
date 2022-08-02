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
import model.mask.AbstractDataMasker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DataSourceUtil;
import util.FileUtil;
import util.IOUtil;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseExportWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BaseExportWorker.class);

    protected final DataSource druid;
    protected final TableTopology topology;
    protected final TableFieldMetaInfo tableFieldMetaInfo;
    protected final byte[] separator;

    protected final List<byte[]> specialCharList;
    protected final QuoteEncloseMode quoteEncloseMode;
    protected CompressMode compressMode;
    protected FileFormat fileFormat;

    protected final List<Boolean> isStringTypeList;

    protected Map<Integer, AbstractDataMasker> columnDataMasker;
    protected ByteArrayOutputStream os;
    protected int bufferedRowNum = 0;       // 已经缓存的行数

    protected BaseExportWorker(DataSource druid, TableTopology topology,
                               TableFieldMetaInfo tableFieldMetaInfo,
                               String separator, QuoteEncloseMode quoteEncloseMode) {
        this(druid, topology, tableFieldMetaInfo, separator, quoteEncloseMode, CompressMode.NONE, FileFormat.NONE);
    }

    protected BaseExportWorker(DataSource druid, TableTopology topology,
                               TableFieldMetaInfo tableFieldMetaInfo,
                               String separator, QuoteEncloseMode quoteEncloseMode,
                               CompressMode compressMode, FileFormat fileFormat) {

        this.druid = druid;
        this.topology = topology;
        this.tableFieldMetaInfo = tableFieldMetaInfo;

        this.separator = separator.getBytes();
        this.specialCharList = new ArrayList<>();
        specialCharList.add(this.separator);
        specialCharList.add(FileUtil.CR_BYTE);
        specialCharList.add(FileUtil.LF_BYTE);
        specialCharList.add(FileUtil.DOUBLE_QUOTE_BYTE);

        this.quoteEncloseMode = quoteEncloseMode;
        this.isStringTypeList = tableFieldMetaInfo.getFieldMetaInfoList().stream()
            .map(info -> (info.getType() == FieldMetaInfo.Type.STRING))
            .collect(Collectors.toList());

        switch (compressMode) {
        case NONE:
        case GZIP:
            this.compressMode = compressMode;
            break;
        default:
            throw new IllegalArgumentException("Unsupported compression mode: " + compressMode.name());
        }
        this.fileFormat = fileFormat;
    }

    protected void produceData() {
        String sql = getExportSql();

        try (Connection conn = druid.getConnection();
            Statement stmt = DataSourceUtil.createStreamingStatement(conn);
            ResultSet resultSet = stmt.executeQuery(sql)) {

            logger.info("{} 开始执行导出", topology);

            byte[] value;
            int colNum = resultSet.getMetaData().getColumnCount();
            this.os = new ByteArrayOutputStream(colNum * 16);
            while (resultSet.next()) {
                for (int i = 1; i < colNum; i++) {
                    value = resultSet.getBytes(i);
                    writeFieldValue(os, value, i - 1);
                    // 附加分隔符
                    os.write(separator);
                }
                value = resultSet.getBytes(colNum);
                writeFieldValue(os, value, colNum - 1);
                // 附加换行符
                os.write(FileUtil.SYS_NEW_LINE_BYTE);
                bufferedRowNum++;

                if (bufferedRowNum == GlobalVar.EMIT_BATCH_SIZE) {
                    emitBatchData();
                    os.reset();
                    bufferedRowNum = 0;
                }
            }
            if (bufferedRowNum != 0) {
                // 最后剩余的元组
                dealWithRemainData();
                os.reset();
            }
            afterProduceData();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            logger.error("{} 导出发生错误: {}", topology, e.getMessage());
        } finally {
            IOUtil.close(os);
        }
    }

    protected void afterProduceData() {
    }

    protected abstract void emitBatchData();

    protected abstract void dealWithRemainData();

    protected abstract String getExportSql();

    /**
     * 根据引号模式来写入字段值
     * @param columnIdx 从 0 开始
     */
    protected void writeFieldValue(ByteArrayOutputStream os, byte[] value, int columnIdx) throws IOException {
        boolean isStringType = isStringTypeList.get(columnIdx);
        switch (quoteEncloseMode) {
        case NONE:
            FileUtil.writeToByteArrayStream(os, value);
            break;
        case FORCE:
            FileUtil.writeToByteArrayStreamWithQuote(os, value);
            break;
        case AUTO:
            if (!isStringType) {
                FileUtil.writeToByteArrayStream(os, value);
            } else {
                // 检查是否有特殊字符
                boolean needQuote = FileUtil.containsSpecialBytes(value, specialCharList);

                if (needQuote) {
                    FileUtil.writeToByteArrayStreamWithQuote(os, value);
                } else {
                    FileUtil.writeToByteArrayStream(os, value);
                }
            }
            break;
        }
    }

    public void setCompressMode(CompressMode compressMode) {
        this.compressMode = compressMode;
    }

    public void setColumnDataMasker(Map<Integer, AbstractDataMasker> columnDataMasker) {
        this.columnDataMasker = columnDataMasker;
    }
}
