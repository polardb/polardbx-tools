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
import model.config.QuoteEncloseMode;
import model.db.FieldMetaInfo;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import util.FileUtil;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseExportWorker implements Runnable {
    protected final DataSource druid;
    protected final TableTopology topology;
    protected final TableFieldMetaInfo tableFieldMetaInfo;
    protected final byte[] separator;

    protected final List<byte[]> specialCharList;
    protected final QuoteEncloseMode quoteEncloseMode;
    protected CompressMode compressMode;

    protected final List<Boolean> isStringTypeList;

    protected BaseExportWorker(DataSource druid, TableTopology topology,
                               TableFieldMetaInfo tableFieldMetaInfo,
                               String separator, QuoteEncloseMode quoteEncloseMode) {
        this(druid, topology, tableFieldMetaInfo, separator, quoteEncloseMode, CompressMode.NONE);
    }

    protected BaseExportWorker(DataSource druid, TableTopology topology,
                               TableFieldMetaInfo tableFieldMetaInfo,
                               String separator, QuoteEncloseMode quoteEncloseMode,
                               CompressMode compressMode) {

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
    }

    /**
     * 根据引号模式来写入字段值
     * todo 字段值本身包含引号 还需要对引号重新转义
     */
    protected void writeFieldValue(ByteArrayOutputStream os, byte[] value, boolean isStringType) throws IOException {
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

}
