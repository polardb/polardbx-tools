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

import model.config.CompressMode;
import model.config.FileFormat;
import model.config.QuoteEncloseMode;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import model.encrypt.BaseCipher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.export.DirectExportWorker;
import worker.util.ExportUtil;

import javax.sql.DataSource;
import java.nio.charset.Charset;
import java.util.List;

public class DirectOrderExportWorker extends DirectExportWorker {
    private static final Logger logger = LoggerFactory.getLogger(DirectOrderExportWorker.class);
    /**
     * 单个文件最大行数
     */
    private final List<String> orderByColumnName;
    /**
     * 默认升序
     */
    private final boolean isAscending;

    public DirectOrderExportWorker(DataSource dataSource, String filename,
                                   TableFieldMetaInfo tableFieldMetaInfo,
                                   String tableName, List<String> orderByColumnName,
                                   int maxLine, Charset charset, String separator, boolean isAscending,
                                   boolean isWithHeader, QuoteEncloseMode quoteEncloseMode,
                                   CompressMode compressMode, FileFormat fileFormat, BaseCipher cipher) {
        super(dataSource, new TableTopology(tableName), tableFieldMetaInfo, maxLine, filename, separator, isWithHeader,
            quoteEncloseMode, compressMode, fileFormat, charset, cipher);
        this.orderByColumnName = orderByColumnName;
        this.isAscending = isAscending;
    }

    public void exportSerially() {
        this.run();
    }

    @Override
    protected String getExportSql() {
        return ExportUtil.getDirectOrderBySql(topology.getTableName(), tableFieldMetaInfo.getFieldMetaInfoList(),
            orderByColumnName, whereCondition, isAscending);
    }
}
