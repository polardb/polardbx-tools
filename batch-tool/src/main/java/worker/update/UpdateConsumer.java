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

package worker.update;

import model.db.FieldMetaInfo;
import model.db.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.BaseDefaultConsumer;
import worker.util.UpdateUtil;

import java.util.List;

public class UpdateConsumer extends BaseDefaultConsumer {
    private static final Logger logger = LoggerFactory.getLogger(UpdateConsumer.class);

    private List<PrimaryKey> pkList;
    private String[] pkValues;
    private List<FieldMetaInfo> fieldMetaInfoList;

    @Override
    protected void initLocalVars() {
        this.pkList = consumerContext.getTablePkList(tableName);
        this.pkValues = new String[pkList.size()];
        this.fieldMetaInfoList = consumerContext.getTableFieldMetaInfo(tableName).getFieldMetaInfoList();
    }

    @Override
    protected void fillLocalBuffer(StringBuilder stringBuilder, List<String> values) {
        for (int i = 0; i < pkList.size(); i++) {
            pkValues[i] = values.get(pkList.get(i).getOrdinalPosition() - 1);
        }
        stringBuilder.append(UpdateUtil.getUpdateSql(tableName,
            pkList, consumerContext.getTablePkIndexSet(tableName),
            fieldMetaInfoList, values,
            consumerContext.getWhereCondition()));
    }

    @Override
    protected String getSql(StringBuilder data) {
        return data.toString();
    }

}
