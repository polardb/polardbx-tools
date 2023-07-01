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

package worker.delete;

import model.db.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.BaseDefaultConsumer;
import worker.util.DeleteUtil;

import java.util.ArrayList;
import java.util.List;

public class DeleteConsumer extends BaseDefaultConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DeleteConsumer.class);

    private List<PrimaryKey> pkList;

    @Override
    protected void initLocalVars() {
        super.initLocalVars();
        this.pkList = consumerContext.getTablePkList(tableName);
    }

    @Override
    protected void fillLocalBuffer(StringBuilder stringBuilder, List<String> values) {
        List<String> pkValues = new ArrayList<>(pkList.size());
        for (PrimaryKey primaryKey : pkList) {
            pkValues.add(values.get(primaryKey.getOrdinalPosition() - 1));
        }
        stringBuilder.append(DeleteUtil.getDeleteSql(tableName, pkList,
            pkValues, consumerContext.getWhereCondition()));
    }

    @Override
    protected String getSql(StringBuilder data) {
        return data.toString();
    }

}
