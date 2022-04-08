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
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.BaseShardedConsumer;
import worker.util.UpdateUtil;

import java.util.List;

public class ShardedReplaceConsumer extends BaseShardedConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ShardedReplaceConsumer.class);

    @Override
    protected void fillLocalBuffer(StringBuilder localBuffer, String[] values, List<FieldMetaInfo> fieldMetaInfoList) {
        localBuffer.append("(");
        localBuffer.append(UpdateUtil.getUpdatedValuesByMetaInfo(consumerContext.getTablePkIndexSet(tableName),
            values, fieldMetaInfoList));
        localBuffer.append("),");
    }

    @Override
    protected String getSqlWithHint(TableTopology topology, StringBuilder data) {
        // 去除最后的逗号
        data.setLength(data.length() - 1);
        return UpdateUtil.getBatchReplaceSqlWithHint(topology.getGroupName(),
            topology.getTableName(), consumerContext.getToUpdateColumns(), data.toString());
    }
}
