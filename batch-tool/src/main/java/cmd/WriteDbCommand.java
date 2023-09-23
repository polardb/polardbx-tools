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

package cmd;

import model.ConsumerExecutionContext;
import model.ProducerExecutionContext;
import model.config.ConfigConstant;

import javax.validation.constraints.NotNull;

/**
 * 写入数据库相关的命令
 * 包括插入、更新、删除
 */
public class WriteDbCommand extends BaseOperateCommand {

    private final ProducerExecutionContext producerExecutionContext;

    private final ConsumerExecutionContext consumerExecutionContext;

    public WriteDbCommand(String dbName,
                          @NotNull ProducerExecutionContext producerExecutionContext,
                          @NotNull ConsumerExecutionContext consumerExecutionContext) {
        super(dbName, ConfigConstant.DEFAULT_IMPORT_SHARDING_ENABLED);
        this.producerExecutionContext = producerExecutionContext;
        this.consumerExecutionContext = consumerExecutionContext;
    }

    public ProducerExecutionContext getProducerExecutionContext() {
        return producerExecutionContext;
    }

    public ConsumerExecutionContext getConsumerExecutionContext() {
        return consumerExecutionContext;
    }

    @Override
    public String toString() {
        return "WriteDbCommand{" +
            producerExecutionContext + ", " + consumerExecutionContext +
            '}';
    }
}
