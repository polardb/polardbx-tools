/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package worker.tpch;

import com.lmax.disruptor.WorkHandler;
import model.ConsumerExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TpchConsumer implements WorkHandler<BatchInsertSqlEvent> {

    private static final Logger logger = LoggerFactory.getLogger(TpchConsumer.class);

    protected final ConsumerExecutionContext consumerContext;

    public TpchConsumer(ConsumerExecutionContext consumerContext) {
        this.consumerContext = consumerContext;
    }

    @Override
    public void onEvent(BatchInsertSqlEvent event) {
        String sql = event.getSql();
        try (Connection conn = consumerContext.getDataSource().getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            logger.error(sql + ", due to " + e.getMessage());
//            logger.error(sql.substring(0, Math.min(32, sql.length())) + ", due to" + e.getMessage());
            consumerContext.setException(e);
            throw new RuntimeException(e);
        } finally {
            consumerContext.getEmittedDataCounter().decrementAndGet();
        }
    }
}
