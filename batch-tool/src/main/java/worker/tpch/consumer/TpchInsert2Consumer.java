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

package worker.tpch.consumer;

import com.lmax.disruptor.WorkHandler;
import model.ConsumerExecutionContext;
import model.config.GlobalVar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.tpch.model.BatchInsertSql2Event;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * insert orders & lineitem with the same orderkey in a transaction
 */
public class TpchInsert2Consumer implements WorkHandler<BatchInsertSql2Event> {

    private static final Logger logger = LoggerFactory.getLogger(TpchInsert2Consumer.class);

    protected final ConsumerExecutionContext consumerContext;

    public TpchInsert2Consumer(ConsumerExecutionContext consumerContext) {
        this.consumerContext = consumerContext;
    }

    @Override
    public void onEvent(BatchInsertSql2Event event) {
        if (consumerContext.getException() != null) {
            // fail fast on exception
            consumerContext.getEmittedDataCounter().getAndDecrement();
            return;
        }
        String sql1 = event.getSql1();
        String sql2 = event.getSql2();
        try {
            insertInTrx(sql1, sql2);
        } finally {
            consumerContext.getEmittedDataCounter().decrementAndGet();
        }
    }

    private void insertInTrx(String sql1, String sql2) {
        Connection conn = null;
        String sql = null;
        try {
            conn = consumerContext.getDataSource().getConnection();
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                sql = sql1;
                stmt.execute(sql);
                sql = sql2;
                stmt.execute(sql);
            }
            conn.commit();
        } catch (SQLException e) {
            if (sql == null) {
                logger.error(e.getMessage());
            } else {
                if (GlobalVar.DEBUG_MODE) {
                    logger.error(sql + ", due to " + e.getMessage());
                } else {
                    logger.error(sql.substring(0, Math.min(32, sql.length())) + ", due to " + e.getMessage());
                }
            }
            consumerContext.setException(e);
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }
}
