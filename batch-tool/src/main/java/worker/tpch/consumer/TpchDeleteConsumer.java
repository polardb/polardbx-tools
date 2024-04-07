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
import worker.tpch.generator.OrderLineDeleteGenerator;
import worker.tpch.model.BatchDeleteSqlEvent;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static worker.tpch.model.TpchTableModel.LINEITEM;
import static worker.tpch.model.TpchTableModel.ORDERS;

/**
 * delete orders & lineitem with the same orderkey in a transaction
 */
public class TpchDeleteConsumer implements WorkHandler<BatchDeleteSqlEvent> {

    private static final Logger logger = LoggerFactory.getLogger(TpchDeleteConsumer.class);

    private static final String DELETE_ORDERS_SQL = "delete from " + ORDERS.getName() + " where O_ORDERKEY in ";
    private static final String DELETE_LINEITEM_SQL = "delete from " + LINEITEM.getName() + " where L_ORDERKEY in ";

    protected final ConsumerExecutionContext consumerContext;

    private final StringBuilder deleteOrdersBuilder =
        new StringBuilder(OrderLineDeleteGenerator.DEFAULT_DELETE_BATCH_NUM * 8 + 24);
    private final StringBuilder deleteLineitemBuilder =
        new StringBuilder(OrderLineDeleteGenerator.DEFAULT_DELETE_BATCH_NUM * 8 + 24);
    private final int maxRetry;

    public TpchDeleteConsumer(ConsumerExecutionContext consumerContext) {
        this.consumerContext = consumerContext;
        this.maxRetry = consumerContext.getMaxRetry();
    }

    @Override
    public void onEvent(BatchDeleteSqlEvent event) {
        if (consumerContext.getException() != null) {
            // fail fast on exception
            consumerContext.getEmittedDataCounter().getAndDecrement();
            return;
        }
        String values = event.getValues();
        try {
            deleteInTrx(values);
        } finally {
            consumerContext.getEmittedDataCounter().decrementAndGet();
        }
    }

    private void deleteInTrx(String values) {
        Connection conn = null;
        String sql = null;
        Exception finalException = null;
        try {
            conn = consumerContext.getDataSource().getConnection();
            conn.setAutoCommit(false);

            int retry = 0;
            while (retry <= maxRetry) {
                try {
                    doDelete(conn, values);
                    break;
                } catch (Exception e) {
                    retry++;
                    try {
                        conn.rollback();
                    } catch (SQLException e1) {
                        logger.error("rollback failed", e1);
                        finalException = e1;
                        break;
                    }
                    if (retry > maxRetry) {
                        finalException = e;
                    }
                }
            }
        } catch (Exception e) {
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
            if (finalException != null && consumerContext.getException() == null) {
                consumerContext.setException(finalException);
            }
        }
    }

    private void doDelete(Connection conn, String values) throws SQLException {
        String sql = null;
        try (Statement stmt = conn.createStatement();) {
            sql = buildDeleteOrdersSql(values);
            stmt.execute(sql);
            sql = buildDeleteLineitemSql(values);
            stmt.execute(sql);
            stmt.close();
            conn.commit();
        } catch (SQLException e) {
            if (sql == null) {
                logger.error(e.getMessage());
            } else {
                if (GlobalVar.DEBUG_MODE) {
                    logger.error(sql + ", due to " + e.getMessage());
                } else {
                    logger.error(sql.substring(0, Math.min(32, sql.length())) + ", due to" + e.getMessage());
                }
            }
            throw e;
        }
    }

    private String buildDeleteOrdersSql(String pkValue) {
        deleteOrdersBuilder.setLength(0);
        deleteOrdersBuilder.append(DELETE_ORDERS_SQL).append('(').append(pkValue).append(");");
        return deleteOrdersBuilder.toString();
    }

    private String buildDeleteLineitemSql(String pkValue) {
        deleteLineitemBuilder.setLength(0);
        deleteLineitemBuilder.append(DELETE_LINEITEM_SQL).append('(').append(pkValue).append(");");
        return deleteLineitemBuilder.toString();
    }
}
