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

import com.alibaba.druid.util.JdbcUtils;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DataSourceUtil;
import util.FileUtil;
import worker.util.ExportUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class OrderByExportProducer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(OrderByExportProducer.class);

    private String whereCondition;
    private final int queueIndex;
    private final LinkedBlockingQueue<OrderByExportEvent> orderByExportEventQueue;
    private final DataSource druid;
    private final TableTopology topology;
    private final TableFieldMetaInfo tableFieldMetaInfo;
    private final List<String> orderByColumnName;
    private final AtomicBoolean finished;

    /**
     * 默认升序
     */
    private boolean isAscending = true;

    public OrderByExportProducer(DataSource druid, TableTopology topology, TableFieldMetaInfo tableFieldMetaInfo,
                                 LinkedBlockingQueue<OrderByExportEvent> orderByExportEventQueue, int queueIndex,
                                 List<String> orderByColumnName, AtomicBoolean finished) {
        this.tableFieldMetaInfo = tableFieldMetaInfo;
        this.queueIndex = queueIndex;
        this.orderByExportEventQueue = orderByExportEventQueue;
        this.druid = druid;
        this.orderByColumnName = orderByColumnName;
        this.topology = topology;
        this.finished = finished;
    }

    public void produceData() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String sql = ExportUtil.getOrderBySql(topology, tableFieldMetaInfo.getFieldMetaInfoList(),
            orderByColumnName, whereCondition, isAscending);
        // 字段数
        int colNum;
        OrderByExportEvent exportEvent;
        long startTime = System.currentTimeMillis();
        try {
            conn = druid.getConnection();
            stmt = DataSourceUtil.createStreamingStatement(conn);
            resultSet = stmt.executeQuery(sql);
            colNum = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                byte[][] data = LocalOrderByExportProducer.getRowBytes(resultSet, colNum);
                exportEvent = new OrderByExportEvent(queueIndex,
                    data);
                orderByExportEventQueue.put(exportEvent);
            }
            long endTime = System.currentTimeMillis();
            logger.debug("{} 发送完成，耗时 {} s", topology, (endTime - startTime) / 1000F);
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            finished.set(true);
            // 写入一条空消息, 保证设置 finished 标志后队列不空。
            orderByExportEventQueue.offer(
                new OrderByExportEvent(queueIndex));
            JdbcUtils.close(resultSet);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    @Override
    public void run() {
        produceData();
    }

    public boolean isAscending() {
        return isAscending;
    }

    public void setAscending(boolean ascending) {
        isAscending = ascending;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }
}
