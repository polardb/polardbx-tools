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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 将每个分片排好序的数据放入内存
 * 交给消费者做归并排序
 */
public class LocalOrderByExportProducer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LocalOrderByExportProducer.class);

    private String whereCondition;
    private final LinkedList<ParallelOrderByExportEvent> orderByExportEventList;
    private final DataSource druid;
    private final TableTopology topology;
    private final TableFieldMetaInfo tableFieldMetaInfo;
    private final List<String> orderByColumnName;

    private final CountDownLatch countDownLatch;

    /**
     * 默认升序
     */
    private boolean isAscending = true;

    public LocalOrderByExportProducer(DataSource druid, TableTopology topology, TableFieldMetaInfo tableFieldMetaInfo,
                                      LinkedList<ParallelOrderByExportEvent> orderByExportEventList,
                                      List<String> orderByColumnName, CountDownLatch countDownLatch) {
        this.tableFieldMetaInfo = tableFieldMetaInfo;
        this.orderByExportEventList = orderByExportEventList;
        this.druid = druid;
        this.orderByColumnName = orderByColumnName;
        this.topology = topology;
        this.countDownLatch = countDownLatch;
    }

    public void produceData() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String sql = ExportUtil.getOrderBySql(topology, tableFieldMetaInfo.getFieldMetaInfoList(),
            orderByColumnName, whereCondition, isAscending);
        // 字段数
        int colNum;

        long startTime = System.currentTimeMillis();
        try {
            conn = druid.getConnection();
            stmt = DataSourceUtil.createStreamingStatement(conn);
            logger.info("{} 开始获取数据", topology);
            resultSet = stmt.executeQuery(sql);
            colNum = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                byte[][] data = getRowBytes(resultSet, colNum);
                orderByExportEventList.add(new ParallelOrderByExportEvent(data));
            }
            long endTime = System.currentTimeMillis();
            logger.debug("{} 发送至缓冲区完毕，耗时 {} s", topology, (endTime - startTime) / 1000F);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            countDownLatch.countDown();
            JdbcUtils.close(resultSet);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    static byte[][] getRowBytes(ResultSet resultSet, int colNum) throws SQLException {
        byte[][] data = new byte[colNum][];
        for (int i = 0; i < colNum; i++) {
            byte[] fieldValue = resultSet.getBytes(i + 1);
            if (fieldValue != null) {
                data[i] = fieldValue;
            } else {
                data[i] = FileUtil.NULL_ESC_BYTE;
            }
        }
        return data;
    }

    @Override
    public void run() {
        produceData();
    }

    public void setAscending(boolean ascending) {
        isAscending = ascending;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }
}
