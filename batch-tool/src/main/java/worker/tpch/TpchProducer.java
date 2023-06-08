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

import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.Producer;
import worker.tpch.generator.CustomerGenerator;
import worker.tpch.generator.LineItemGenerator;
import worker.tpch.generator.NationGenerator;
import worker.tpch.generator.OrderGenerator;
import worker.tpch.generator.PartGenerator;
import worker.tpch.generator.PartSupplierGenerator;
import worker.tpch.generator.RegionGenerator;
import worker.tpch.generator.SupplierGenerator;
import worker.tpch.generator.TableRowGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static model.config.GlobalVar.EMIT_BATCH_SIZE;

public class TpchProducer implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(TpchProducer.class);

    private final ProducerExecutionContext context;
    private final RingBuffer<BatchInsertSqlEvent> ringBuffer;
    private final ThreadPoolExecutor executor;
    private final int scale;
    private int workerCount;

    private Map<TpchTableModel, List<TableRowGenerator>> tableGeneratorsMap;

    public TpchProducer(ProducerExecutionContext context, List<String> tableNames,
                        RingBuffer<BatchInsertSqlEvent> ringBuffer) {
        this.context = context;
        this.ringBuffer = ringBuffer;

        this.scale = context.getScale();
        if (scale <= 0) {
            throw new IllegalArgumentException("Scale must be a positive integer");
        }
        this.executor = context.getProducerExecutor();

        this.tableGeneratorsMap = new HashMap<>();
        initGenerators(tableNames);
        initWorkerCount();
    }

    private void initGenerators(List<String> tableNames) {
        if (tableNames == null || tableNames.isEmpty()) {
            addRegionGenerator();
            addNationGenerator();
            addCustomerGenerator();
            addPartGenerator();
            addSupplierGenerator();
            addPartSuppGenerator();
            addOrdersGenerator();
            addLineitemGenerator();
            return;
        }
        Set<TpchTableModel> tables = tableNames.stream().map(TpchTableModel::parse)
            .collect(Collectors.toSet());

        for (TpchTableModel table : tables) {
            switch (table) {
            case LINEITEM:
                addLineitemGenerator();
                break;
            case CUSTOMER:
                addCustomerGenerator();
                break;
            case ORDERS:
                addOrdersGenerator();
                break;
            case PART:
                addPartGenerator();
                break;
            case SUPPLIER:
                addSupplierGenerator();
                break;
            case PART_SUPP:
                addPartSuppGenerator();
                break;
            case NATION:
                addNationGenerator();
                break;
            case REGION:
                addRegionGenerator();
                break;
            default:
                throw new UnsupportedOperationException(table.getName());
            }
        }
    }

    private void initWorkerCount() {
        this.workerCount = tableGeneratorsMap.values().stream()
            .mapToInt(List::size).sum();
    }

    public int getWorkerCount() {
        return this.workerCount;
    }

    private void addRegionGenerator() {
        List<TableRowGenerator> regions = new ArrayList<>(1);
        regions.add(new RegionGenerator());
        this.tableGeneratorsMap.put(TpchTableModel.REGION, regions);
    }

    private void addNationGenerator() {
        List<TableRowGenerator> nations = new ArrayList<>(1);
        nations.add(new NationGenerator());
        this.tableGeneratorsMap.put(TpchTableModel.NATION, nations);
    }

    private void addCustomerGenerator() {
        int customerPart;
        if (scale < 10) {
            customerPart = 1;
        } else {
            customerPart = 4;
        }

        List<TableRowGenerator> customers = new ArrayList<>(customerPart);
        for (int i = 1; i <= customerPart; i++) {
            customers.add(new CustomerGenerator(scale, i, customerPart));
            this.tableGeneratorsMap.put(TpchTableModel.CUSTOMER, customers);
        }
    }

    private void addPartGenerator() {
        int partPart;
        if (scale < 10) {
            partPart = 1;
        } else {
            partPart = 5;
        }

        List<TableRowGenerator> parts = new ArrayList<>(partPart);
        for (int i = 1; i <= partPart; i++) {
            parts.add(new PartGenerator(scale, i, partPart));
            this.tableGeneratorsMap.put(TpchTableModel.PART, parts);
        }
    }

    private void addSupplierGenerator() {
        final int supplierPart = 1;

        List<TableRowGenerator> suppliers = new ArrayList<>(supplierPart);
        for (int i = 1; i <= supplierPart; i++) {
            suppliers.add(new SupplierGenerator(scale, i, supplierPart));
            this.tableGeneratorsMap.put(TpchTableModel.SUPPLIER, suppliers);
        }
    }

    /**
     * 第三大表
     */
    private void addPartSuppGenerator() {
        int partSuppPart;
        if (scale < 10) {
            partSuppPart = 4;
        } else {
            partSuppPart = 10;
        }

        List<TableRowGenerator> partSupps = new ArrayList<>(partSuppPart);
        for (int i = 1; i <= partSuppPart; i++) {
            partSupps.add(new PartSupplierGenerator(scale, i, partSuppPart));
            this.tableGeneratorsMap.put(TpchTableModel.PART_SUPP, partSupps);
        }
    }

    /**
     * 第二大表
     */
    private void addOrdersGenerator() {
        int ordersPart;
        if (scale < 10) {
            ordersPart = 8;
        } else {
            ordersPart = 30;
        }

        List<TableRowGenerator> orders = new ArrayList<>(ordersPart);
        for (int i = 1; i <= ordersPart; i++) {
            orders.add(new OrderGenerator(scale, i, ordersPart));
            this.tableGeneratorsMap.put(TpchTableModel.ORDERS, orders);
        }
    }

    /**
     * 第一大表
     * 此处暂不考虑生产者线程池太大导致生产者空闲
     */
    private void addLineitemGenerator() {
        int lineitemPart;
        if (scale < 10) {
            lineitemPart = 16;
        } else {
            lineitemPart = 60;
        }

        List<TableRowGenerator> lineitems = new ArrayList<>(lineitemPart);
        for (int i = 1; i <= lineitemPart; i++) {
            lineitems.add(new LineItemGenerator(scale, i, lineitemPart));
            this.tableGeneratorsMap.put(TpchTableModel.LINEITEM, lineitems);
        }
    }

    @Override
    public void produce() {
        boolean hasNext = true;
        Map<TpchTableModel, Iterator<TableRowGenerator>> iteratorMap = new HashMap<>(32);
        for (Map.Entry<TpchTableModel, List<TableRowGenerator>> entry : tableGeneratorsMap.entrySet()) {
            iteratorMap.put(entry.getKey(), entry.getValue().iterator());
        }

        Map<TpchTableModel, Integer> partMap = new HashMap<>(32);

        // 按表依次提交任务
        while (hasNext) {
            hasNext = false;
            for (Map.Entry<TpchTableModel, Iterator<TableRowGenerator>> entry : iteratorMap.entrySet()) {
                if (entry.getValue().hasNext()) {
                    hasNext = true;
                    TpchTableModel table = entry.getKey();
                    int part = partMap.getOrDefault(table, 1);

                    TableRowGenerator nextGenerator = entry.getValue().next();
                    TpchTableWorker producer = new TpchTableWorker(ringBuffer,
                        nextGenerator, table.getName(), table.getRowStrLen(), part);
                    executor.submit(producer);

                    partMap.put(table, part + 1);
                }
            }
        }

    }

    /**
     * 尽可能同时并发写入不同的表
     */
    class TpchTableWorker implements Runnable {
        protected final RingBuffer<BatchInsertSqlEvent> ringBuffer;
        protected final StringBuilder sqlBuffer;
        protected final TableRowGenerator rowGenerator;
        protected final String tableName;
        protected final int estimateRowLen;
        protected final int part;
        protected int bufferedLineCount = 0;

        TpchTableWorker(RingBuffer<BatchInsertSqlEvent> ringBuffer, TableRowGenerator rowGenerator,
                        String tableName, int estimateRowLen, int part) {
            this.ringBuffer = ringBuffer;
            this.rowGenerator = rowGenerator;
            this.tableName = tableName;
            this.estimateRowLen = estimateRowLen;
            this.part = part;

            this.sqlBuffer = new StringBuilder("INSERT INTO  VALUES ()".length() +
                this.tableName.length() + EMIT_BATCH_SIZE * (2 + estimateRowLen));
            refreshBuffer();
        }

        @Override
        public void run() {
            try {
                while (rowGenerator.hasNext()) {
                    rowGenerator.appendNextRow(sqlBuffer);
                    bufferedLineCount++;
                    if (bufferedLineCount >= EMIT_BATCH_SIZE) {
                        emitLineBuffer();
                    }
                }
                if (bufferedLineCount != 0) {
                    emitLineBuffer();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                context.getCountDownLatch().countDown();
                logger.info("{}-{} produce done", tableName, part);
            }
        }

        protected void emitLineBuffer() {
            long sequence = ringBuffer.next();
            BatchInsertSqlEvent event;
            try {
                event = ringBuffer.get(sequence);
                sqlBuffer.setCharAt(sqlBuffer.length() - 1, ';');   // 最后一个逗号替换为分号
                String sql = sqlBuffer.toString();
//                System.out.println(sql);
                event.setSql(sql);
                refreshBuffer();
            } finally {
                bufferedLineCount = 0;
                context.getEmittedDataCounter().getAndIncrement();
                ringBuffer.publish(sequence);
            }
        }

        private void refreshBuffer() {
            sqlBuffer.setLength(0);
            sqlBuffer.append("INSERT INTO `").append(tableName).append("` VALUES ");
        }
    }
}
