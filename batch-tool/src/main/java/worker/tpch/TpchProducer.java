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
import io.airlift.tpch.Customer;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.Nation;
import io.airlift.tpch.Order;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartSupplier;
import io.airlift.tpch.Region;
import io.airlift.tpch.Supplier;
import io.airlift.tpch.TpchTable;
import model.ProducerExecutionContext;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.Producer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static model.config.GlobalVar.EMIT_BATCH_SIZE;

public class TpchProducer implements Producer {

    private static final Logger logger = LoggerFactory.getLogger(TpchProducer.class);

    private final ProducerExecutionContext context;
    private final RingBuffer<BatchInsertSqlEvent> ringBuffer;
    private final ThreadPoolExecutor executor;
    private final int scale;
    private int workerCount;

    private Map<TpchTableModel, List<Iterator>> tableIteratorsMap;

    public TpchProducer(ProducerExecutionContext context, RingBuffer<BatchInsertSqlEvent> ringBuffer) {
        this.context = context;
        this.ringBuffer = ringBuffer;

        this.scale = context.getScale();
        if (scale <= 0) {
            throw new IllegalArgumentException("Scale must be a positive integer");
        }
        this.executor = context.getProducerExecutor();

        this.tableIteratorsMap = new HashMap<>();
        initIterators();
        initWorkerCount();
    }

    private void initIterators() {
        addRegionIterator();
        addNationIterator();
        addCustomerIterator();
        addPartIterator();
        addSupplierIterator();
        addPartSuppIterator();
        addOrdersIterator();
        addLineitemIterator();
    }

    private void initWorkerCount() {
        this.workerCount = tableIteratorsMap.values().stream()
            .mapToInt(List::size).sum();
    }

    public int getWorkerCount() {
        return this.workerCount;
    }

    private void addRegionIterator() {
        List<Iterator> regionIters = new ArrayList<>(1);
        regionIters.add(TpchTable.REGION.createGenerator(scale, 1, 1).iterator());
        this.tableIteratorsMap.put(TpchTableModel.REGION, regionIters);
    }

    private void addNationIterator() {
        List<Iterator> nationIters = new ArrayList<>(1);
        nationIters.add(TpchTable.NATION.createGenerator(scale, 1, 1).iterator());
        this.tableIteratorsMap.put(TpchTableModel.NATION, nationIters);
    }

    private void addCustomerIterator() {
        int customerPart;
        if (scale < 10) {
            customerPart = 1;
        } else {
            customerPart = 4;
        }

        List<Iterator> customerIters = new ArrayList<>(customerPart);
        for (int i = 1; i <= customerPart; i++) {
            customerIters.add(TpchTable.CUSTOMER.createGenerator(scale, i, customerPart).iterator());
            this.tableIteratorsMap.put(TpchTableModel.CUSTOMER, customerIters);
        }
    }

    private void addPartIterator() {
        int partPart;
        if (scale < 10) {
            partPart = 1;
        } else {
            partPart = 5;
        }

        List<Iterator> partIters = new ArrayList<>(partPart);
        for (int i = 1; i <= partPart; i++) {
            partIters.add(TpchTable.PART.createGenerator(scale, i, partPart).iterator());
            this.tableIteratorsMap.put(TpchTableModel.PART, partIters);
        }
    }

    private void addSupplierIterator() {
        final int supplierPart = 1;

        List<Iterator> supplierIters = new ArrayList<>(supplierPart);
        for (int i = 1; i <= supplierPart; i++) {
            supplierIters.add(TpchTable.SUPPLIER.createGenerator(scale, i, supplierPart).iterator());
            this.tableIteratorsMap.put(TpchTableModel.SUPPLIER, supplierIters);
        }
    }

    /**
     * 第三大表
     */
    private void addPartSuppIterator() {
        int partSuppPart;
        if (scale < 10) {
            partSuppPart = 4;
        } else {
            partSuppPart = 20;
        }

        List<Iterator> partSuppIters = new ArrayList<>(partSuppPart);
        for (int i = 1; i <= partSuppPart; i++) {
            partSuppIters.add(TpchTable.PART_SUPPLIER.createGenerator(scale, i, partSuppPart).iterator());
            this.tableIteratorsMap.put(TpchTableModel.PART_SUPP, partSuppIters);
        }
    }

    /**
     * 第二大表
     */
    private void addOrdersIterator() {
        int ordersPart;
        if (scale < 10) {
            ordersPart = 8;
        } else {
            ordersPart = 40;
        }

        List<Iterator> ordersIters = new ArrayList<>(ordersPart);
        for (int i = 1; i <= ordersPart; i++) {
            ordersIters.add(TpchTable.ORDERS.createGenerator(scale, i, ordersPart).iterator());
            this.tableIteratorsMap.put(TpchTableModel.ORDERS, ordersIters);
        }
    }

    /**
     * 第一大表
     * 此处暂不考虑生产者线程池太大导致生产者空闲
     */
    private void addLineitemIterator() {
        int lineitemPart;
        if (scale < 10) {
            lineitemPart = 16;
        } else {
            lineitemPart = 80;
        }

        List<Iterator> lineitemIters = new ArrayList<>(lineitemPart);
        for (int i = 1; i <= lineitemPart; i++) {
            lineitemIters.add(TpchTable.LINE_ITEM.createGenerator(scale, i, lineitemPart).iterator());
            this.tableIteratorsMap.put(TpchTableModel.LINEITEM, lineitemIters);
        }
    }

    @Override
    public void produce() {
        boolean hasNext = true;
        Map<TpchTableModel, Iterator<Iterator>> iteratorMap = new HashMap<>(32);
        for (Map.Entry<TpchTableModel, List<Iterator>> entry : tableIteratorsMap.entrySet()) {
            iteratorMap.put(entry.getKey(), entry.getValue().iterator());
        }

        Map<TpchTableModel, Integer> partMap = new HashMap<>(32);

        // 按表依次提交任务
        while (hasNext) {
            hasNext = false;
            for (Map.Entry<TpchTableModel, Iterator<Iterator>> entry : iteratorMap.entrySet()) {
                if (entry.getValue().hasNext()) {
                    hasNext = true;
                    int part = partMap.getOrDefault(entry.getKey(), 1);

                    Iterator nextTableIter = entry.getValue().next();
                    TpchTableWorker<?> producer = buildProducer(entry.getKey(), nextTableIter, part);
                    executor.submit(producer);

                    partMap.put(entry.getKey(), part + 1);
                }
            }
        }

    }

    private TpchTableWorker<?> buildProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        switch (table) {
        case LINEITEM:
            return buildLineitemProducer(table, nextTableIter, part);
        case CUSTOMER:
            return buildCustomerProducer(table, nextTableIter, part);
        case ORDERS:
            return buildOrdersProducer(table, nextTableIter, part);
        case PART:
            return buildPartProducer(table, nextTableIter, part);
        case SUPPLIER:
            return buildSupplierProducer(table, nextTableIter, part);
        case PART_SUPP:
            return buildPartSuppProducer(table, nextTableIter, part);
        case NATION:
            return buildNationProducer(table, nextTableIter, part);
        case REGION:
            return buildRegionWorker(table, nextTableIter, part);
        }
        throw new UnsupportedOperationException("Unsupported TPC-H table: " + table);
    }

    /**
     * 转为两位小数的字符串表示
     * 并append
     */
    private void appendDecimalWithFrac2(StringBuilder sqlBuffer, long l) {
        long intPart = l / 100;
        long fracPart = l % 100;
        sqlBuffer.append(intPart).append('.').append(fracPart);
    }

    private TpchTableWorker<?> buildLineitemProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<LineItem> lineitemProducer = new TpchTableWorker<LineItem>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(LineItem row) {

                this.sqlBuffer.append('(').append(row.getOrderKey())
                    .append(',').append(row.getPartKey())
                    .append(',').append(row.getSupplierKey())
                    .append(',').append(row.getLineNumber())
                    .append(',').append(row.getQuantity())
                    .append(",");

                appendDecimalWithFrac2(this.sqlBuffer, row.getExtendedPriceInCents());
                this.sqlBuffer.append(',');
                appendDecimalWithFrac2(this.sqlBuffer, row.getDiscountPercent());
                this.sqlBuffer.append(',');
                appendDecimalWithFrac2(this.sqlBuffer, row.getTaxPercent());

                this.sqlBuffer.append(",\"").append(row.getReturnFlag())
                    .append("\",\"").append(row.getStatus())
                    .append("\",\"").append(formatDate(row.getShipDate()))
                    .append("\",\"").append(formatDate(row.getCommitDate()))
                    .append("\",\"").append(formatDate(row.getReceiptDate()))
                    .append("\",\"").append(row.getShipInstructions())
                    .append("\",\"").append(row.getShipMode())
                    .append("\",\"").append(row.getComment()).append("\"),");

            }
        };
        return lineitemProducer;
    }

    private TpchTableWorker<?> buildCustomerProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<Customer> customerProducer = new TpchTableWorker<Customer>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(Customer row) {
                this.sqlBuffer.append('(').append(row.getCustomerKey())
                    .append(",\"").append(row.getName())
                    .append("\",\"").append(row.getAddress())
                    .append("\",").append(row.getNationKey())
                    .append(",\"").append(row.getPhone())
                    .append("\",");

                appendDecimalWithFrac2(this.sqlBuffer, row.getAccountBalanceInCents());

                this.sqlBuffer.append(",\"").append(row.getMarketSegment())
                    .append("\",\"").append(row.getComment()).append("\"),");
            }
        };
        return customerProducer;
    }

    private TpchTableWorker<?> buildOrdersProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<Order> orderProducer = new TpchTableWorker<Order>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(Order row) {

                this.sqlBuffer.append('(').append(row.getOrderKey())
                    .append(',').append(row.getCustomerKey())
                    .append(",\"").append(row.getOrderStatus())
                    .append("\",");

                appendDecimalWithFrac2(this.sqlBuffer, row.getTotalPriceInCents());

                this.sqlBuffer.append(",\"").append(formatDate(row.getOrderDate()))
                    .append("\",\"").append(row.getOrderPriority())
                    .append("\",\"").append(row.getClerk())
                    .append("\",").append(row.getShipPriority())
                    .append(",\"").append(row.getComment()).append("\"),");
            }
        };
        return orderProducer;
    }

    private TpchTableWorker<?> buildPartProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<Part> partProducer = new TpchTableWorker<Part>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(Part row) {

                this.sqlBuffer.append('(').append(row.getPartKey())
                    .append(",\"").append(row.getName())
                    .append("\",\"").append(row.getManufacturer())
                    .append("\",\"").append(row.getBrand())
                    .append("\",\"").append(row.getType())
                    .append("\",").append(row.getSize())
                    .append(",\"").append(row.getContainer())
                    .append("\",");

                appendDecimalWithFrac2(this.sqlBuffer, row.getRetailPriceInCents());

                this.sqlBuffer.append(",\"").append(row.getComment()).append("\"),");
            }
        };
        return partProducer;
    }

    private TpchTableWorker<?> buildSupplierProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<Supplier> supplierProducer = new TpchTableWorker<Supplier>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(Supplier row) {

                this.sqlBuffer.append('(').append(row.getSupplierKey())
                    .append(",\"").append(row.getName())
                    .append("\",\"").append(row.getAddress())
                    .append("\",").append(row.getNationKey())
                    .append(",\"").append(row.getPhone())
                    .append("\",");

                appendDecimalWithFrac2(this.sqlBuffer, row.getAccountBalanceInCents());

                this.sqlBuffer.append(",\"").append(row.getComment()).append("\"),");
            }
        };
        return supplierProducer;
    }

    private TpchTableWorker<?> buildPartSuppProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<PartSupplier> partSuppProducer = new TpchTableWorker<PartSupplier>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(PartSupplier row) {

                this.sqlBuffer.append('(').append(row.getPartKey())
                    .append(',').append(row.getSupplierKey())
                    .append(',').append(row.getAvailableQuantity())
                    .append(',');

                appendDecimalWithFrac2(this.sqlBuffer, row.getSupplyCostInCents());

                this.sqlBuffer.append(",\"").append(row.getComment()).append("\"),");
            }
        };
        return partSuppProducer;
    }

    private TpchTableWorker<?> buildNationProducer(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<Nation> nationProducer = new TpchTableWorker<Nation>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(Nation row) {
                this.sqlBuffer.append('(').append(row.getNationKey())
                    .append(",\"").append(row.getName())
                    .append("\",").append(row.getRegionKey())
                    .append(",\"").append(row.getComment()).append("\"),");
            }
        };
        return nationProducer;
    }

    private TpchTableWorker<?> buildRegionWorker(TpchTableModel table, Iterator nextTableIter, int part) {
        TpchTableWorker<Region> regionProducer = new TpchTableWorker<Region>(ringBuffer, nextTableIter,
            table.getName(), table.getRowStrLen(), part) {
            @Override
            protected void appendToBuffer(Region row) {
                this.sqlBuffer.append('(').append(row.getRegionKey())
                    .append(",\"").append(row.getName())
                    .append("\",\"").append(row.getComment()).append("\"),");
            }
        };
        return regionProducer;
    }

    /**
     * 尽可能同时并发写入不同的表
     */
    abstract class TpchTableWorker<T> implements Runnable {
        protected final RingBuffer<BatchInsertSqlEvent> ringBuffer;
        protected final StringBuilder sqlBuffer;
        protected final Iterator<T> iterator;
        protected final String tableName;
        protected final int estimateRowLen;
        protected final int part;
        protected final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
        protected int bufferedLineCount = 0;

        TpchTableWorker(RingBuffer<BatchInsertSqlEvent> ringBuffer, Iterator<T> iterator,
                        String tableName, int estimateRowLen) {
            this(ringBuffer, iterator, tableName, estimateRowLen, 0);
        }

        TpchTableWorker(RingBuffer<BatchInsertSqlEvent> ringBuffer, Iterator<T> iterator,
                        String tableName, int estimateRowLen, int part) {
            this.ringBuffer = ringBuffer;
            this.iterator = iterator;
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
                while (iterator.hasNext()) {
                    T row = iterator.next();
                    appendToBuffer(row);
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

        /**
         * TODO 可以自己实现这个逻辑并优化
         * 返回格式 yyyy-MM-dd
         *
         * @param days 自从1970-01-01经过的天数
         */
        String formatDate(int days) {
            Date date = new Date(TimeUnit.DAYS.toMillis(days));
            return dateFormat.format(date);
        }

        protected abstract void appendToBuffer(T row);

        protected void emitLineBuffer() {
            long sequence = ringBuffer.next();
            BatchInsertSqlEvent event;
            try {
                event = ringBuffer.get(sequence);
                sqlBuffer.setCharAt(sqlBuffer.length() - 1, ';');   // 最后一个逗号替换为分号
                String sql = sqlBuffer.toString();
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
