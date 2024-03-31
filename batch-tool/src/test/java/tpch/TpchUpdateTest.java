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

package tpch;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.tpch.generator.OrderLineDeleteGenerator;
import worker.tpch.generator.OrderLineInsertGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TpchUpdateTest {

    private static final Logger logger = LoggerFactory.getLogger(TpchUpdateTest.class);

    private static final File TPCH_RESOURCE_DIR = new File(TpchUpdateTest.class.getResource("/").getPath() + "tpch/");

    @Test
    public void testGenDelete1G() throws IOException {
        testDelete(1, 3);
    }

    @Test
    public void testGenDelete10G() throws IOException {
        testDelete(10, 2);
    }

    @Test
    public void testGenInsert1G() throws IOException {
        testInsert(1, 2);
    }

    @Test
    public void testGenInsert5G() throws IOException {
        testInsert(5, 3);
    }

    @Test
    public void testDeleteBatch1G() {
        final int batchSize = 40;
        OrderLineDeleteGenerator generator = new OrderLineDeleteGenerator(1, 1);
        generator.setBatchSize(batchSize);
        int round = 1500 / batchSize;
        int remain = 1500 % batchSize;
        for (int i = 0; i < round; i++) {
            Assert.assertTrue(generator.hasData());
            String batchKeys = generator.getBatchDeleteOrderkey();
            String[] split = batchKeys.split(",");
            Assert.assertEquals(batchSize, split.length);
            generator.next();
        }
        String batchKeys = generator.getBatchDeleteOrderkey();
        String[] split = batchKeys.split(",");
        Assert.assertEquals(remain, split.length);
        generator.next();
        Assert.assertFalse(generator.hasData());
    }

    @Test
    public void testInsertBatch1G() {
        final int batchSize = 40;
        final int expectOrdersRows = 1500;
        final int expectLineitemRows = 5822;
        OrderLineInsertGenerator generator = new OrderLineInsertGenerator(1, 1);
        generator.setBatchSize(batchSize);
        int round = 1500 / batchSize;
        int remain = 1500 % batchSize;
        int curOrdersRows = 0;
        int curLineitemRows = 0;

        for (int i = 0; i < round; i++) {
            Assert.assertTrue(generator.hasData());
            generator.nextBatch();
            String insertOrdersSql = generator.getInsertOrdersSqls();
            String insertLineitemSql = generator.getInsertLineitemSqls();

            String[] ordersRows = insertOrdersSql.split("\\),\\(");
            String[] lineitemRows = insertLineitemSql.split("\\),\\(");
            curOrdersRows += ordersRows.length;
            curLineitemRows += lineitemRows.length;
        }
        Assert.assertTrue(generator.hasData());
        generator.nextBatch();
        String insertOrdersSql = generator.getInsertOrdersSqls();
        String insertLineitemSql = generator.getInsertLineitemSqls();

        String[] ordersRows = insertOrdersSql.split("\\),\\(");
        String[] lineitemRows = insertLineitemSql.split("\\),\\(");
        curOrdersRows += ordersRows.length;
        curLineitemRows += lineitemRows.length;

        Assert.assertEquals("Orders row count does not match", expectOrdersRows, curOrdersRows);
        Assert.assertEquals("Lineitem row count does not match", expectLineitemRows, curLineitemRows);
        Assert.assertFalse(generator.hasData());
    }

    private void testDelete(int scale, int round) throws IOException {
        File deleteFileDir = new File(TPCH_RESOURCE_DIR, String.format("delete-%dg-%d", scale, round));
        if (!deleteFileDir.exists() || !deleteFileDir.isDirectory()) {
            Assert.fail(deleteFileDir.getAbsolutePath() + " not exists or not directory");
        }
        for (int i = 1; i <= round; i++) {
            OrderLineDeleteGenerator generator = new OrderLineDeleteGenerator(scale, i);
            String batchDeleteOrderkey = generator.getAllDeleteOrderkey();
            String[] keys = batchDeleteOrderkey.split(",");
            int idx = 0;

            File deletePart = new File(deleteFileDir, "delete." + i);
            if (!deletePart.exists() || !deletePart.isFile()) {
                Assert.fail(deletePart.getAbsolutePath() + " not exists or not file");
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(deletePart))) {
                String line = null;
                int lineIdx = 1;
                while ((line = reader.readLine()) != null) {
                    String deleteKey = line.substring(0, line.length() - 1);
                    Assert.assertTrue(idx < keys.length);
                    Assert.assertEquals("Failed in file: " + deletePart.getName() + ", at line: " + lineIdx, keys[idx],
                        deleteKey);
                    idx++;
                    lineIdx++;
                }
            }
            logger.info("{} checked successfully", deletePart.getName());
        }
    }

    private void testInsert(int scale, int round) throws IOException {
        File insertFileDir = new File(TPCH_RESOURCE_DIR, String.format("insert-%dg-%d", scale, round));
        if (!insertFileDir.exists() || !insertFileDir.isDirectory()) {
            Assert.fail(insertFileDir.getAbsolutePath() + " not exists or not directory");
        }
        for (int i = 1; i <= round; i++) {
            OrderLineInsertGenerator insertGenerator = new OrderLineInsertGenerator(scale, i);

            File insertLineitemPart = new File(insertFileDir, "lineitem.tbl.u" + i);
            File insertOrdersPart = new File(insertFileDir, "orders.tbl.u" + i);
            if (!insertLineitemPart.exists() || !insertLineitemPart.isFile()) {
                Assert.fail(insertLineitemPart.getAbsolutePath() + " not exists or not file");
            }
            if (!insertOrdersPart.exists() || !insertOrdersPart.isFile()) {
                Assert.fail(insertOrdersPart.getAbsolutePath() + " not exists or not file");
            }

            try (BufferedReader orderReader = new BufferedReader(new FileReader(insertOrdersPart));
                BufferedReader lineitemReader = new BufferedReader(new FileReader(insertLineitemPart))) {
                int orderFileLineIdx = 1;
                int lineitemFileLineIdx = 1;
                String[] expectedParts;
                String[] actualParts;
                while (insertGenerator.hasData()) {
                    insertGenerator.nextRow();
                    // check orders
                    String orderRow = insertGenerator.getOrderRow();
                    String orderStrLine = orderReader.readLine();
                    Assert.assertNotNull(orderStrLine);
                    orderStrLine = orderStrLine.substring(0, orderStrLine.length() - 1);
                    expectedParts = orderStrLine.split("\\|");
                    actualParts = orderRow.split("\\|");
                    Assert.assertEquals(
                        "Failed in file: " + insertOrdersPart.getName() + ", at line: " + orderFileLineIdx,
                        expectedParts.length, actualParts.length);
                    for (int j = 0; j < expectedParts.length; j++) {
                        String actualValues = actualParts[j];
                        if (actualValues.startsWith("\"")) {
                            actualValues = actualValues.substring(1, actualValues.length() - 1);

                            Assert.assertEquals(
                                "Failed in file: " + insertOrdersPart.getName() + ", at line: " + orderFileLineIdx
                                    + ", field: "
                                    + j, expectedParts[j], actualValues);

                        }
                    }
                    orderFileLineIdx++;

                    // check lineitem
                    String lineitemRowCollect = insertGenerator.getLineitemRows();
                    String[] lineitemRows = lineitemRowCollect.split("\n");
                    for (String lineitemRow : lineitemRows) {
                        String lineitemStrLine = lineitemReader.readLine();
                        Assert.assertNotNull(lineitemStrLine);
                        expectedParts = lineitemStrLine.split("\\|");
                        actualParts = lineitemRow.split("\\|");
                        for (int j = 0; j < expectedParts.length; j++) {
                            String actualValues = actualParts[j];
                            if (actualValues.startsWith("\"")) {
                                actualValues = actualValues.substring(1, actualValues.length() - 1);

                                Assert.assertEquals(
                                    "Failed in file: " + insertLineitemPart.getName() + ", at line: "
                                        + lineitemFileLineIdx + ", field: "
                                        + j, expectedParts[j], actualValues);

                            }
                        }
                        lineitemFileLineIdx++;
                    }

                }

                Assert.assertNull(orderReader.readLine());
                Assert.assertNull(lineitemReader.readLine());

                logger.info("{} checked successfully", insertOrdersPart.getName());
                logger.info("{} checked successfully", insertLineitemPart.getName());
            }
        }
    }
}
