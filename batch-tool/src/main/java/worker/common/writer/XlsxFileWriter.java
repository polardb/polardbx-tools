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

package worker.common.writer;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import util.IOUtil;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Excel的xlsx表格
 * 或 WPS的et表格
 */
public class XlsxFileWriter implements IFileWriter {

    private SXSSFWorkbook wb;
    private OutputStream outputStream;

    private Sheet sheet;

    private int curLine = 0;

    @Override
    public void nextFile(String fileName) {
        // streaming write XLSX
        this.wb = new SXSSFWorkbook();
        this.wb.setCompressTempFiles(true);
        this.sheet = wb.createSheet();

        File file = new File(fileName);
        FileUtils.deleteQuietly(file);
        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(file.getAbsolutePath()), 32 * 1024);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void writeLine(String[] values) {
        Row row = sheet.createRow(curLine++);
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            Cell cell = row.createCell(i);
            cell.setCellValue(value);
        }
    }

    @Override
    public boolean produceByBlock() {
        return false;
    }

    @Override
    public void close() {
        try {
            wb.write(outputStream);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        IOUtil.close(outputStream);
        wb.dispose();
        IOUtil.close(wb);
        this.sheet = null;
    }
}
