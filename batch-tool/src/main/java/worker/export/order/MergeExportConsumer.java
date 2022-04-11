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

import model.db.FieldMetaInfo;
import org.apache.commons.io.FileUtils;
import util.FileUtil;
import util.IOUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;

import static model.config.GlobalVar.EMIT_BATCH_SIZE;

public abstract class MergeExportConsumer {

    /**
     * 排序字段的信息
     */
    protected final List<FieldMetaInfo> orderByColumnInfoList;
    /**
     * 默认升序
     */
    protected final boolean isAscending = true;
    /**
     * 当前写入文件的行数
     */
    protected int curLineNum = 0;
    /**
     * 缓冲区的行数
     */
    protected int bufferedLineNum = 0;
    /**
     * 当前文件序号
     */
    protected int curFileSeq = 0;

    protected FileChannel appendChannel = null;

    protected ByteArrayOutputStream outputStream;

    /**
     * 单个文件最大行数
     */
    protected final int maxLine;
    protected final String filePath;
    protected final byte[] separator;

    protected Comparator comparator;

    protected MergeExportConsumer(List<FieldMetaInfo> orderByColumnInfoList, int maxLine,
                                  String filePath, byte[] separator) {
        this.orderByColumnInfoList = orderByColumnInfoList;
        this.maxLine = maxLine;
        this.filePath = filePath;
        this.separator = separator;

        this.outputStream = new ByteArrayOutputStream();
        File file = new File(filePath + curFileSeq);
        try {
            FileUtils.deleteQuietly(file);
            file.createNewFile();
            appendChannel = FileChannel.open(Paths.get(filePath + curFileSeq),
                StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void writeToBuffer(byte[][] rowData) throws IOException {
        if (rowData.length == 0) {
            return;
        }
        curLineNum++;
        bufferedLineNum++;
        if (bufferedLineNum == EMIT_BATCH_SIZE) {
            writeToFile(outputStream.toByteArray());
            outputStream.reset();
            bufferedLineNum = 0;
        }
        if (maxLine != 0 && curLineNum == maxLine) {
            // 开启新文件
            createNewPartFile();
        }
        for (int i = 0; i < rowData.length - 1; i++) {
            outputStream.write(rowData[i]);
            outputStream.write(separator);
        }
        outputStream.write(rowData[rowData.length - 1]);
        outputStream.write(FileUtil.SYS_NEW_LINE_BYTE);
    }


    public void writeToFile(byte[] rowData) throws IOException {
        IOUtil.writeNio(appendChannel, rowData);
    }

    private void createNewPartFile() {
        if (bufferedLineNum != 0) {
            try {
                writeToFile(outputStream.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
            outputStream.reset();
            bufferedLineNum = 0;
        }
        curFileSeq++;
        String tmpFileName = filePath + curFileSeq;
        File file = new File(tmpFileName);
        try {
            appendChannel.close();
            FileUtils.deleteQuietly(file);
            file.createNewFile();
            appendChannel = FileChannel.open(Paths.get(tmpFileName),
                StandardOpenOption.APPEND);
            curLineNum = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
