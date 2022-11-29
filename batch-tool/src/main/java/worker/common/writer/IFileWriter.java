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

import java.io.Closeable;

public interface IFileWriter extends Closeable {

    void nextFile(String fileName);

    default void write(byte[] data) {
        throw new UnsupportedOperationException(getClass() + " does not support write raw bytes");
    }

    default void writeLine(String[] values) {
        throw new UnsupportedOperationException(getClass() + " does not support write line with values");
    }

    boolean produceByBlock();

    void close();
}
