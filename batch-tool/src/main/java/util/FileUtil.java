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

package util;

import model.config.ConfigConstant;
import model.db.FieldMetaInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileUtil {
    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);
    /**
     * NULL字段的转义
     * 参考 https://dev.mysql.com/doc/refman/8.0/en/null-values.html
     */
    public static final String NULL_STR = "NULL";
    public static final ByteBuffer NULL_STR_WITH_COMMA_BYTE_BUFFER = ByteBuffer.wrap("NULL,".getBytes());
    public static final byte[] NULL_STR_BYTE = "NULL".getBytes();
    public static final ByteBuffer NULL_STR_BYTE_BUFFER = ByteBuffer.wrap(NULL_STR_BYTE);
    public static final String NULL_ESC_STR = "\\N";
    // CSV读取转义后仍为\N
    public static final String NULL_ESC_STR_IN_QUOTE = "\\\\N";
    public static final byte[] DOUBLE_QUOTE_BYTE = "\"".getBytes();
    public static final byte[] BACK_SLASH_BYTE = "\\".getBytes();
    public static final byte[] NULL_ESC_BYTE = NULL_ESC_STR.getBytes();
    public static final byte[] NULL_ESC_BYTE_IN_QUOTE = NULL_ESC_STR_IN_QUOTE.getBytes();
    public static final byte[] LF_BYTE = "\n".getBytes();
    public static final byte[] CR_BYTE = "\r".getBytes();
    public static final byte[] SYS_NEW_LINE_BYTE = System.lineSeparator().getBytes();
    public static final ByteBuffer NEW_LINE_BYTE_BUFFER = ByteBuffer.wrap(SYS_NEW_LINE_BYTE);

    public static void writeToByteArrayStream(ByteArrayOutputStream os, byte[] value) throws IOException {
        if (value != null) {
            writeWithQuoteEscape(os, value);
        } else {
            os.write(FileUtil.NULL_ESC_BYTE);
        }
    }

    public static void writeToByteArrayStreamInQuote(ByteArrayOutputStream os, byte[] value) throws IOException {
        if (value != null) {
            writeWithQuoteEscapeInQuote(os, value);
        } else {
            os.write(FileUtil.NULL_ESC_BYTE_IN_QUOTE);
        }
    }

    /**
     * 如果value中包含`"`
     * 则用`""`进行转义
     */
    private static void writeWithQuoteEscape(ByteArrayOutputStream os, byte[] value) {
        // ascii字符为一字节
        byte quoteByte = DOUBLE_QUOTE_BYTE[0];
        for (byte b : value) {
            if (b == quoteByte) {
                os.write(quoteByte);
                os.write(quoteByte);
            } else {
                os.write(b);
            }
        }
    }

    /**
     * 如果value中包含`"` `\`
     * 则用`""` `\\`进行转义
     */
    private static void writeWithQuoteEscapeInQuote(ByteArrayOutputStream os, byte[] value) {
        // ascii字符为一字节
        byte quoteByte = DOUBLE_QUOTE_BYTE[0];
        byte backSlashByte = BACK_SLASH_BYTE[0];

        for (byte b : value) {
            if (b == quoteByte) {
                os.write(quoteByte);
                os.write(quoteByte);
            } else if (b == backSlashByte) {
                os.write(backSlashByte);
                os.write(backSlashByte);
            } else {
                os.write(b);
            }
        }
    }

    public static void writeToByteArrayStreamWithQuote(ByteArrayOutputStream os, byte[] value) throws IOException {
        os.write(DOUBLE_QUOTE_BYTE);
        writeToByteArrayStreamInQuote(os, value);
        os.write(DOUBLE_QUOTE_BYTE);
    }

    public static byte[] getHeaderBytes(List<FieldMetaInfo> metaInfoList, byte[] separator) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        int len = metaInfoList.size();
        try {
            for (int i = 0; i < len - 1; i++) {
                FileUtil.writeToByteArrayStream(os, metaInfoList.get(i).getName().getBytes());
                // 附加分隔符
                os.write(separator);
            }
            FileUtil.writeToByteArrayStream(os, metaInfoList.get(len - 1).getName().getBytes());
            // 附加换行符
            os.write(SYS_NEW_LINE_BYTE);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return os.toByteArray();
    }

    public static ByteBuffer getNullStrWithCommaByteBuffer() {
        NULL_STR_WITH_COMMA_BYTE_BUFFER.rewind();
        return NULL_STR_WITH_COMMA_BYTE_BUFFER;
    }


    public static String[] split(String line, String sep, boolean withLastSep, boolean hasEscapedQuote) {
        ArrayList<String> values = splitWithQuoteEscape(line, sep, withLastSep, 10, hasEscapedQuote);
        return values.toArray(new String[values.size()]);
    }

    /**
     * 不能直接做split
     * 因为字段内容可能包含分隔符
     *
     * @param line 已判不为空字符串
     * @param expectedCount 预期的字段数
     */
    public static String[] split(String line, String sep, boolean withLastSep, int expectedCount,
                                 boolean hasEscapedQuote) {
        ArrayList<String> values = splitWithQuoteEscape(line, sep, withLastSep, expectedCount, hasEscapedQuote);
        if (values.size() != expectedCount) {
            badFormatException(line, expectedCount, values.size());
        }
        return values.toArray(new String[values.size()]);
    }

    private static ArrayList<String> splitWithQuoteEscape(String line, String sep, final boolean withLastSep,
                                                          int expectedCount, final boolean hasEscapedQuote) {

        char[] chars = line.toCharArray();
        int len = chars.length;
        if (withLastSep) {
            // 结尾有分隔符则忽略
            len -= sep.length();
        }
        ArrayList<String> subStrings = new ArrayList<>(expectedCount);
        StringBuilder stringBuilder = new StringBuilder(line.length() / expectedCount);
        char sepStart = sep.charAt(0);
        boolean enclosingByQuote = false;
        boolean endsWithSep = false;
        for (int i = 0; i < len; i++) {
            if (i == len - 1) {
                // 最后一个字符
                if (chars[i] == '\"' && hasEscapedQuote) {
                    stringBuilder.append(chars[i]);
                    subStrings.add(stringBuilder.toString());
                    stringBuilder.setLength(0);
                    break;
                }
                if (chars[i] != '\"') {
                    if (!hasEscapedQuote && enclosingByQuote) {
                        badFormatException("Unclosed quote", line);
                    } else {
                        // 说明当前为最后一个字段
                        stringBuilder.append(chars[i]);
                        subStrings.add(stringBuilder.toString());
                        stringBuilder.setLength(0);
                    }
                    break;
                }
                badFormatException("Failed to split", line);
            }
            if (chars[i] == '\"' && !hasEscapedQuote) {
                if (!enclosingByQuote) {
                    enclosingByQuote = true;
                } else {
                    // look ahead
                    if (i + 1 < len) {
                        if (chars[i + 1] == '\"') {
                            // 转义为单个双引号
                            stringBuilder.append('\"');
                            i++;
                        } else {
                            // 理论上后面只能为分隔符
                            // 此处直接假设是分隔符
                            // 如果最后发现字段数不对 说明该行格式有误
                            subStrings.add(stringBuilder.toString());
                            stringBuilder.setLength(0);
                            enclosingByQuote = false;
                            i += sep.length();
                        }
                    } else {
                        // 说明当前为最后一个字段
                        subStrings.add(stringBuilder.toString());
                        stringBuilder.setLength(0);
                        enclosingByQuote = false;
                    }
                }
            } else if (chars[i] == sepStart && !enclosingByQuote) {
                // 判断是否为分隔符
                // 匹配剩余字符
                int j = i + 1;
                int end = j + sep.length() - 1;
                for (int k = 1; j < end && chars[j] == sep.charAt(k); j++, k++) {
                    // do nothing
                }
                if (j == end) {
                    // 匹配成功
                    subStrings.add(stringBuilder.toString());
                    stringBuilder.setLength(0);
                    enclosingByQuote = false;
                    i += sep.length() - 1;
                    if (i == len -1) {
                        endsWithSep = true;
                    }
                } else {
                    stringBuilder.append(chars[i]);
                }
            } else {
                stringBuilder.append(chars[i]);
            }
        }
        if (endsWithSep && !withLastSep) {
            subStrings.add("");
        }
        return subStrings;
    }

    private static void badFormatException(String msg, String line) {
        throw new IllegalArgumentException(msg + " in line: " + line);
    }

    private static void badFormatException(String line, int expectedCount, int actualCount)
        throws IllegalFormatException {
        throw new IllegalArgumentException(String.format("Bad format line: %s. Expected field count: %d, found: %d",
            line, expectedCount, actualCount));
    }

    public static String trimEndAndMoveQuote(String s) {
        char[] chars = s.toCharArray();
        int len = chars.length;
        int st = 0;
        while ((st < len) && (chars[len - 1] <= ' ')) {
            len--;
        }
        while ((st < len) && (chars[st] <= ' ')) {
            st++;
        }

        // 判断引号
        if (len >= 2 && chars[len - 1] == '\"' && chars[st] == '\"') {
            st++;
            len--;
            final String tmp = s.substring(st, len);
            // Replace "" to ".
            if (tmp.contains("\"\"")) {
                return tmp.replaceAll("\"\"", "\"");
            } else {
                return tmp;
            }
        }
        return (len < chars.length) ? s.substring(0, len) : s;
    }

    /**
     * 判断原字节数组中是否包含特殊字符
     *
     * @param targetList 特殊字符列表
     */
    public static boolean containsSpecialBytes(byte[] source, List<byte[]> targetList) {
        if (source == null || source.length == 0) {
            return false;
        }

        int specialCharCount = targetList.size();

        final int minTargetLen = 1;

        int max = source.length - minTargetLen;
        byte[] cur;
        byte first;
        int targetLen;
        for (int src_i = 0; src_i <= max; src_i++) {
            // 找到第一个匹配的字符
            for (int n = 0; n < specialCharCount; n++) {
                cur = targetList.get(n);
                first = cur[0];
                targetLen = cur.length;
                if (source[src_i] != first) {
                    continue;
                }

                // 匹配剩余字符
                int src_j = src_i + 1;
                int end = src_j + targetLen - 1;
                for (int k = 1; src_j < end && source[src_j] == cur[k]; src_j++, k++) {

                }

                if (src_j == end) {
                    // 匹配成功
                    return true;
                }
            }
        }
        return false;
    }

    public static String getFilePathPrefix(String path, String filenamePrefix, String tableName) {
        return String.format("%s%s%s_", path, filenamePrefix, tableName);
    }

    public static Map<String, List<File>> getDataFile(String baseDirectory) {
        File dir = new File(baseDirectory);
        if (!dir.isDirectory()) {
            logger.error("Not valid directory.");
            return null;
        }

        final Map<String, List<File>> result = new HashMap<>();
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (!file.isFile()) {
                continue;
            }
            final String[] items = file.getName().split("\\.");
            if (items.length < 3) {
                logger.error("Error file name: " + file.getName());
                return null;
            }
            final String tableName = items[2].toLowerCase();
            final List<File> fileList = result.computeIfAbsent(tableName, k -> new ArrayList<>());
            fileList.add(file);
        }
        return result;
    }

    /**
     * 筛选出非ddl的数据文件
     */
    public static List<String> getFilesAbsPathInDir(String dirPathStr) {
        File dir = new File(dirPathStr);
        if (!dir.exists()|| !dir.isDirectory()) {
            throw new IllegalArgumentException(String.format("[%s] does not exist or is not a directory", dirPathStr));
        }
        return FileUtils.listFiles(dir, null, false).stream()
            .filter(file -> file.isFile() && file.canRead() &&
                !file.getName().endsWith(ConfigConstant.DDL_FILE_SUFFIX))
            .map(File::getAbsolutePath).collect(Collectors.toList());
    }

    public static String getFileAbsPath(String filename) {
        File file = new File(filename);
        if (!file.exists() || !file.isFile() || !file.canRead()) {
            throw new IllegalArgumentException("Failed to read from " + filename);
        }
        return file.getAbsolutePath();
    }
}
