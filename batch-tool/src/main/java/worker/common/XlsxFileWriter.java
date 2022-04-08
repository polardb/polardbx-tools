package worker.common;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import util.IOUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class XlsxFileWriter implements IFileWriter {

    private Workbook wb;
    private OutputStream outputStream;

    private Sheet sheet;
    private String tableName;

    @Override
    public void nextFile(String fileName) {
        this.wb = new XSSFWorkbook();
        File file = new File(fileName);
        FileUtils.deleteQuietly(file);
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(file.getAbsolutePath());
            wb.write(outputStream);
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtil.close(outputStream);
        }
        this.sheet = wb.createSheet(tableName);
    }

    @Override
    public void writeLine(String[] values) {

    }

    @Override
    public void close() {
        IOUtil.flush(outputStream);
        IOUtil.close(outputStream);
        IOUtil.close(wb);
        this.sheet = null;
    }
}
