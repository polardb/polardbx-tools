package worker.ddl;

import model.config.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 直接通过source导入库表
 */
public class DdlImportWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DdlExportWorker.class);
    private final String filepath;
    private final DataSource druid;

    public DdlImportWorker(String dbOrTbName, DataSource druid) {
        this.druid = druid;
        String filename = dbOrTbName + ConfigConstant.DDL_FILE_SUFFIX;
        File file = new File(filename);
        if (!file.exists() || !file.isFile()) {
            throw new IllegalStateException("File " + filename + " does not exist");
        }
        this.filepath = file.getAbsolutePath();
    }

    @Override
    public void run() {
        BufferedReader reader = null;
        String line = null;
        try (Connection conn = druid.getConnection();
            Statement stmt = conn.createStatement()) {
            reader = new BufferedReader(new FileReader(filepath));
            StringBuilder sqlStringBuilder = new StringBuilder(100);
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("--")) {
                    continue;
                }
                if (!line.endsWith(";")) {
                    sqlStringBuilder.append(line).append("\n");
                } else {
                    sqlStringBuilder.append(line);
                    String sql = sqlStringBuilder.toString();
                    stmt.execute(sql);
                    sqlStringBuilder.setLength(0);
                }
            }
        } catch (IOException | SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
        logger.info("DDL语句导入完毕");
    }
}
