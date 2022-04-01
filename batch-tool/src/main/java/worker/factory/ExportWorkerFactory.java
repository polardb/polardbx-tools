package worker.factory;

import cmd.ExportCommand;
import exception.DatabaseException;
import model.config.ExportConfig;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import model.encrypt.Cipher;
import util.DbUtil;
import worker.export.DirectExportWorker;
import worker.export.order.DirectOrderByExportWorker;

import javax.sql.DataSource;
import java.sql.SQLException;

public class ExportWorkerFactory {

    public static DirectExportWorker buildDefaultDirectExportWorker(DataSource druid,
                                                                    TableTopology topology,
                                                                    TableFieldMetaInfo tableFieldMetaInfo,
                                                                    String filename,
                                                                    ExportConfig config) {
        DirectExportWorker directExportWorker;
        switch (config.getExportWay()) {
        case MAX_LINE_NUM_IN_SINGLE_FILE:
            directExportWorker = new DirectExportWorker(druid,
                topology, tableFieldMetaInfo,
                config.getLimitNum(),
                filename,
                config.getSeparator(), config.isWithHeader(),
                config.getQuoteEncloseMode(), config.getCompressMode());
            break;
        case DEFAULT:
            directExportWorker = new DirectExportWorker(druid,
                topology, tableFieldMetaInfo,
                filename,
                config.getSeparator(), config.isWithHeader(),
                config.getQuoteEncloseMode(), config.getCompressMode());
            break;
        case FIXED_FILE_NUM:
        default:
            throw new UnsupportedOperationException("Do not support direct export when fixed file num");
        }
        directExportWorker.setWhereCondition(config.getWhereCondition());
        directExportWorker.setCipher(Cipher.getCipher(config.getEncryptionConfig(), true));
        return directExportWorker;
    }

    public static DirectOrderByExportWorker buildDirectExportWorker(DataSource druid,
                                                                    TableFieldMetaInfo tableFieldMetaInfo,
                                                                    ExportCommand command) {
        ExportConfig config = command.getExportConfig();
        int maxLine = 0;
        switch (config.getExportWay()) {
        case MAX_LINE_NUM_IN_SINGLE_FILE:
            maxLine = config.getLimitNum();
            break;
        case FIXED_FILE_NUM:
            // 固定文件数的情况 先拿到全部的行数
            double totalRowCount;
            try {
                totalRowCount = DbUtil.getTableRowCount(druid.getConnection(), command.getTableName());
            } catch (DatabaseException | SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            int fileNum = config.getLimitNum();
            maxLine = (int) Math.ceil(totalRowCount / fileNum);
            break;
        case DEFAULT:
        default:
            break;
        }
        return new DirectOrderByExportWorker(druid, command.getFilePathPrefix(),
                tableFieldMetaInfo,
                command.getTableName(), config.getOrderByColumnNameList(), maxLine,
                config.getSeparator().getBytes(),
                config.isAscending());
    }
}
