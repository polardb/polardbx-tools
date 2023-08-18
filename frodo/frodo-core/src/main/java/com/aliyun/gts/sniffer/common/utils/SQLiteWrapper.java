package com.aliyun.gts.sniffer.common.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLiteWrapper {
    private Connection conn;
    String path;
    public SQLiteWrapper(String path) throws Exception{
        this.path=path;
        Class.forName("org.sqlite.JDBC");
        conn = DriverManager.getConnection("jdbc:sqlite:"+path);
    }

    public void execute(String sql) throws SQLException {
        try(Statement stmt = conn.createStatement();){
            stmt.execute(sql);
        }
    }
    public void commit()throws SQLException {
        conn.commit();
    }

}
