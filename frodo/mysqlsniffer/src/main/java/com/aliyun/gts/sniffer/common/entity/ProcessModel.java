package com.aliyun.gts.sniffer.common.entity;

/**
 * Created by zhaoke on 17/10/28.
 */
public class ProcessModel {
    private long id=0l;
    private String user;
    private String host;
    private String DB;
    private String command;
    private int time=0;
    private String state;
    private String info;
    private long timeMs=0l;
    private long rowsSend=0l;
    private long rowsExamined=0l;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDB() {
        return DB;
    }

    public void setDB(String DB) {
        this.DB = DB;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public long getTimeMs() {
        return timeMs;
    }

    public void setTimeMs(long timeMs) {
        this.timeMs = timeMs;
    }

    public long getRowsSend() {
        return rowsSend;
    }

    public void setRowsSend(long rowsSend) {
        this.rowsSend = rowsSend;
    }

    public long getRowsExamined() {
        return rowsExamined;
    }

    public void setRowsExamined(long rowsExamined) {
        this.rowsExamined = rowsExamined;
    }
}
