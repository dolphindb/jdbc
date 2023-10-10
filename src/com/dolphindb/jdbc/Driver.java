package com.dolphindb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * url: jdbc:dolphindb://hostName:port?databasePath=
 *      or jdbc:dolphindb://databasePath=
 * hostName default localhost
 * port default 8848
 *
 */

public class Driver implements java.sql.Driver {
    private static final String URL_PREFIX = "jdbc:dolphindb://";
    public static final String DB = "system_db";
    public static final Properties SYSTEM_PROPS = System.getProperties();
    public static final int V=2, v=0;

    private static final Logger LOGGER = Logger.getLogger("dolphindb");
    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        return createConnection(url, properties);
    }

    @Override
    public boolean acceptsURL(String s) throws SQLException {
        return isValidURL(s);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return V;
    }

    @Override
    public int getMinorVersion() {
        return v;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOGGER;
    }

    public static boolean isValidURL(String url) {
        return url != null && url.toLowerCase().startsWith(URL_PREFIX);
    }

    public Connection createConnection(String url, Properties prop) throws SQLException {
        if (!isValidURL(url)) new SQLException("url is not valid");
        String old_url = url;
        url = url.trim().substring(URL_PREFIX.length());
        if (url.length() == 0 || url.equals("?")){
            prop.setProperty("hostName","localhost");
            prop.setProperty("port","8848");
            return new JDBCConnection(old_url,prop);
        }
        else {
            String[] strings = url.split("\\?");
            if(strings.length == 1){
                String s = strings[0];
                if(s.length() >0){
                    if(s.contains("=")){
                        prop.setProperty("hostName", "localhost");
                        prop.setProperty("port", "8848");
                        Utils.parseProperties(s,prop,"&","=");
                    }else{
                        String[] hostname_port = s.split(":");
                        if(hostname_port.length ==2) {
                            prop.setProperty("hostName", hostname_port[0]);
                            prop.setProperty("port", hostname_port[1]);
                        }else{
                            throw new SQLException("hostname_port " + strings[0] + " error");
                        }
                    }
                }
            } else if(strings.length == 2){
                String s1 = strings[0];
                if (s1.length() > 0) {
                    String[] hostname_port = s1.split(":");
                    if (hostname_port.length == 2) {
                        prop.setProperty("hostName", hostname_port[0]);
                        prop.setProperty("port", hostname_port[1]);
                    }
                    else{
                        throw new SQLException("hostname_port " + strings[0] + " error");
                    }
                } else{
                    prop.setProperty("hostName", "localhost");
                    prop.setProperty("port", "8848");
                }
                String s2 = strings[1];
                if (s2.length() > 0) {
                    // 这一步会解析jdbc连接配置字符串中的属性，并以kv的方式添加到prop里
                    Utils.parseProperties(s2,prop,"&","=");
                }
            }
            return new JDBCConnection(old_url,prop);
        }
    }

    public static void unused(String s)throws SQLException{
        throw new SQLException(s);
    }

    public static void unused()throws SQLException{
        throw new SQLFeatureNotSupportedException("The current method is not supported.");
    }

    public static void unused(Exception e)throws SQLException{
        throw new SQLException(e.getMessage());
    }
}
