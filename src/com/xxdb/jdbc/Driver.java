package com.xxdb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * 协议: jdbc:dolphindb://hostName:port;databasePath=
 *      或者 jdbc:dolphindb://databasePath=
 * hostName default localhost
 * port default 8848
 */

public class Driver implements java.sql.Driver {
    private static final String URL_PREFIX = "jdbc:dolphindb://";
    static int V=2,v=0;
    static {
        try {
            DriverManager.registerDriver(new Driver());
        }
        catch (SQLException e) {
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
        return null;
    }

    public static boolean isValidURL(String url) {
        return url != null && url.toLowerCase().startsWith(URL_PREFIX);
    }


    public static Connection createConnection(String url, Properties prop) throws SQLException {
        if (!isValidURL(url))
            return null;

        url = url.trim().substring(URL_PREFIX.length());
        System.out.println(url);
        String[] s = url.split(";");
        String databasePath;

        if(s.length == 1){
            prop.setProperty("hostName","localhost");
            prop.setProperty("port","8848");
            databasePath = s[0].split("=")[1];
        }else{
            String[] hostname_port = s[0].split(":");
            prop.setProperty("hostName",hostname_port[0]);
            prop.setProperty("port",hostname_port[1]);
            databasePath = s[1].split("=")[1];
        }

        int index = databasePath.lastIndexOf("/");
        String file = databasePath.substring(0,index);
        String filename = databasePath.substring(index+1);
        return new _DBConnection(file,filename, prop);
    }

    public static void unused(String s)throws SQLException{
        throw new SQLException(s);
    }
    public static void unused()throws SQLException{
        throw new SQLFeatureNotSupportedException("NotSupported");
    }
    public static void unused(Exception e)throws SQLException{
        throw new SQLException(e.getMessage());
    }
}
