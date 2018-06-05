package com.dolphindb.jdbc;

import com.xxdb.DBConnection;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public  class JDBCConnection implements Connection {

    private DBConnection dbConnection;

    private String filePath;
    private String tableName;
    private String hostName;
    private int port;
    private boolean success;
    private com.xxdb.data.Vector databases;
    private HashMap<String,Boolean> loadTable;


    public JDBCConnection(Properties prop) throws SQLException{
        this.filePath = filePath;
        this.tableName = tableName;
        dbConnection = new DBConnection();
        hostName = prop.getProperty("hostName");
        port = Integer.parseInt(prop.getProperty("port"));
        try {
            open(hostName,port,prop);
        }catch (IOException e){
            e.printStackTrace();
            String s = e.getMessage();
            if(s.contains("Connection refused")){
                throw new SQLException(MessageFormat.format("{0}  ==> hostName = {1}, port = {2}",s,hostName,port));
            }else{
                throw new SQLException(s);
            }

        }
    }

    private void open(String hostname, int port, Properties prop) throws SQLException, IOException{
        System.out.println(hostname+port);
        success = dbConnection.connect(hostname, port);
        // database(directory, [partitionType], [partitionScheme], [locations])
        if(!success) throw new SQLException("Connection is fail");
        String[] keys = new String[]{"databasePath","partitionType","partitionScheme","locations"};
        String[] values = Utils.getProperties(prop,keys);
        if(values[0] != null && values[0].length() > 0){
            values[0] = "\""+values[0]+"\"";
            System.out.println(values[0]);
            StringBuilder sb = new StringBuilder(Driver.DB).append(" = database(");
            Utils.joinOrder(sb,values,",");
            sb.append(");\n");
            sb.append("getTables(").append(Driver.DB).append(")");
            databases = (com.xxdb.data.Vector) dbConnection.run(sb.toString());
            if(values[0].trim().startsWith("dfs://")) {
                StringBuilder loadTableSb = new StringBuilder();
                for (int i = 0, len = databases.rows(); i < len; ++i) {
                    String name = databases.get(i).getString();
                    loadTableSb.append(name).append(" = ").append("loadTable(").append(Driver.DB).append(",`").append(name).append(");\n");
                }
                dbConnection.run(loadTableSb.toString());
            }
            //loadTable = new HashMap<>(databases.rows());
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkIsClosed();
        return new JDBCStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String s) throws SQLException {
        checkIsClosed();
        return new JDBCPrepareStatement(this,s);
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        checkIsClosed();
        return null;
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        checkIsClosed();
        return s;
    }

    @Override
    public void setAutoCommit(boolean b) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        if (isClosed())
            return;

        dbConnection.close();
        dbConnection = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return dbConnection == null;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String s) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int i, int i1) throws SQLException {
        checkIsClosed();
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
        checkIsClosed();
        return prepareStatement(s);
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
        checkIsClosed();
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int i) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int i, int i1, int i2) throws SQLException {
        checkIsClosed();
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
        checkIsClosed();
        return prepareStatement(s);
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i) throws SQLException {
        checkIsClosed();
        return prepareStatement(s);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
        checkIsClosed();
        return prepareStatement(s);
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        checkIsClosed();
        return prepareStatement(s);
    }

    @Override
    public Clob createClob() throws SQLException {
        // TODO Support this
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO Support this
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO Support this
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Support this
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        if(getDbConnection() == null){
            return false;
        }
        //TODO
        return true;

    }

    @Override
    public void setClientInfo(String s, String s1) throws SQLClientInfoException {
        // TODO Auto-generated method stub
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // TODO Auto-generated method stub
    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String s) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int i) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return aClass.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return aClass.isInstance(this);
    }

    private void checkIsClosed() throws SQLException{
        if(dbConnection ==null) throw new SQLException("dbConnection is null");

        if(this==null || this.isClosed()) throw new SQLException("connection isClosed");
    }

    public DBConnection getDbConnection() {
        return dbConnection;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFilePath() {
        return filePath;
    }

    protected void checkOpen() throws SQLException {
        if (isClosed())
            throw new SQLException("database connection closed");
    }



}
