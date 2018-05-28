package com.xxdb.jdbc;

import com.xxdb.DBConnection;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public  class _DBConnection implements Connection {

    private DBConnection db;

    private String file;
    private String fileName;
    private String hostName;
    private int port;
    private boolean success;


    public _DBConnection(String file, String fileName, Properties prop) throws SQLException{
        //this.url = url;
        //this.fileName = extractPragmasFromFilename(fileName, prop);
        this.file = file;
        this.fileName = fileName;
        db = new DBConnection();
        hostName = prop.getProperty("hostName");
        port = Integer.parseInt(prop.getProperty("port"));
        try {
            open(hostName,port);
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

    private void open(String hostname, int port) throws SQLException, IOException{
//        File file = new File(file).getAbsoluteFile();
//        File parent = file.getParentFile();
//        if (parent != null && !parent.exists()) {
//            for (File up = parent; up != null && !up.exists();) {
//                parent = up;
//                up = up.getParentFile();
//            }
//            throw new SQLException("path to '" + fileName + "': '" + parent + "' does not exist");
//        }
//
//        // check write access if file does not exist
//        try {
//            if (!file.exists() && file.createNewFile())
//                file.delete();
//        }
//        catch (Exception e) {
//            throw new SQLException("opening db: '" + fileName + "': " + e.getMessage());
//        }
//        fileName = file.getAbsolutePath();

        System.out.println(hostname+port);
        success = db.connect(hostname, port);
        if(!success) throw new SQLException("Connection is fail");
        db.run(MessageFormat.format("{1} = loadTable(\"{0}\",`{1})",file,fileName));
    }


    private String extractPragmasFromFilename(String filename, Properties prop) throws SQLException {
        int parameterDelimiter = filename.indexOf('?');
        if (parameterDelimiter == -1) {
            // nothing to extract
            return filename;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(filename.substring(0, parameterDelimiter));

        int nonPragmaCount = 0;
        String [] parameters = filename.substring(parameterDelimiter + 1).split("&");
        for (int i = 0; i < parameters.length; i++) {
            // process parameters in reverse-order, last specified pragma value wins
            String parameter = parameters[parameters.length - 1 - i].trim();

            if (parameter.isEmpty()) {
                // duplicated &&& sequence, drop
                continue;
            }

            String [] kvp = parameter.split("=");
            String key = kvp[0].trim().toLowerCase();
            sb.append(nonPragmaCount == 0 ? '?' : '&');
            sb.append(parameter);
            nonPragmaCount++;
        }

        final String newFilename = sb.toString();
        return newFilename;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkIsClosed();
        return new DBStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String s) throws SQLException {
        checkIsClosed();
        return new DBPrepareStatement(this,s);
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

        db.close();
        db = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return db == null;
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
        if(getDb() == null){
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
        if(db==null) throw new SQLException("db is null");

        if(this==null || this.isClosed()) throw new SQLException("connection isClosed");
    }

    public DBConnection getDb() {
        return db;
    }

    protected void checkOpen() throws SQLException {
        if (isClosed())
            throw new SQLException("database connection closed");
    }



}
