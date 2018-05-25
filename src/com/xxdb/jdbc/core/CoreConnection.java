package com.xxdb.jdbc.core;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

public abstract class CoreConnection {

    protected DBConnection db;

    private String file;
    private String fileName;
    private boolean success;
    private BasicTable table;


    public CoreConnection(String file, String fileName, Properties prop) throws SQLException{
        //this.url = url;
        //this.fileName = extractPragmasFromFilename(fileName, prop);
        this.file = file;
        this.fileName = fileName;
        open(prop.getProperty("hostName"),Integer.parseInt(prop.getProperty("port")));
    }

    private void open(String hostname, int port) throws SQLException{
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

        db = new DBConnection();
        try {
            System.out.println(hostname+port);
            success = db.connect(hostname, port);
            db.run(MessageFormat.format("{1} = loadTable(\"{0}\",`{1})",file,fileName));
            table = (BasicTable) db.run(fileName);
        }catch (Exception e){
            e.printStackTrace();
        }

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

    public DBConnection getDb() {
        return db;
    }

    public BasicTable getTable() {
        return table;
    }

    protected void checkOpen() throws SQLException {
        if (isClosed())
            throw new SQLException("database connection closed");
    }

    protected boolean isClosed() throws SQLException {
        return db == null;
    }


    /**
     * @see java.sql.Connection#close()
     */
    public void close() throws SQLException {
        if (isClosed())
            return;

        db.close();
        db = null;
    }


}
