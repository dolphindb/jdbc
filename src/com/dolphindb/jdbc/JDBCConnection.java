/**
 * Hard code part controlHost in open method
 */
package com.dolphindb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.comm.SqlStdEnum;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.ProgressListener;
import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Executor;

public class JDBCConnection implements Connection {
	private DBConnection dbConnection;
	private final String hostName;
	private final int port;
	private boolean success;
	private String database;
	private Vector tables;
	private final String url;
	private DatabaseMetaData metaData;
	private String user;
	private String password;

	public JDBCConnection(String url, Properties prop) throws SQLException {
		this.url = url;
		String sqlStdProp = prop.getProperty("sqlStd");
		if (Objects.nonNull(sqlStdProp)) {
			SqlStdEnum sqlStd = SqlStdEnum.getByName(sqlStdProp);
			dbConnection = new DBConnection(sqlStd);
		} else {
			dbConnection = new DBConnection();
		}
		hostName = prop.getProperty("hostName");
		port = Integer.parseInt(prop.getProperty("port"));
		setUser(null);
		setPassword(null);
        clientInfo = prop;
		try {
			open(hostName, port, prop);
		} catch (IOException e) {
			e.printStackTrace();
			String s = e.getMessage();
			if (s.contains("Connection refused")) {
				throw new SQLException(MessageFormat.format("{0}  ==> hostName = {1}, port = {2}", s, hostName, port));
			} else {
				throw new SQLException(e);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public DBConnection getDBConnection() {
		return dbConnection;
	}
	
	public void setDBConnection(DBConnection dbConnection) {
		this.dbConnection = dbConnection;
	}

	/**
	 * build connect to port
	 * 
	 * @param hostname
	 * @param port
	 * @param prop
	 * @throws IOException
	 * @throws SQLException
	 */
	private void connect(String hostname, int port, Properties prop, String appendInitScript) throws IOException, SQLException {
		String userId = prop.getProperty("user");
		String password = prop.getProperty("password");
		String initialScript = prop.getProperty("initialScript");
		initialScript = Utils.changeCase(initialScript);
		if (initialScript!=null&&initialScript.equals("select 1"))
			initialScript = "select 1 as val";
		if(appendInitScript != null) {
			if(initialScript!=null)
				initialScript = appendInitScript + "\n" + initialScript;
			else
				initialScript = appendInitScript;
		}

		String highAvailabilityStr = prop.getProperty("highAvailability");
		String enableHighAvailabilityStr = prop.getProperty("enableHighAvailability");
		Boolean highAvailability = false;
		if(highAvailabilityStr == null){
			highAvailability = Boolean.valueOf(enableHighAvailabilityStr);
		}else if(enableHighAvailabilityStr == null){
			highAvailability = Boolean.valueOf(highAvailabilityStr);
		}else{
			Boolean param1 = Boolean.valueOf(highAvailabilityStr);
			Boolean param2 = Boolean.valueOf(enableHighAvailabilityStr);
			if(param1 != param2)
				throw new SQLException("The values of the \"highAvailability\" and \"enableHighAvailability\" parameters in the URL must be the same if both are configured. ");
			highAvailability = param1;
		}

		String rowHighAvailabilitySites = prop.getProperty("highAvailabilitySites");
		String[] highAvailabilitySites = null;
		if (rowHighAvailabilitySites != null) {
			if (rowHighAvailabilitySites.contains(",")) {
				highAvailabilitySites = rowHighAvailabilitySites.split(",");
				highAvailabilitySites = Arrays.stream(highAvailabilitySites).map(String::trim).toArray(String[]::new);
			} else {
				highAvailabilitySites = rowHighAvailabilitySites.split(" ");
			}
		}

		String tableAliasValue = prop.getProperty("tableAlias");
		if (Utils.isNotEmpty(tableAliasValue)) {
			String tableAliasScript = Utils.parseTableAliasPropToScript(tableAliasValue);
			if (Objects.nonNull(initialScript)) {
				initialScript = initialScript + "\n" + tableAliasScript;
			} else {
				initialScript = tableAliasScript;
			}
		}

		if(userId != null && password != null){
			if (highAvailability){
				success = dbConnection.connect(hostname, port, userId, password, initialScript, highAvailability, highAvailabilitySites);
			}else {
				success = dbConnection.connect(hostname, port, userId, password, initialScript,false,null,true);
			}
		}else if(initialScript != null && highAvailabilitySites != null){
			success = dbConnection.connect(hostname, port, initialScript, highAvailabilitySites);
		}else {
			success = dbConnection.connect(hostName, port,"","",null,false,null,true);
		}
	}

	private String loadTables(String dbName, List<String> tableNames, boolean ignoreError){
		StringBuilder sbInitScript = new StringBuilder();
		for(String tableName:tableNames) {
			StringBuilder builder = new StringBuilder();
			builder.append("loadTable(\"").append(dbName).append("\", \"").append(tableName).append("\");");
			try {
				this.dbConnection.run(builder.toString());
				sbInitScript.append(tableName).append("=").append("loadTable(\"").append(dbName).append("\", \"").append(tableName).append("\");\n");
			} catch (Exception e) {
				if(ignoreError) {
					System.out.println("Load table " + dbName + "." + tableName + " failed " + e.getMessage());
					tableNames.remove(tableName);
				}else{
					throw new RuntimeException(e.getMessage());
				}
			}
		}
		return sbInitScript.toString();
	}

	private void open(String hostname, int port, Properties prop) throws SQLException, IOException{
		this.connect(hostname, port, prop,null);
		if (!this.success) {
			throw new SQLException("Connection is failed");
		}
		StringBuffer sbInitScript=new StringBuffer();
		String[] key = new String[]{"databasePath"};
		String[] valueName = Utils.getProperties(prop, key);
		if (valueName[0] != null && valueName[0].length() > 0) {
			this.dbConnection.run("system_db" + " = database(\"" + valueName[0] + "\");\n");
			if (valueName[0].trim().startsWith("dfs://")) {
				//this.isDFS = true;
				this.database = valueName[0];
				List<String> dbtables=new ArrayList<>();
				// if set specific tableanme to load
				if (Utils.isNotEmpty(prop.getProperty("tableName"))) {
					String tablename = prop.getProperty("tableName");
					tablename = tablename.trim();
					String[] tableNames = tablename.split(",");
					for (String tableName : tableNames) {
						if (!tableName.isEmpty())
							dbtables.add(tableName);
					}
					String script=loadTables(this.database,dbtables,false);
					sbInitScript.append(script);
				} else {
					// if not specific tableanme, load all tables; but need to authenticate every table.
					Vector vector = (Vector) this.dbConnection.run("getTables(system_db)");
					for (int i = 0; i < vector.rows(); i++) {
						dbtables.add(vector.getString(i));
					}
					String script=loadTables(this.database,dbtables,true);
					sbInitScript.append(script);
				}
				this.tables = new BasicStringVector(dbtables);
			}
		}
		String hasScripts = prop.getProperty("length");
		if (hasScripts != null) {
			int length = Integer.parseInt(prop.getProperty("length"));
			if (length > 0) {
				for(int i = 0; i < length; ++i) {
					sbInitScript.append(prop.getProperty("script" + i)+"\n");
				}
			}
		}
		if(sbInitScript.length()>0)
			this.connect(hostname, port, prop, sbInitScript.toString());
	}

	@Override
	public Statement createStatement() throws SQLException {
		checkIsClosed();
		return new JDBCStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new JDBCPrepareStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return new JDBCCallableStatement(this,sql);
	}

	@Override
	public String nativeSQL(String s) throws SQLException {
		checkIsClosed();
		return s;
	}

	@Override
	public void setAutoCommit(boolean b) throws SQLException {
		checkIsClosed();
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		checkIsClosed();
		return true;
	}

	@Override
	public void commit() throws SQLException {
		// The method 'commit()' is not supported.
	}

	@Override
	public void rollback() throws SQLException {
		// The method 'rollback' is not supported.
	}

	@Override
	public void close() throws SQLException {
		if (isClosed()) {
			return;
		}
		dbConnection.close();
		dbConnection = null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return dbConnection == null;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		checkIsClosed();
		if (metaData == null) {
			metaData = new JDBCDataBaseMetaData(this, null);
		}
		return metaData;
	}

	@Override
	public void setReadOnly(boolean b) throws SQLException {
		checkIsClosed();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		checkIsClosed();
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		if (Utils.isEmpty(catalog))
			throw new SQLException("The param catalog cannot be null or empty.");

		StringBuilder sbInitScript = new StringBuilder();

		if (Objects.nonNull(this.tables)) {
			// undef existed table handle;
			for (int i = 0; i < this.tables.rows(); i ++) {
				String tableName = this.tables.get(i).getString();
				try {
					this.dbConnection.run(tableName);
					this.dbConnection.run("undef(`" + tableName + ");");
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		try {
			this.dbConnection.run("system_db" + " = database(\"" + catalog + "\");\n");

			List<String> dbtables=new ArrayList<>();

			// if not specific tableanme, load all tables; but need to authenticate every table.
			Vector vector = (Vector) this.dbConnection.run("getTables(system_db)");
			if (vector.rows() != 0) {
				for (int i = 0; i < vector.rows(); i++)
					dbtables.add(vector.getString(i));
				this.database = catalog;
			} else {
				throw new SQLException("The catalog '" + catalog + "' doesn't exist in server.");
			}

			String script = loadTables(this.database, dbtables,true);
			sbInitScript.append(script);
			this.tables = new BasicStringVector(dbtables);
			this.dbConnection.run(sbInitScript.toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public String getCatalog() throws SQLException {
		return this.database;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
//		Driver.unused("getWarnings not implemented");
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
//		Driver.unused("clearWarnings not implemented");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return prepareCall(sql);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		Driver.unused("getTypeMap not implemented");
		return null;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
	}

	@Override
	public int getHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		Driver.unused("setSavepoint not supported");
		return null;
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		Driver.unused("setSavepoint not supported");
		return null;
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		Driver.unused("rollback not supported");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		Driver.unused("releaseSavepoint not supported");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkIsClosed();
		return createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		checkIsClosed();
		return prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		checkIsClosed();
		return prepareCall(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		checkIsClosed();
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		checkIsClosed();
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		checkIsClosed();
		return prepareStatement(sql);
	}

	@Override
	public Clob createClob() throws SQLException {
		Driver.unused("createClob not implemented");
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		Driver.unused("createBlob not implemented");
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		Driver.unused("createNClob()");
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		Driver.unused("createSQLXML()");
		return null;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return dbConnection.isConnected();
	}

	private Properties clientInfo = new Properties();

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		clientInfo.setProperty(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		clientInfo = properties;
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return clientInfo.getProperty(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return clientInfo;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return null;
	}

	@Override
	public Struct createStruct(String typeName, Object[] elements) throws SQLException {
		return null;
	}

	@Override
	public void setSchema(String schema) throws SQLException {
	}

	@Override
	public String getSchema() throws SQLException {
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> aClass) throws SQLException {
		checkIsClosed();
		return aClass.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> aClass) throws SQLException {
		checkIsClosed();
		return aClass.isInstance(this);
	}

	private void checkIsClosed() throws SQLException {
		if (dbConnection == null)
			throw new SQLException("connection isClosed");

		if (this == null || this.isClosed())
			throw new SQLException("connection isClosed");
	}

	// Automatic switching node
	public Entity run(String function, List<Entity> arguments) throws IOException {
		return this.dbConnection.run(function, arguments);
	}

	// Automatic switching node
	public Entity run(String script) throws IOException {
		return this.dbConnection.run(script);
	}

	public Entity run(String script, int fetchSize) throws IOException {
		return this.dbConnection.run(script, (ProgressListener) null, 4, 2, fetchSize);
	}

	public String getUrl() {
		return url;
	}

	public String getHostName() {
		if (this.dbConnection != null) {
			return this.dbConnection.getHostName();
		} else {
			return null;
		}
	}

	public int getPort() {
		if (this.dbConnection != null) {
			return this.dbConnection.getPort();
		} else {
			return -1;
		}
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDatabase() {
		return database;
	}
}
