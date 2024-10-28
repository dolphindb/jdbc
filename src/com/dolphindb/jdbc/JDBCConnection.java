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
	private String hostName;
	private int port;
	private boolean success;
	private String database;
	private String catalog;
	private Vector tables;
	private String url;
	private DatabaseMetaData metaData;
	private String user;
	private String password;

	public JDBCConnection(String url, Properties prop) throws SQLException {
		this.url = url;
		Driver.parseProp(url, prop);
		this.clientInfo = prop;
		this.hostName = this.clientInfo.getProperty("hostName");
		this.port = Integer.parseInt(this.clientInfo.getProperty("port"));
		setUser(Optional.ofNullable(this.clientInfo.getProperty("user")).orElse(""));

		initDBConnectionInternal(prop);

		try {
			connectInternal(this.hostName, this.port, this.clientInfo);
		} catch (IOException e) {
			e.printStackTrace();
			String msg = e.getMessage();
			if (msg.contains("Connection refused"))
				throw new SQLException(MessageFormat.format("{0}  ==> hostName = {1}, port = {2}", msg, this.hostName, this.port));
			else
				throw new SQLException(e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected JDBCConnection(Properties prop, String url) throws SQLException {
		this.url = url;
		this.clientInfo = prop;
		this.hostName = this.clientInfo.getProperty("hostName");
		this.port = Integer.parseInt(this.clientInfo.getProperty("port"));
		setUser(Optional.ofNullable(this.clientInfo.getProperty("user")).orElse(""));

		initDBConnectionInternal(prop);

		try {
			connectInternal(this.hostName, this.port, this.clientInfo);
		} catch (IOException e) {
			e.printStackTrace();
			String msg = e.getMessage();
			if (msg.contains("Connection refused"))
				throw new SQLException(MessageFormat.format("{0}  ==> hostName = {1}, port = {2}", msg, this.hostName, this.port));
			else
				throw new SQLException(e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void initDBConnectionInternal(Properties clientInfo) {
		String sqlStdProp = this.clientInfo.getProperty("sqlStd");
		if (Objects.nonNull(sqlStdProp)) {
			SqlStdEnum sqlStd = SqlStdEnum.getByName(sqlStdProp);
			this.dbConnection = new DBConnection(sqlStd);
		} else {
			this.dbConnection = new DBConnection();
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
	 * @param appendInitScript
	 * @throws IOException
	 * @throws SQLException
	 */
	private void connect(String hostname, int port, Properties prop, String appendInitScript) throws IOException, SQLException {
		String userId = Optional.ofNullable(prop.getProperty("user")).orElse("");
		String password = Optional.ofNullable(prop.getProperty("password")).orElse("");
		String initialScript = Optional.ofNullable(prop.getProperty("initialScript"))
				.map(Utils::changeCase)
				.orElse("");

		if (initialScript.equals("select 1"))
			initialScript = "select 1 as val";

		if(appendInitScript != null) {
			if(!initialScript.isEmpty())
				initialScript = appendInitScript + "\n" + initialScript;
			else
				initialScript = appendInitScript;
		}

		String highAvailabilityStr = prop.getProperty("highAvailability");
		String enableHighAvailabilityStr = prop.getProperty("enableHighAvailability");
		boolean highAvailability;
		if (highAvailabilityStr == null) {
			highAvailability = Boolean.parseBoolean(enableHighAvailabilityStr);
		} else if (enableHighAvailabilityStr == null){
			highAvailability = Boolean.parseBoolean(highAvailabilityStr);
		} else {
			boolean param1 = Boolean.parseBoolean(highAvailabilityStr);
			boolean param2 = Boolean.parseBoolean(enableHighAvailabilityStr);
			if(param1 != param2)
				throw new SQLException("The values of the \"highAvailability\" and \"enableHighAvailability\" parameters in the URL must be the same if both are configured. ");
			highAvailability = param1;
		}

		String highAvailabilitySitesStr = prop.getProperty("highAvailabilitySites");
		String[] highAvailabilitySites = null;
		if (highAvailabilitySitesStr != null) {
			if (highAvailabilitySitesStr.contains(",")) {
				highAvailabilitySites = highAvailabilitySitesStr.split(",");
				highAvailabilitySites = Arrays.stream(highAvailabilitySites).map(String::trim).toArray(String[]::new);
			} else {
				highAvailabilitySites = highAvailabilitySitesStr.split(" ");
			}
		}

		String reconnectStr = prop.getProperty("reconnect");
		boolean reconnect = reconnectStr!=null ? Boolean.parseBoolean(reconnectStr) : false;

		String tableAliasStr = prop.getProperty("tableAlias");
		if (Utils.isNotEmpty(tableAliasStr)) {
			String tableAliasScript = Utils.parseTableAliasPropToScript(tableAliasStr);
			if (!initialScript.isEmpty())
				initialScript = initialScript + "\n" + tableAliasScript;
			else
				initialScript = tableAliasScript;
		}

		String enableLoadBalanceStr = prop.getProperty("enableLoadBalance");

		if (Objects.nonNull(enableLoadBalanceStr)) {
			boolean enableLoadBalance = Boolean.parseBoolean(enableLoadBalanceStr);
			success = dbConnection.connect(hostname, port, userId, password, initialScript, highAvailability, highAvailabilitySites, reconnect, enableLoadBalance);
		} else {
			success = dbConnection.connect(hostname, port, userId, password, initialScript, highAvailability, highAvailabilitySites, reconnect);
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

	private void connectInternal(String hostname, int port, Properties prop) throws SQLException, IOException{
		this.connect(hostname, port, prop,null);
		if (!this.success)
			throw new SQLException("Connection is failed");

		StringBuffer sbInitScript = buildInitialScript(prop);
		if(sbInitScript.length()>0)
			this.connect(hostname, port, prop, sbInitScript.toString());
	}

	private StringBuffer buildInitialScript(Properties prop) throws IOException {
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

		return sbInitScript;
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

		try {
			this.dbConnection.run("use CATALOG " + catalog + ";");
			this.catalog = catalog;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getCatalog() throws SQLException {
		return this.catalog;
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
		return this.user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	@Deprecated
	public String getPassword() {
		return password;
	}

	@Deprecated
	public void setPassword(String password) {
		this.password = password;
	}

	public String getDatabase() {
		return database;
	}

	protected DBConnection createNewConnection() throws IOException {
		DBConnection dbConnection = new DBConnection();
		dbConnection.connect(this.hostName, this.port, clientInfo.getProperty("user", ""), clientInfo.getProperty("password", ""));
		return dbConnection;
	}
}
