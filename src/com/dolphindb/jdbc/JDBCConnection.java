/**
 * Hard code part controlHost in open method
 */
package com.dolphindb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.io.ProgressListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.sql.*;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;

public class JDBCConnection implements Connection {
	private DBConnection controlConnection;
	private DBConnection dbConnection;
	private String hostName;
	private int port;
	private boolean success;
	private String databases;
	private Vector tables;
	private String url;
	private DatabaseMetaData metaData;
	private List<String> hostName_ports;
	private boolean isDFS;
	private StringBuilder sqlSb;
	private  String controlHost;
	private  int controlPort;
	private String user;
	private String password;

	public JDBCConnection(String url, Properties prop) throws SQLException {
		this.url = url;
		dbConnection = new DBConnection();
		hostName = prop.getProperty("hostName");
		port = Integer.parseInt(prop.getProperty("port"));
		controlHost = null;
		controlPort = -1;
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

		}
	}
	
	public DBConnection getDBConnection() {
		return dbConnection;
	}
	
	public void setDBConnection(DBConnection dbConnection) {
		this.dbConnection = dbConnection;
	}

	/**
	 * Connect to other node
	 * 
	 * @param hostname
	 * @param FuncationPort
	 * @param prop:
	 *            get controllerNode from prop, if prop does not contain
	 *            controllerNode key, then the default controllerNode is 8920
	 * @throws IOException
	 * @throws SQLException
	 */
	private boolean tryOtherNode(String hostname, int FuncationPort, Properties prop) throws IOException, SQLException {
		controlConnection = new DBConnection();
	    if(controlHost != null && controlPort > 0){
	            controlConnection.connect(controlHost,controlPort);
	            BasicTable table = (BasicTable) controlConnection.run("getClusterChunkNodesStatus()");
	            Vector siteVector = table.getColumn("site");

	            LinkedList<String> other_ports = new LinkedList<>();
	    		for (int i = 0, len = siteVector.rows(); i < len; i++) {
	    			other_ports.add(siteVector.get(i).getString());
	    		}

	    		// try to connect node, which does not contain the broken one.
	    		int size = other_ports.size();
	    		for (int index = 0; index < size; ++index) {
	    			String[] hostName_port = other_ports.get(index).split(":");
	    			if (!hostName_port[1].equals(String.valueOf(FuncationPort))) {
	    				System.out.println("connecting " + hostname + ":" + hostName_port[1]);
	    				if (!reachable(hostname, Integer.parseInt(hostName_port[1]), prop)) {
	    					System.out.println("Cannot connect " + hostname + ":" + hostName_port[1]);
	    					continue;
	    				}
	    				checklogin(hostname, Integer.parseInt(hostName_port[1]),prop);
	    				port = Integer.parseInt(hostName_port[1]);
	    				break;
	    			}
	    		}
	        }
	    return false;
	}

	/**
	 * Whether the node is reachable
	 * 
	 * @param hostname
	 * @param port
	 * @param prop
	 *            get waitingTime from prop, if prop does not contain waitingTime
	 *            key, then the default controllerNode is 3
	 * @return
	 */
	private boolean reachable(String hostname, int port, Properties prop) {
		Socket s = new Socket();
		SocketAddress add = new InetSocketAddress(hostname, port);
		int waitingTime = 3;
		if (prop.containsKey("waitingTime")) {
			waitingTime = Integer.parseInt(prop.getProperty("waitingTime"));
		}
		try {
			s.connect(add, waitingTime * 1000);
		} catch (IOException e) {
			System.out.println("cannot reach" + hostname + ":" + port);
			return false;
		} finally {
			try {
				s.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	private void checklogin(String hostname, int port, Properties prop) {
		try {
			if(prop.containsKey("user") && prop.containsKey("password")) {
				success = dbConnection.connect(hostname, port,prop.getProperty("user"),prop.getProperty("password"));	
				setUser(prop.getProperty("user"));
				setPassword(prop.getProperty("password"));
			}else {
				success = dbConnection.connect(hostname, port);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
	private void connect(String hostname, int port, Properties prop) throws IOException {
		String userId = prop.getProperty("user");
		String password = prop.getProperty("password");
		String initialScript = prop.getProperty("initialScript");
		initialScript = Utils.changeCase(initialScript);
		if (initialScript!=null&&initialScript.equals("select 1"))
			initialScript = "select 1 as val";
		Boolean highAvailability = Boolean.valueOf(prop.getProperty("highAvailability"));
		String rowHighAvailabilitySites = prop.getProperty("highAvailabilitySites");
		String[] highAvailabilitySites = null;
		if (rowHighAvailabilitySites != null) {
			highAvailabilitySites = rowHighAvailabilitySites.split(" ");
		}
		if(userId != null && password != null){
			if (initialScript != null && highAvailability && highAvailabilitySites != null){
				success = dbConnection.connect(hostname, port, userId, password, initialScript, highAvailability, highAvailabilitySites);
			}else {
				success = dbConnection.connect(hostname, port, userId, password);
			}
		}else if(initialScript != null && highAvailabilitySites != null){
			success = dbConnection.connect(hostname, port, initialScript, highAvailabilitySites);
		}else {
			success = dbConnection.connect(hostName, port);
		}
	}

	private void open(String hostname, int port, Properties prop) throws SQLException, IOException{
		this.connect(hostname, port, prop);
		if (!this.success) {
			throw new SQLException("Connection is fail");
		} else {
			String[] key = new String[]{"databasePath"};
			String[] value = new String[6];
			String[] valueName = Utils.getProperties(prop, key);
			String hasScripts = prop.getProperty("length");
			if (valueName[0] != null && valueName[0].length() > 0) {
				valueName[0] = "\"" + valueName[0] + "\"";
				value[0] = valueName[0];
				Entity dbInfo = this.dbConnection.run("database(" + value[0] + ").schema()");
				Entity partitionTypeName = ((BasicDictionary) dbInfo).get(new BasicString("partitionTypeName"));
				value[1] = partitionTypeName.getString();
				Entity partitionScheme = ((BasicDictionary) dbInfo).get(new BasicString("partitionSchema"));
				value[2] = partitionScheme.getString();
				value[3] = " ";
				Entity engineType = ((BasicDictionary) dbInfo).get(new BasicString("engineType"));
				value[4] = "\"" + engineType.getString()  + "\"";
				Entity atomic = ((BasicDictionary) dbInfo).get(new BasicString("atomic"));
				value[5] = "\"" + atomic.getString()  + "\"";
				StringBuilder sb = (new StringBuilder("system_db")).append(" = database(");
				Utils.joinOrder(sb, value, ",");
				sb.append(");\n");
				this.sqlSb = new StringBuilder();
				this.sqlSb.append(sb);
				this.dbConnection.run(sb.toString());
				if (value[0].trim().startsWith("\"dfs://")) {
					this.isDFS = true;
					this.databases = value[0];
					this.tables = (Vector)this.dbConnection.run("getTables(system_db)");
					StringBuilder loadTableSb = new StringBuilder();
					int i = 0;

					for(int len = this.tables.rows(); i < len; ++i) {
						String name = this.tables.get(i).getString();
						loadTableSb.append(name).append(" = ").append("loadTable(").append("system_db").append(",`").append(name).append(");\n");
					}

					this.sqlSb.append(loadTableSb);
					String sql = loadTableSb.toString();
					this.dbConnection.run(sql);
				}

				String controllerAlias = this.dbConnection.run("getControllerAlias()").getString();
				if (controllerAlias != null && controllerAlias.length() > 0) {
					this.isDFS = true;
					this.controlHost = this.dbConnection.run("rpc(\"" + controllerAlias + "\", getNodeHost)").getString();
					this.controlPort = ((BasicInt)this.dbConnection.run("rpc(\"" + controllerAlias + "\", getNodePort)")).getInt();
					this.controlConnection = new DBConnection();
					this.controlConnection.connect(this.controlHost, this.controlPort);
					BasicTable table = (BasicTable)this.controlConnection.run("getClusterChunkNodesStatus()");
					Vector siteVector = table.getColumn("site");
					this.hostName_ports = new LinkedList();
					int i = 0;

					for(int len = siteVector.rows(); i < len; ++i) {
						this.hostName_ports.add(siteVector.get(i).getString());
					}
				} else {
					this.isDFS = false;
				}
			}

			if (hasScripts != null) {
				int length = Integer.parseInt(prop.getProperty("length"));
				if (length > 0) {
					for(int i = 0; i < length; ++i) {
						this.dbConnection.run(prop.getProperty("script" + i));
					}
				}
			}

		}
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
		Driver.unused("commit not implemented");
	}

	@Override
	public void rollback() throws SQLException {
		Driver.unused("rollback not implemented");
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
		Driver.unused("setCatalog not implemented");
	}

	@Override
	public String getCatalog() throws SQLException {
		StringBuilder sb = new StringBuilder();
		if (databases != null){
			return databases;
		}else {
			try {
				BasicStringVector dbs = (BasicStringVector) dbConnection.run("getClusterDFSDatabases()");
				for (int i = 0; i < dbs.rows(); i++){
					sb.append(dbs.getString(i) + "\n");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return sb.toString();
		}
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		return;
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
		return;
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
		return;
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		return;
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
		return (String) clientInfo.getProperty(name);
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
		if (!isDFS) {
			return this.dbConnection.run(function, arguments);
		}

		int size = hostName_ports.size();
		Entity entity = null;
		try {
			entity = this.dbConnection.run(function, arguments);
			return entity;
		} catch (IOException e) {
			for (int index = 0; index < size; ++index) {
				String[] hostName_port = hostName_ports.get(index).split(":");
				if (hostName_port[0] == hostName && Integer.parseInt(hostName_port[1]) == port ){
					continue;
				}
				this.dbConnection = new DBConnection();
				try {
					boolean succeeded;
					if(getUser()!=null && getPassword()!=null){
						succeeded = this.dbConnection.connect(hostName_port[0], Integer.parseInt(hostName_port[1]), getUser(), getPassword());
					}
					else{
						succeeded = this.dbConnection.connect(hostName_port[0], Integer.parseInt(hostName_port[1]));
					}
					if (succeeded) {
						this.dbConnection.run(sqlSb.toString());
						entity = this.dbConnection.run(function, arguments);
						return entity;
					}
				} catch (IOException e1) {
					return entity;
				}
				
//				this.dbConnection.close();
//				this.dbConnection = new DBConnection();
//				try {
//					if (this.dbConnection.connect(hostName_port[0], Integer.parseInt(hostName_port[1]))) {
//						System.out.println("Connect " + this.dbConnection.getHostName() + ":" + this.dbConnection.getPort());
//						this.dbConnection.run(sqlSb.toString());
//						entity = this.dbConnection.run(function, arguments);
//						return entity;
//					}
//				} catch (IOException e1) {
//					message = e1.getMessage();
//				}
			}
			throw new IOException("All dataNodes were dead");
		}
	}

	// Automatic switching node
	public Entity run(String script) throws IOException {
		if (!isDFS) {
			return this.dbConnection.run(script);
		}
		script = script.trim();
		Matcher matcher = Utils.ASSIGN_PATTERN.matcher(script);
		if (matcher.find()) {
			sqlSb.append(script).append(";\n");
		}

		int size = hostName_ports.size();
		Entity entity = null;
		try {
			entity = this.dbConnection.run(script);
			return entity;
		} catch (IOException e) {
			for (int index = 0; index < size; ++index) {
				String[] hostName_port = hostName_ports.get(index).split(":");
				if (hostName_port[0] == hostName && Integer.parseInt(hostName_port[1]) == port ){
					continue;
				}
				this.dbConnection = new DBConnection();
				try {
					boolean succeeded;
					if(getUser()!=null && getPassword()!=null){
						succeeded = this.dbConnection.connect(hostName_port[0], Integer.parseInt(hostName_port[1]), getUser(), getPassword());
					}
					else{
						succeeded = this.dbConnection.connect(hostName_port[0], Integer.parseInt(hostName_port[1]));
					}
					if (succeeded) {
						this.dbConnection.run(sqlSb.toString());
						entity = this.dbConnection.run(script);
						return entity;
					}
				} catch (IOException e1) {
					return entity;
				}
			}		
			throw new IOException("All dataNodes were dead");
		}
	}

	public Entity run(String script, int fetchSize) throws IOException {
		if (!isDFS) {
			return this.dbConnection.run(script, (ProgressListener) null, 4, 2, fetchSize);
		}
		script = script.trim();
		Matcher matcher = Utils.ASSIGN_PATTERN.matcher(script);
		if (matcher.find()) {
			sqlSb.append(script).append(";\n");
		}

		int size = hostName_ports.size();
		Entity entity = null;
		try {
			entity = this.dbConnection.run(script);
			return entity;
		} catch (IOException e) {
			for (int index = 0; index < size; ++index) {
				String[] hostName_port = hostName_ports.get(index).split(":");
				if (hostName_port[0] == hostName && Integer.parseInt(hostName_port[1]) == port ){
					continue;
				}
				this.dbConnection = new DBConnection();
				try {
					boolean succeeded;
					if(getUser()!=null && getPassword()!=null){
						succeeded = this.dbConnection.connect(hostName_port[0], Integer.parseInt(hostName_port[1]), getUser(), getPassword());
					}
					else{
						succeeded = this.dbConnection.connect(hostName_port[0], Integer.parseInt(hostName_port[1]));
					}
					if (succeeded) {
						this.dbConnection.run(sqlSb.toString());
						entity = this.dbConnection.run(script, (ProgressListener) null, 4, 2, fetchSize);
						return entity;
					}
				} catch (IOException e1) {
					return entity;
				}
			}
			throw new IOException("All dataNodes were dead");
		}
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
		return databases;
	}
}
