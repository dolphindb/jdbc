/**
 * Hard code part controlHost in open method
 */
package com.dolphindb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.comm.SqlStdEnum;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.ProgressListener;
import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JDBCConnection implements Connection {
	//private DBConnection controlConnection;
	private DBConnection dbConnection;
	private String hostName;
	private int port;
	private boolean success;
	private String databases;
	private Vector tables;
	private String url;
	private DatabaseMetaData metaData;
	//private List<String> hostName_ports;
	//private boolean isDFS;
	//private StringBuilder sqlSb;
	//private  String controlHost;
	//private  int controlPort;
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
		//controlHost = null;
		//controlPort = -1;
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
	/*
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
	*/
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
	/*private boolean reachable(String hostname, int port, Properties prop) {
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
	}*/

	/*private void checklogin(String hostname, int port, Properties prop) {
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
		
	}*/

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
		if (StringUtils.isNotEmpty(tableAliasValue)) {
			String tableAliasScript = parseTableAliasPropToScript(tableAliasValue);
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

	public static String parseTableAliasPropToScript(String tableAliasValue) {
		// 示例：
		String eg =
				"dfs://db1/tb1," +
				"tb2:dfs://db1/tb2," +
				"tb3:dfs://db2/tb1," +
				"tb4:mvcc:///data/mvccfolder/tb1," +
				"tb5:mvcc://mvccfolder/tb2," +
				"tb6:memTb2";

		Set<String> aliasSet = new HashSet<>();
		StringBuilder stringBuilder = new StringBuilder();
		// stringBuilder.append("homeDir=getHomeDir();\n");

		try {
			String[] strs = tableAliasValue.split(",");
			for (String str : strs) {
				str = str.trim();
				// 按 ':' 分割，而不是按 '://' 分割
				String[] split = str.split("(?<!:)[:](?!/)");
				if (str.contains("dfs")) {
					// 1、dfs 表
					if (split.length == 1) {
						// 1）不含别名的 dfs://db1/tb1
						String finalStr;
						String[] pathSplit = str.split("(?<!/)/(?!/)");
						String alias = pathSplit[1];
						String dbPath = pathSplit[0];
						if (aliasSet.contains(alias)) {
							throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
						}
						aliasSet.add(alias);

						finalStr = alias + "=loadTable(\"" + dbPath + "\",\"" + alias + "\");\n";
						stringBuilder.append(finalStr);
					} else if (split.length == 2) {
						String finalStr;
						if (split[0].contains("dfs") && !split[1].contains("dfs")) {
							finalStr = parseOtherParh(str, str.split(":"), aliasSet);
						} else {
							// 2）含别名的 tb2:dfs://db1/tb2
							String alias = split[0].replaceAll(":", "");
							String path = split[1]; // dfs://db1/tb2
							if (aliasSet.contains(alias)) {
								throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
							}
							aliasSet.add(alias);
							if (StringUtils.isEmpty(path)) {
								throw new RuntimeException("");
							}

							String[] pathSplit = path.split("(?<!/)/(?!/)");
							String dbPath = pathSplit[0];
							String tbName = pathSplit[1];
							finalStr = alias + "=loadTable(\"" + dbPath + "\"," + "\"" + tbName + "\");\n";
						}

						stringBuilder.append(finalStr);
					}
				} else if (str.contains("mvcc")) {
					// 2、mvcc 表
					// "tb4:mvcc:///data/mvccfolder/tb1"
					// "tb5:mvcc://mvccfolder/tb2"
					if (str.startsWith("mvcc") && Pattern.matches(".*[a-zA-Z]:\\\\.*", str)) {
						// win 模式下的无别名
						String[] path = str.split("://");
						int lastDoubleSlashIndex = path[1].lastIndexOf("\\");
						String dropTableName = path[1].substring(0, lastDoubleSlashIndex - 1);
						String tbNameAndAlias = path[1].substring(lastDoubleSlashIndex + 1);
						if (aliasSet.contains(tbNameAndAlias)) {
							throw new RuntimeException("Duplicate table alias found in property tableAlias: " + tbNameAndAlias);
						}
						aliasSet.add(tbNameAndAlias);

						String finalStr = tbNameAndAlias + "=loadMvccTable(\"" + dropTableName + "\",\"" + tbNameAndAlias + "\");\n";
						stringBuilder.append(finalStr);
					} else if (split.length == 1) {
						// 无别名
						String finalStr;
						List<String> mvccPathSplit = parseMvccPath(split[0]); // split[0] mvcc:///data/mvccfolder/tb1
						// tb4=loadMvccTable(“/data/mvccfolder“,”tb1”)
						String mvccPath = mvccPathSplit.get(1);
						String[] pathSplit = mvccPath.split("/");// /data/mvccfolder/tb1
						String alias = pathSplit[pathSplit.length - 1];
						if (aliasSet.contains(alias)) {
							throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
						}
						aliasSet.add(alias);
						String mvccFilePath = mvccPath.substring(0, mvccPath.lastIndexOf("/"));
						if (mvccPath.startsWith("/")) {
							finalStr = alias + "=loadMvccTable(\"" + mvccFilePath + "\",\"" + alias + "\");\n";
						} else {
							finalStr = alias + "=loadMvccTable(" + "\"/" + mvccFilePath + "\",\"" + alias + "\");\n";
						}

						stringBuilder.append(finalStr);
					} else {
						String finalStr;
//						if (split[0].contains("mvcc") && ((split[1].contains("mvcc") && !split[1].matches("^mvcc$") || !split[1].contains("mvcc")))) {
//						if (split[0].contains("mvcc") && ((!split[1].contains("mvcc") || !split[1].matches("^mvcc$")))) {
						if (split[0].contains("mvcc") && ((!split[1].contains("mvcc") || !split[1].contains("mvcc:")))) {
							finalStr = parseOtherParh(str, str.split(":"), aliasSet);
						} else {
							// 有别名
							String alias= split[0]; // tb4
							if (str.contains("\\\\")) {
								// 如果是 'win \\' 写法
								String[] tempSplit = split[1].split("://"); // mvcc://C

								if (aliasSet.contains(alias)) {
									throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
								}
								aliasSet.add(alias);

								int lastDoubleSlashIndex = split[2].lastIndexOf("\\\\");
								String dropTableName = split[2].substring(0, lastDoubleSlashIndex);
								String tbName = split[2].substring(lastDoubleSlashIndex + 2);

								finalStr = alias + "=loadMvccTable(\"" + tempSplit[1] + ":" + dropTableName + "\",\"" + tbName + "\");\n";
							} else {
								List<String> mvccPathSplit = parseMvccPath(split[1]); // split[1] mvcc:///data/mvccfolder/tb1
								String mvccPath = mvccPathSplit.get(1);
								if (aliasSet.contains(alias)) {
									throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
								}
								aliasSet.add(alias);
								// tb4=loadMvccTable(“/data/mvccfolder“,”tb1”)
								String[] pathSplit = mvccPath.split("/");// /data/mvccfolder/tb1
								String mvccFilePath = mvccPath.substring(0, mvccPath.lastIndexOf("/"));
								if (mvccPath.startsWith("/")) {
									finalStr = alias + "=loadMvccTable(\"" + mvccFilePath + "\",\"" + pathSplit[pathSplit.length - 1] + "\");\n";
								} else {
									finalStr = alias + "=loadMvccTable(" + "\"" + mvccFilePath + "\",\"" + pathSplit[pathSplit.length - 1] + "\");\n";
//									if (Pattern.matches("^[A-Za-z]:.*$", mvccFilePath)) {
//										finalStr = alias + "=loadMvccTable(" + "\"" + mvccFilePath + "\",\"" + pathSplit[pathSplit.length - 1] + "\");\n";
//									} else {
//										finalStr = alias + "=loadMvccTable(" + "\"" + mvccFilePath + "\",\"" + pathSplit[pathSplit.length - 1] + "\");\n";
//									}
								}
							}
						}

						stringBuilder.append(finalStr);
					}
				} else {
					// 3、other
					String finalStr = parseOtherParh(str, str.split(":"), aliasSet);
//					split = str.split(":");
//					String alias = split[0];
//					String memTableName = split[1];
//					if (aliasSet.contains(alias)) {
//						throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
//					}
//					aliasSet.add(alias);
//					String finalStr = alias + "=" + memTableName + ";\n";
					stringBuilder.append(finalStr);
				}
			}
		} catch (RuntimeException e) {
			// e.printStackTrace();
			throw e;
		} catch (Exception e) {
			throw new RuntimeException("parse tableAlias error!");
		}

		return stringBuilder.toString();
	}

	private static String parseOtherParh(String str, String[] split, Set<String> aliasSet) {
		// 3、内存表
		split = str.split(":");
		String alias = split[0];
		String memTableName = split[1];
		if (aliasSet.contains(alias)) {
			throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
		}
		aliasSet.add(alias);
		String finalStr = alias + "=" + memTableName + ";\n";

		return finalStr;
	}

	public static List<String> parseMvccPath(String str) {
		Pattern pattern = Pattern.compile("^(.*?://)(.*)");
		Matcher matcher = pattern.matcher(str);
		List<String> result = new ArrayList<>();

		if (matcher.find()) {
			result.add(matcher.group(1));
			result.add(matcher.group(2));
		}

		return result;
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
			StringBuilder sb = (new StringBuilder("system_db")).append(" = database(\"").append(valueName[0]).append("\");\n");
			this.dbConnection.run(sb.toString());
			if (valueName[0].trim().startsWith("dfs://")) {
				//this.isDFS = true;
				this.databases = valueName[0];
				List<String> dbtables=new ArrayList<>();
				// if set specific tableanme to load
				if (StringUtils.isNotEmpty(prop.getProperty("tableName"))) {
					String tablename = prop.getProperty("tableName");
					tablename = tablename.trim();
					String[] tableNames = tablename.split(",");
					for (int i = 0; i < tableNames.length; i++) {
						if(tableNames[i].isEmpty()==false)
							dbtables.add(tableNames[i]);
					}
					String script=loadTables(this.databases,dbtables,false);
					sbInitScript.append(script);
				} else {
					// if not specific tableanme, load all tables; but need to authenticate every table.
					Vector vector = (Vector) this.dbConnection.run("getTables(system_db)");
					for (int i = 0; i < vector.rows(); i++) {
						dbtables.add(vector.getString(i));
					}
					String script=loadTables(this.databases,dbtables,true);
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
		//if (!isDFS) {
		return this.dbConnection.run(function, arguments);
		/*}

		//int size = hostName_ports.size();
		Entity entity = null;
		entity = this.dbConnection.run(function, arguments);
		return entity;
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
		}*/
	}

	// Automatic switching node
	public Entity run(String script) throws IOException {
		//if (!isDFS) {
		return this.dbConnection.run(script);
		/*}
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
		}*/
	}

	public Entity run(String script, int fetchSize) throws IOException {
		//if (!isDFS) {
		return this.dbConnection.run(script, (ProgressListener) null, 4, 2, fetchSize);
		/*}
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
		}*/
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
