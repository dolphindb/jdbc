package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.text.MessageFormat;
import java.util.*;

public class JDBCPrepareStatement extends JDBCStatement implements PreparedStatement {

	private String tableName;
	private Entity tableNameArg;
	private String preSql;
	private String[] sqlSplit;
	private Object[] values;
	private int dml;
	private Object arguments;
	private List<Object> argumentsBatch; // String List<Entity> Vector
	private BasicTable tableDFS;
	private boolean isInsert;
	private String tableType;
	private HashMap<Integer, Integer> colType;
	private List<String> colNames;
	private List<String> colTypeString;
	private HashMap<String, ArrayList> unNameTable;
	private int count;
//	int countD = 0;
	
	public String getTableName() {
		return tableName;
	}

	public JDBCPrepareStatement(JDBCConnection connection, String sql) throws SQLException {
		
		super(connection);
		this.connection = connection;
		this.preSql = sql.trim();
		String[] strings = preSql.split(";");
		if (strings.length == 0) {
			throw new SQLException("SQL was empty");
		} else if (strings.length == 2) {
			throw new SQLException("check the SQL " + preSql);
		}
		this.preSql = strings[0];
		this.tableName = Utils.getTableName(sql);
		this.dml = Utils.getDml(sql);
		this.isInsert = this.dml == Utils.DML_INSERT;
		if (tableName != null) {
			tableName = tableName.trim();
			switch (this.dml) {
			case Utils.DML_SELECT:
			case Utils.DML_INSERT:
			case Utils.DML_DELETE: {
				if (tableName.length() > 0) {
					tableNameArg = new BasicString(tableName);
					if (tableTypes == null) {
						tableTypes = new LinkedHashMap<>();
					}
				} else {
					throw new SQLException("check the SQl " + preSql);
				}
			}
			}
		}
		this.preSql += ";";
		sqlSplit = this.preSql.split("\\?");
		values = new Object[sqlSplit.length + 1];
		batch = new StringBuilder();
		this.count = 0;
	}

	private void getTableType() {
		if (tableType == null) {
			try {
				tableType = connection.run("typestr " + tableName).getString();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (tableType != null) {
				if (tableTypes == null) {
					tableTypes = new LinkedHashMap<>();
				}
				tableTypes.put(tableName, tableType);
			}
		}
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return super.executeQuery(createSql());
	}

	@Override
	public int executeUpdate() throws SQLException {
		checkClosed();
		if (arguments == null) {
			try {
				arguments = createArguments();
			} catch (IOException e) {
				return 0;
			}
		}
		switch (dml) {
		case Utils.DML_INSERT:
			if (tableName != null) {
				getTableType();
				BasicInt basicInt;
//				System.out.println(tableType);//print------------------------------------
				if (tableType.equals(IN_MEMORY_TABLE)) {
					try {
						basicInt = (BasicInt) connection.run("tableInsert", (List<Entity>) arguments);
						return basicInt.getInt();
					} catch (IOException e) {
						throw new SQLException(e);
					}
				} else {
					return tableAppend();
				}
			} else {
				throw new SQLException("check the SQL " + preSql);
			}

		case Utils.DML_UPDATE:
		case Utils.DML_DELETE:
			if (tableName != null) {
				getTableType();
//				System.out.println(tableType);//print------------------------------------
				if (tableType.equals(IN_MEMORY_TABLE)) {
					try {
						return super.executeUpdate((String) arguments);
					} catch (SQLException e) {
						throw new SQLException(e);
					}
				} else {
//					System.out.println(tableName);
					throw new SQLException("only local in-memory table can update");
				}
			} else {
				throw new SQLException("check the SQL " + preSql);
			}
		case Utils.DML_SELECT:
			throw new SQLException("can not produces ResultSet");

		default:
			Entity entity;
			if (arguments instanceof String) {
				try {
					entity = connection.run((String) arguments);
				} catch (IOException e) {
					throw new SQLException(e);
				}
				if (entity instanceof BasicTable) {
					throw new SQLException("can not produces ResultSet");
				}
			}

			return 0;
		}
	}

	private int tableAppend() throws SQLException {

		if (unNameTable.size() > 1) {
				int insertRows = 0;
				List<Vector> cols = new ArrayList<>(unNameTable.size());
				
				
				for(int i = 0; i< colNames.size(); i++) {					
					if(colTypeString.get(i).equals("INT")) { 
						List<Integer> col = unNameTable.get(colNames.get(i));
						cols.add(new BasicIntVector(col));
					}
					if(colTypeString.get(i).equals("DOUBLE")) {
						List<Double> col = unNameTable.get(colNames.get(i));
						cols.add(new BasicDoubleVector(col));
					}
					if(colTypeString.get(i).equals("SYMBOL") || colTypeString.get(i).equals("STRING")) {
						List<String> col = unNameTable.get(colNames.get(i));
						cols.add(new BasicStringVector(col));
					}
					if(colTypeString.get(i).equals("DATE")) {
						List<BasicDate> col = unNameTable.get(colNames.get(i));
						BasicDateVector vdate = new BasicDateVector(col.size());
						for(int j = 0; j < col.size(); j++) {
							vdate.setInt(j,col.get(j).getInt());
						}
						cols.add(vdate);
					}if(colTypeString.get(i).equals("DATETIME")) {
						List<BasicDate> col = unNameTable.get(colNames.get(i));
						BasicDateTimeVector vdate = new BasicDateTimeVector(col.size());
						for(int j = 0; j < col.size(); j++) {
							vdate.setInt(j,col.get(j).getInt());
						}
						cols.add(vdate);
					}if(colTypeString.get(i).equals("MINUTE")) {
						List<BasicDate> col = unNameTable.get(colNames.get(i));
						BasicMinuteVector vdate = new BasicMinuteVector(col.size());
						for(int j = 0; j < col.size(); j++) {
							vdate.setInt(j,col.get(j).getInt());
						}
						cols.add(vdate);
					}if(colTypeString.get(i).equals("SECOND")) {
						List<BasicDate> col = unNameTable.get(colNames.get(i));
						BasicSecondVector vdate = new BasicSecondVector(col.size());
						for(int j = 0; j < col.size(); j++) {
							vdate.setInt(j,col.get(j).getInt());
						}
						cols.add(vdate);
					}if(colTypeString.get(i).equals("TIMESTAMP")) {
						List<Long> col = unNameTable.get(colNames.get(i));
						cols.add(new BasicTimestampVector(col));
					}if(colTypeString.get(i).equals("NANOTIME")) {
						List<Long> col = unNameTable.get(colNames.get(i));
						cols.add(new BasicNanoTimeVector(col));
					}if(colTypeString.get(i).equals("NANOTIMESTAMP")) {
						List<Long> col = unNameTable.get(colNames.get(i));
						cols.add(new BasicNanoTimestampVector(col));
					}if(colTypeString.get(i).equals("BOOL")) {
						List<Byte> col = unNameTable.get(colNames.get(i));
						cols.add(new BasicBooleanVector(col));
					}
					
					
														
				}		
				unNameTable = null;
				

				BasicTable insertTable = new BasicTable(colNames, cols);				
				Map<String, Entity> vars = new HashMap<String, Entity>();
				vars.put("t1", insertTable);
				
				try {
					connection.getDBConnection().upload(vars);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				
				
				try {
					connection.run("trade.append!(t1)");
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				cols = null;
				insertTable = null;
				vars = null;	
			
				return insertRows;
		}
		return 0;
	}

	@Override
	public void setNull(int parameterIndex, int x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal bigDecimal) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, bigDecimal);
	}

	@Override
	public void setString(int parameterIndex, String s) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, s);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] bytes) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, bytes);
	}

	@Override
	public void setDate(int parameterIndex, Date date) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, date);
	}

	@Override
	public void setTime(int parameterIndex, Time time) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, time);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp timestamp) throws SQLException {
		super.checkClosed();
		setObject(parameterIndex, timestamp);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
		Driver.unused();
	}

	@Override
	public void clearParameters() throws SQLException {
		super.clearBatch();
		if (values != null) {
			for (int i = 0, len = values.length; i < len; ++i) {
				values[i] = null;
			}
		}
	}

	@Override
	public void setObject(int parameterIndex, Object object) throws SQLException {
		super.checkClosed();
		if (parameterIndex > sqlSplit.length - 1) {
			throw new SQLException(
					MessageFormat.format("Parameter index out of range ({0} > number of parameters, which is {1}).",
							parameterIndex, sqlSplit.length - 1));
		}
		values[parameterIndex] = object;
	}

	@Override
	public void setObject(int parameterIndex, Object object, int targetSqlType) throws SQLException {
		setObject(parameterIndex, object);
	}

	@Override
	public void setObject(int parameterIndex, Object object, int targetSqlType, int scaleOrLength) throws SQLException {
		setObject(parameterIndex, object);
	}

	@Override
	public boolean execute() throws SQLException {
		super.checkClosed();
		checkClosed();
		switch (dml) {
		case Utils.DML_SELECT: {
			ResultSet resultSet_ = executeQuery(preSql);
			resultSets.offerLast(resultSet_);
			objectQueue.offer(executeQuery());
		}
			break;
		case Utils.DML_INSERT:
		case Utils.DML_UPDATE:
		case Utils.DML_DELETE: {
			objectQueue.offer(executeUpdate());
		}
			break;
		default: {
			Entity entity;
			String newSql;
			if (arguments instanceof String) {
				try {
					newSql = (String) arguments;
					entity = connection.run(newSql);
				} catch (IOException e) {
					throw new SQLException(e);
				}
				if (entity instanceof BasicTable) {
					ResultSet resultSet_ = new JDBCResultSet(connection, this, entity, newSql);
					resultSets.offerLast(resultSet_);
					objectQueue.offer(resultSet_);
				}
			}
		}
		}

		if (objectQueue.isEmpty()) {
			return false;
		} else {
			result = objectQueue.poll();
			if (result instanceof ResultSet) {
				return true;
			} else {
				return false;
			}
		}
	}

	@Override
	public void addBatch() throws SQLException {
		super.checkClosed();
		if (argumentsBatch == null) {
			argumentsBatch = new ArrayList<>();
		}
		try {
			arguments = createArguments();
		} catch (IOException e) {
			throw new SQLException(e);
		}
		if (arguments != null) {
			argumentsBatch.add(arguments);
		}
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		super.checkClosed();
		if (argumentsBatch == null) {
			argumentsBatch = new ArrayList<>();
		}
		argumentsBatch.add(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		super.clearBatch();
		if (argumentsBatch != null) {
			argumentsBatch.clear();
		}
	}

	@Override
	public void close() throws SQLException {
		super.close();
		sqlSplit = null;
		values = null;
	}

	@Override
	public int[] executeBatch() throws SQLException {
		super.checkClosed();	
		int[] arr_int = new int[argumentsBatch.size()];
		int index = 0;
		try {
			for (Object args : argumentsBatch) {
				if (args == null) {
					arr_int[index++] = 0;
				} else if (args instanceof String) {
					arr_int[index++] = super.executeUpdate((String) args);
				} else {
					arr_int[index++] = executeUpdate();
					return arr_int;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new BatchUpdateException(e.getMessage(), Arrays.copyOf(arr_int, index));
		}
		return arr_int;
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setRef(int parameterIndex, Ref ref) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setBlob(int parameterIndex, Blob blob) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setClob(int parameterIndex, Clob clob) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setArray(int parameterIndex, Array array) throws SQLException {
		Driver.unused();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		if (resultSet != null) {
			return resultSet.getMetaData();
		} else {
			return null;
		}
	}

	@Override
	public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException {
		setObject(parameterIndex, date);
	}

	@Override
	public void setTime(int parameterIndex, Time time, Calendar cal) throws SQLException {
		setObject(parameterIndex, time);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp timestamp, Calendar cal) throws SQLException {
		setObject(parameterIndex, timestamp);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setURL(int parameterIndex, URL url) throws SQLException {
		Driver.unused();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		Driver.unused();
		return null;
	}

	@Override
	public void setRowId(int parameterIndex, RowId rowId) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setNString(int parameterIndex, String s) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader reader, long l) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setNClob(int parameterIndex, NClob nClob) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long l) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long l) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long l) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML sqlxml) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream inputStream, long l) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream inputStream, long l) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long l) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream inputStream) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream inputStream) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		Driver.unused();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		Driver.unused();
	}

	private Object createArguments() throws IOException {
		if (isInsert) {
			if (colType == null) {
				BasicDictionary schema = (BasicDictionary) connection.run("schema(" + tableName + ")");
				BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
				BasicIntVector typeInt = (BasicIntVector) colDefs.getColumn("typeInt");
				int size = typeInt.rows();
				colType = new LinkedHashMap<>(size);
								
				for (int i = 0; i < size; i++) {
					colType.put(i + 1, typeInt.getInt(i));
				}
			}
			
			if(colNames == null) {
				BasicDictionary schema = (BasicDictionary) connection.run("schema(" + tableName + ")");
				BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
				BasicStringVector names = (BasicStringVector) colDefs.getColumn("name");
				int size = names.rows();
				colNames = new ArrayList<String>();
				for (int i = 0; i < size; i++) {
					colNames.add(names.getString(i).toString());
				}
				
			}
			
			if(colTypeString == null){
				BasicDictionary schema = (BasicDictionary) connection.run("schema(" + tableName + ")");
				BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
				BasicStringVector typeString = (BasicStringVector) colDefs.getColumn("typeString");
				int size = typeString.rows();
				colTypeString = new ArrayList<String>();
				for (int i = 0; i < size; i++) {
					colTypeString.add(typeString.getString(i).toString());
				}
				
			}
			
			
			
			List<Entity> arguments = new ArrayList<>(sqlSplit.length);
			arguments.add(tableNameArg);
			
			int j = 0;
			for (int i = 1; i < sqlSplit.length; ++i) {
				if (values[i] == null) {
					throw new IOException("No value specified for parameter " + i);
				}
				
				setColValue(colNames.get(j), colTypeString.get(j), colType.get(j),values[i]);				
				j++;
			}
			count++;
			
			return arguments;
		} else {
			try {
				return createSql();
			} catch (SQLException e) {
				throw new IOException(e.getMessage());
			}
		}
	}

	private void setColValue(String name, String typeString, Object type, Object value) throws IOException {
		
		ArrayList<Object> tmp = null;
		if(unNameTable == null) {
			unNameTable = new LinkedHashMap<>();
		}	
		tmp = addToCol(name, typeString, type, value );

		unNameTable.put(name, tmp);
				
		
	}

	private ArrayList<Object> addToCol(String name, String typeString, Object type, Object value) {

		
		ArrayList<Object> tmp = null;
		if(!unNameTable.containsKey(name)) {
			tmp = new ArrayList<>();
		}else{
			tmp = unNameTable.get(name);
		}
		
		if(typeString.equals("INT")) {
			tmp.add((int)value);
		}
		if(typeString.equals("DATE")) {
			tmp.add((BasicDate)value);
		}
		if(typeString.equals("SYMBOL")) {
			tmp.add(value.toString());
		}
		if(typeString.equals("DOUBLE")) {
			if (value.getClass() == Integer.class) {
			    tmp.add((double)((int)value));
			}else {
				tmp.add((double)value);
			}
		}if(typeString.equals("DATETIME")) {
			tmp.add((BasicDateTime)value);
		}if(typeString.equals("MINUTE")) {
			tmp.add((BasicMinute)value);
		}if(typeString.equals("SECOND")) {
			tmp.add((BasicMinute)value);
		}if(typeString.equals("TIMESTAMP")) {
			tmp.add((BasicTimestamp)value);
		}if(typeString.equals("NANOTIME")) {
			tmp.add((BasicNanoTime)value);
		}if(typeString.equals("NANOTIMESTAMP")) {
			tmp.add((BasicNanoTimestamp)value);
		}if(typeString.equals("BOOL")) {
			tmp.add((BasicBoolean)value);
		}
		
		
		
		return tmp;
	}



	private String createSql() throws SQLException {
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i < sqlSplit.length; ++i) {
			if (values[i] == null) {
				throw new SQLException("No value specified for parameter " + i);
			}
			String s = TypeCast.castDbString(values[i]);
			if (s == null)
				return null;
			sb.append(sqlSplit[i - 1]).append(s);
		}
		sb.append(sqlSplit[sqlSplit.length - 1]);
		return sb.toString();
	}
}
