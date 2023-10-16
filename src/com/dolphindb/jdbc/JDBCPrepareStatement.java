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
import java.util.*;

public class JDBCPrepareStatement extends JDBCStatement implements PreparedStatement {
	private String sql;
	private final String tableName;
	private final int sqlDmlType;
	private List<ColumnBindValue> columnBindValues;

	private Map<Integer, Integer> indexSQLToDDB;
	private BindValue[] bufferArea;
	String tableTypeCache;
	private InsertType insertSqlType;
	private int batchSize;
	List<String> sqlBuffer;

	public JDBCPrepareStatement(JDBCConnection conn,String sql) throws SQLException {
		super(conn);
		this.batchSize = 0;
		this.connection = conn;
		this.sql = processSql(sql);
		String[] sqlSplit = sql.split(";");
		this.sql = sqlSplit[sqlSplit.length - 1].trim();
		this.tableName = Utils.getTableName(sql);
		this.sqlDmlType = Utils.getDml(sql);
		this.sqlBuffer = new ArrayList<>();
		this.indexSQLToDDB = new HashMap<>();
		if (this.sqlDmlType == Utils.DML_INSERT) {
			this.insertSqlType = Utils.getInsertSqlType(sql);
			initColumnBindValues(this.tableName);
			Utils.checkSQLValid(sql, this.tableName);

			Map<String, Integer> columnParamInSql = Utils.getColumnParamInSql(sql, this.tableName);
			for(ColumnBindValue value : columnBindValues){
				String colName = value.getColName();
				if(columnParamInSql.containsKey(colName)){
					indexSQLToDDB.put(columnParamInSql.get(colName), value.getIndex());
					columnParamInSql.remove(colName);
				}
			}
			if(columnParamInSql.size() != 0){
				for (String key : columnParamInSql.keySet())
					throw new SQLException("the column name " + key + "does not exist in table. ");
			}

			this.bufferArea = new BindValue[this.columnBindValues.size()];
		} else {
			int size = 0;
			for (int i = 0; i < sql.length(); i++) {
				char ch = sql.charAt(i);
				if(ch == '?')// TODO: 字符串里的问号会有问题
					size++;
			}

			bufferArea = new BindValue[size];
		}
	}

	private void initColumnBindValues(String tableName) throws SQLException {
		if (this.columnBindValues == null) {
			try {
				this.columnBindValues = new ArrayList<>();
				BasicDictionary schema = (BasicDictionary) connection.run(String.format("schema(%s)", tableName));
				BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
				BasicStringVector names = (BasicStringVector) colDefs.getColumn("name");
				BasicIntVector colDefsTypeInt = (BasicIntVector) colDefs.getColumn("typeInt");
				BasicIntVector extraInt = (BasicIntVector) colDefs.getColumn("extra");
				for (int i = 0; i < names.rows(); i++) {
					String colName = names.getString(i);
					int typeInt = colDefsTypeInt.getInt(i);
					Entity.DATA_TYPE type = Entity.DATA_TYPE.values()[typeInt];
					int extra = extraInt.getInt(i);
					ColumnBindValue columnBindValue = new ColumnBindValue(i, colName, type, extra);
					columnBindValues.add(columnBindValue);
				}
			} catch (IOException e) {
				throw new SQLException(e);
			}
		}
	}

	private void bind(int paramIndex, Object obj, int scaleOrLength) throws SQLException {
		if (this.sqlDmlType == Utils.DML_INSERT) {
			int index = getDataIndexBySQLIndex(paramIndex);
			if(index >= this.columnBindValues.size())
				throw new SQLException("the index of columnBindValues is out of range");
			Vector column = this.columnBindValues.get(index).getBindValues();
			try {
				column.Append((Scalar) BasicEntityFactory.createScalar(column.getDataType(), obj, this.columnBindValues.get(index).getScale()));
			}catch (Exception e){
				throw new SQLException(e);
			}
		} else
			bufferArea[paramIndex - 1] = new BindValue(obj, false);
	}

	@Override
	public void clearBatch() throws SQLException {
		super.clearBatch();
		batchSize = 0;
		sqlBuffer.clear();
		clearParameters();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		// todo @zouminxing 我们现在是不是还不支持 batchsize，我看默认设置为 0.
		if(this.batchSize == 0)
			return new int[0];
		switch (this.sqlDmlType){
			case Utils.DML_INSERT:
				return tableAppend();
		}
		int[] executeRes = new int[this.batchSize];
		try {
			for (int i = 0; i < this.batchSize; i++){
				switch (this.sqlDmlType){
					case Utils.DML_UPDATE:
					case Utils.DML_DELETE:
						if(tableName != null)
							executeRes[i] = super.executeUpdate(sqlBuffer.get(i));
						else
							throw new SQLException("check the SQL " + sqlBuffer.get(i));
						break;
					case Utils.DML_SELECT:
					case Utils.DML_EXEC:
						throw new SQLException("can not produces ResultSet");
					default:
						Entity entity = connection.run(sqlBuffer.get(i));
						if (entity instanceof BasicTable)
							throw new SQLException("can not produces ResultSet");
						executeRes[i] = 0;
						sqlBuffer.clear();
				}
			}
		} catch (Exception e){
			throw new SQLException(e);
		} finally {
			sqlBuffer.clear();
		}
		return executeRes;
	}

	private void bind(int paramIndex, Object obj) throws SQLException {
		bind(paramIndex, obj, 0);
	}

	private int getDataIndexBySQLIndex(int paramIndex) throws SQLException {
		int index = paramIndex - 1;
		if (indexSQLToDDB.size() != 0) {
			if(!indexSQLToDDB.containsKey(index))
				throw new SQLException("paramIndex is out of range");
			index = indexSQLToDDB.get(index);
		}
		return index;
	}

	private void bindNull(int paramIndex, Object obj) throws SQLException {
		if (this.sqlDmlType == Utils.DML_INSERT) {
			int index = getDataIndexBySQLIndex(paramIndex);
			Vector column = this.columnBindValues.get(index).getBindValues();
			try {
				column.Append((Scalar) BasicEntityFactory.createScalar(column.getDataType(), obj, this.columnBindValues.get(index).getScale()));
			}catch (Exception e){
				throw new SQLException(e);
			}
		} else
			bufferArea[paramIndex - 1] = new BindValue(obj,false);
	}

	private void flushBufferArea(int rows) throws SQLException { //todo:rename
		if (sqlDmlType == Utils.DML_INSERT) {
			for(ColumnBindValue column : columnBindValues){
				if(column.getBindValues().rows() != rows) {
					Vector columnCol = column.getBindValues();
					try {
						columnCol.Append((Scalar) BasicEntityFactory.createScalar(columnCol.getDataType(), null, column.getScale()));
					}catch (Exception e){
						throw new SQLException(e);
					}
				}
			}
		} else{
			sqlBuffer.add(generateSQL());
		}

		clearParameters();
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		flushBufferArea(1);
		//noinspection SqlSourceToSinkFlow
		return super.executeQuery(sqlBuffer.get(0));
	}

	private void checkBindsLegal() throws SQLException {
		if(indexSQLToDDB.size() == 0) {
			for (ColumnBindValue columnBindValue : columnBindValues) {
				if(columnBindValue.getBindValues().rows() != batchSize)
					throw new SQLException("The column " + columnBindValue.getColName() + " is not set.");
			}
		}else{
			for (Integer index : indexSQLToDDB.keySet()){
				if(this.columnBindValues.get(indexSQLToDDB.get(index)).getBindValues().rows() != batchSize)
					throw new SQLException("The column " + this.columnBindValues.get(indexSQLToDDB.get(index)).getColName() + " is not set.");
			}
		}
	}

	@Override
	public int executeUpdate() throws SQLException {
		try {
			flushBufferArea(1);
			switch (this.sqlDmlType) {
				case Utils.DML_INSERT:
					 tableAppend();
					 return 0; // TODO: executeUpdate result
				case Utils.DML_UPDATE:
				case Utils.DML_DELETE:
					if (tableName != null) {
						String sql = sqlBuffer.get(0);
						sqlBuffer.clear();
						connection.run(sql);
						return 0; // TODO: executeUpdate result
					} else
						throw new SQLException("check the SQL " + sql);
				case Utils.DML_SELECT:
				case Utils.DML_EXEC:
					throw new SQLException("can not produces ResultSet");
				default:
					Entity entity = connection.run(sqlBuffer.get(0));
					sqlBuffer.clear();
					if (entity instanceof BasicTable)
						throw new SQLException("can not produces ResultSet");
					return 0;
			}
		} catch (Exception e ) {
			throw new SQLException(e);
		}
	}

	private int[] tableAppend() throws SQLException {
		List<Vector> arguments = createDFSArguments();
		List<String> colNames = new ArrayList<>();
		columnBindValues.forEach(e -> colNames.add(e.getColName()));
		BasicTable basicTable = new BasicTable(colNames, arguments);
		List<Entity> param = new ArrayList<>();
		param.add(basicTable);
		try {
			int size = ((Scalar)connection.run("tableInsert{" + tableName + "}", param)).getNumber().intValue();
			int[] value = new int[size];
			if(arguments.get(0).rows() != size){
				for(int i = 0; i < size; ++i){
					value[i] = EXECUTE_FAILED;
				}
			}else{
				for(int i = 0; i < size; ++i){
					value[i] = SUCCESS_NO_INFO;
				}
			}
			return value;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

//	private List<Entity> createMemArguments(int idx) throws Exception {
//		List<Entity> arguments = new ArrayList<>();
//		arguments.add(new BasicString(tableName));
//		for (ColumnBindValue columnBindValue : columnBindValues)
//			arguments.add(createColVectorFromBindValue(columnBindValue,idx));
//
//		return arguments;
//	}

	private List<Vector> createDFSArguments() {
		List<Vector> arguments = new ArrayList<>();
		for (ColumnBindValue columnBindValue : columnBindValues)
			arguments.add(columnBindValue.getBindValues());
		return arguments;
	}

//	private Vector createColVectorFromBindValue(ColumnBindValue columnBindValue, int idx) throws Exception {
//		Vector col = BasicEntityFactory.instance().createVectorWithDefaultValue(columnBindValue.getType(), 0, columnBindValue.getScale());
//		if (columnBindValue.getType().equals(Entity.DATA_TYPE.DT_DECIMAL32) && (columnBindValue.getScale() < 0 || columnBindValue.getScale() > 9)) {
//			throw new IllegalArgumentException("The size of the Decimal32 type should be in the range 0-9");
//		} else if (columnBindValue.getType().equals(Entity.DATA_TYPE.DT_DECIMAL64) && (columnBindValue.getScale() < 0 || columnBindValue.getScale() > 18)) {
//			throw new IllegalArgumentException("The size of the Decimal64 type should be in the range 0-18");
//		} else if (columnBindValue.getType().equals(Entity.DATA_TYPE.DT_DECIMAL128) && (columnBindValue.getScale() < 0 || columnBindValue.getScale() > 38)) {
//			throw new IllegalArgumentException("The size of the Decimal128 type should be in the range 0-38");
//		}
//
//		BindValue bindValue = columnBindValue.getBindValues().get(idx);
//		// todo: decimal64 has a bug, see JAVAOS-184.
//		col.Append((Scalar) BasicEntityFactory.createScalar(columnBindValue.getType(), bindValue.getValue(), columnBindValue.getScale()));
//		return col;
//	}

	private String getTableType() throws IOException {
		if (tableTypeCache != null)
			return tableTypeCache;

		tableTypeCache = connection.run("typestr " + tableName).getString();
		return tableTypeCache;
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		if (this.sqlDmlType == Utils.DML_INSERT) {
			int index = getDataIndexBySQLIndex(parameterIndex);
			bindNull(parameterIndex, TypeCast.nullScalar(columnBindValues.get(index).getType()));
		}else{
			bufferArea[parameterIndex - 1] = new BindValue("", true);
		}
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		bind(parameterIndex, x.doubleValue());
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		Driver.unused("setAsciiStream not implemented");
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		Driver.unused("setUnicodeStream not implemented");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		Driver.unused("setBinaryStream not implemented");
	}

	@Override
	public void clearParameters() throws SQLException {
		Arrays.fill(bufferArea, null);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public boolean execute() throws SQLException {
		try {
			switch (sqlDmlType){
				case Utils.DML_SELECT:
				case Utils.DML_EXEC: {
					ResultSet resultSet_ = executeQuery();
					resultSets.offerLast(resultSet_);
					objectQueue.offer(resultSet_);
					break;
				}
				case Utils.DML_INSERT:
				case Utils.DML_UPDATE:
				case Utils.DML_DELETE:
					objectQueue.offer(executeUpdate());
					break;
				default: {
					flushBufferArea(1);
					Entity entity = connection.run(sqlBuffer.get(0));
					if (entity instanceof BasicTable) {
						ResultSet resultSet_ = new JDBCResultSet(connection, this, entity, sqlBuffer.get(0), this.getMaxRows());
						resultSets.offerLast(resultSet_);
						objectQueue.offer(resultSet_);
					}
					sqlBuffer.clear();
				}
			}
		} catch (Exception e){
			throw new SQLException(e);
		}

		if (objectQueue.isEmpty())
			return false;
		else {
			result = objectQueue.poll();
			return result instanceof ResultSet;
		}
	}

	@Override
	public void addBatch() throws SQLException {
		batchSize++;
		flushBufferArea(batchSize);
		checkBindsLegal();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		Driver.unused("setCharacterStream not implemented");
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		Driver.unused("setRef not implemented");
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		byte[] blobbyte = x.getBytes(1,(int)x.length());
		String blobstring = new String( blobbyte);
		bind(parameterIndex,blobstring);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		Driver.unused("setClob not implemented");
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		Driver.unused("setArray not implemented");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if(Objects.nonNull(this.resultSet))
			return resultSet.getMetaData();
		else
			return null;
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		Driver.unused("setNull not implemented");
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		Driver.unused("setURL not implemented");
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		Driver.unused("getParameterMetaData not implemented");
		return null;
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		Driver.unused("setRowId not implemented");
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		bind(parameterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		Driver.unused("setNCharacterStream not implemented");
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		Driver.unused("setNClob not implemented");
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		Driver.unused("setClob not implemented");
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		Driver.unused("setBlob not implemented");
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		Driver.unused("setSQLXML not implemented");
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		bind(parameterIndex, x, scaleOrLength);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		Driver.unused("setAsciiStream not implemented");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		Driver.unused("setBinaryStream not implemented");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		Driver.unused("setCharacterStream not implemented");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		Driver.unused("setAsciiStream not implemented");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		Driver.unused("setBinaryStream not implemented");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		Driver.unused("setCharacterStream not implemented");
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		Driver.unused("setNCharacterStream not implemented");
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		Driver.unused("setClob not implemented");
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		Driver.unused("setBlob not implemented");
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		Driver.unused("setNClob not implemented");
	}

	@Override
	public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
		Driver.unused("setObject not implemented");
	}

	@Override
	public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
		Driver.unused("setObject not implemented");
	}

	@Override
	public long executeLargeUpdate() throws SQLException {
		return 0;
	}

	@Override
	public void close() throws SQLException {
		super.close();
		this.columnBindValues = null;
		this.sql = null;
		this.bufferArea = null;
		this.tableTypeCache = null;
		this.insertSqlType = null;
	}

	private String processSql(String sql){
		sql = Utils.changeCase(sql);
		sql = sql.trim();
		while (sql.endsWith(";"))
			sql = sql.substring(0, sql.length() - 1);
		sql = sql.trim();
		if(sql.equals("select 1"))
			sql = "select 1 as val";

		return sql;
	}

	private String generateSQL() throws SQLException {
		String[] sqlSplitByQuestionMark = this.sql.split("\\?");
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < sqlSplitByQuestionMark.length; i++) {
			if(i <= this.bufferArea.length - 1 && (this.bufferArea[i] == null || this.bufferArea[i].getValue() == null))
				throw new SQLException("No value specified for parameter " + (i + 1));

			stringBuilder.append(sqlSplitByQuestionMark[i]);
			if(i <= this.bufferArea.length - 1)
				stringBuilder.append(TypeCast.castDbString(this.bufferArea[i].getValue()));
 		}

		return stringBuilder.toString();
	}
}
