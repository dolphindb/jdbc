package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.data.Void;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


public class JDBCPrepareStatement extends JDBCStatement implements PreparedStatement {
	private String preProcessedSql;
	private String tableName;
	private final int sqlDmlType;
	private List<ColumnBindValue> columnBindValues;
	private Map<Integer, Integer> insertIndexSQLToDDB;
	private Set<Integer> nowColumnIndexes;
	private BindValue[] bufferArea;
	private int batchSize;
	private List<String> sqlBuffer;
	private boolean isPreparedStatement;
	private int insertRowsPerExecution;
	private int insertValueCountPerRow;
	private int insertTotalParameterCount;

	public JDBCPrepareStatement(JDBCConnection conn, String sql) throws SQLException {
		super(conn);
		this.batchSize = 0;
		this.connection = conn;
		this.preProcessedSql = preProcessSql(sql, conn);
		String[] sqlSplit = preProcessedSql.split(";");
		String lastStatement  = sqlSplit.length == 0 ? "" : sqlSplit[sqlSplit.length - 1].trim();
		this.sqlDmlType = Utils.getDml(lastStatement);
		this.sqlBuffer = new ArrayList<>();
		this.insertIndexSQLToDDB = new HashMap<>();
		this.nowColumnIndexes = new HashSet<>();
		this.insertRowsPerExecution = 1;
		this.insertValueCountPerRow = 0;
		this.insertTotalParameterCount = 0;
		boolean isInsert = this.sqlDmlType == Utils.DML_INSERT;
		boolean hasPlaceholder = preProcessedSql.contains("?");
		// values(now()) has no '?'; still needs tableInsert path. Literal INSERT like
		// values(1,100) must keep the legacy whole-SQL path (JAVAOS-624 / official no-placeholder tests).
		boolean hasInsertNow = isInsert && Utils.containsPreparedInsertNow(preProcessedSql);
		boolean usePreparedInsertPath = isInsert && (hasPlaceholder || hasInsertNow);
		if (hasPlaceholder || hasInsertNow)
			isPreparedStatement = true;
		if (isPreparedStatement) {
			if (usePreparedInsertPath) {
				if (sqlSplit.length != 1)
					throw new SQLException("The INSERT statement must be a standalone statement.");

				Utils.PreparedInsertInfo insertInfo = Utils.parsePreparedInsertInfo(preProcessedSql);
				this.tableName = insertInfo.getTableName();
				this.insertRowsPerExecution = insertInfo.getRowsPerExecution();
				this.insertValueCountPerRow = insertInfo.getValueCountPerRow();
				this.insertTotalParameterCount = insertInfo.getTotalParameterCount();
				initColumnBindValues(this.tableName);
				Utils.checkInsertSQLValid(preProcessedSql, columnBindValues.size());
				buildInsertParamMapping(insertInfo);

				this.bufferArea = new BindValue[Math.max(insertTotalParameterCount, 0)];
			} else {
				int size = 0;
				for (int i = 0; i < preProcessedSql.length(); i++) {
					char ch = preProcessedSql.charAt(i);
					if(ch == '?')
						size++;
				}

				bufferArea = new BindValue[size];
			}
		} else {
			this.sqlBuffer.add(preProcessedSql);
		}
	}

	private void buildInsertParamMapping(Utils.PreparedInsertInfo insertInfo) throws SQLException {
		List<InsertValueSlot> slotKinds = insertInfo.getSlotKinds();
		List<String> sqlColumns = insertInfo.getColumnNames();
		boolean explicit = insertInfo.hasExplicitColumns();
		Map<String, Integer> nameToDdb = new HashMap<>();
		for (ColumnBindValue value : columnBindValues) {
			nameToDdb.put(value.getColName(), value.getIndex());
		}

		if (explicit) {
			for (String colName : sqlColumns) {
				if (!nameToDdb.containsKey(colName))
					throw new SQLException("The column name " + colName + " does not exist in table. ");
			}
		}

		int dense = 0;
		for (int row = 0; row < insertRowsPerExecution; row++) {
			for (int sqlCol = 0; sqlCol < slotKinds.size(); sqlCol++) {
				int ddbIndex;
				if (explicit) {
					ddbIndex = nameToDdb.get(sqlColumns.get(sqlCol));
				} else {
					ddbIndex = sqlCol;
				}
				if (slotKinds.get(sqlCol) == InsertValueSlot.PLACEHOLDER) {
					insertIndexSQLToDDB.put(dense++, ddbIndex);
				} else {
					nowColumnIndexes.add(ddbIndex);
				}
			}
		}
	}

	private void initColumnBindValues(String tableName) throws SQLException {
		try {
			this.columnBindValues = new ArrayList<>();
			BasicDictionary schema = (BasicDictionary) connection.run(String.format("schema(%s)", tableName));
			BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
			AbstractVector names = (AbstractVector) colDefs.getColumn("name");
			BasicIntVector colDefsTypeInt = (BasicIntVector) colDefs.getColumn("typeInt");
			BasicIntVector extraInt = (BasicIntVector) colDefs.getColumn("extra");
			for (int i = 0; i < names.rows(); i++) {
				String colName = names.getString(i).toLowerCase();
				int typeInt = colDefsTypeInt.getInt(i);
				Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(typeInt);
				int extra = extraInt.getInt(i);
				ColumnBindValue columnBindValue = new ColumnBindValue(i, colName, type, extra);
				this.columnBindValues.add(columnBindValue);
			}
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void clearBatch() throws SQLException {
		super.clearBatch();
		this.batchSize = 0;
		if(this.sqlBuffer != null) {
			this.sqlBuffer.clear();
			if (!isPreparedStatement && preProcessedSql != null && !preProcessedSql.isEmpty())
				this.sqlBuffer.add(preProcessedSql);
		}
		if(this.columnBindValues != null)
			this.columnBindValues.forEach(ColumnBindValue::clear);
	}

	@Override
	public int[] executeBatch() throws SQLException {
		int[] executeRes = new int[this.batchSize];
		try {
			if (this.sqlDmlType == Utils.DML_INSERT)
				return tableAppend(true);
			for (int i = 0; i < this.batchSize; i++) {
				try {
					if (this.sqlDmlType == Utils.DML_SELECT || this.sqlDmlType == Utils.DML_EXEC) {
						throw new SQLException("Can not issue SELECT or EXEC via executeUpdate().");
					} else if (isStandardDML()) {
						executeRes[i] = executeFinalizedUpdate(i);
					} else {
						connection.run(sqlBuffer.get(i));
						executeRes[i] = 0;
					}
				} catch (Exception e) {
					throw new BatchUpdateException(e.getMessage(), Arrays.copyOf(executeRes, i));
				}
			}
		} finally {
			clearBatch();
		}

		return executeRes;
	}

	private void bind(int paramIndex, Object obj) throws SQLException {
		if (this.sqlDmlType == Utils.DML_INSERT) {
			int index = getDataIndexBySQLIndex(paramIndex);
			if(index >= this.columnBindValues.size())
				throw new SQLException("The index of columnBindValues is out of range.");

			Vector column = this.columnBindValues.get(index).getBindValues();
			try {
				if (obj instanceof Vector) {
					column.Append((Vector) obj);
				} else if (obj instanceof DolphinDBArray) {
					column.Append(((DolphinDBArray) obj).getVector());
				} else {
					Entity data;
					Entity.DATA_TYPE dataType = column.getDataType();
					int scale = this.columnBindValues.get(index).getScale();
					if (obj instanceof String) {
						data = Utils.createScalar(dataType, (String) obj, scale);
					} else if (obj instanceof String[]) {
						data = Utils.createStringVector(dataType, (String[]) obj, scale);
					} else {
						data = BasicEntityFactory.createScalar(dataType, obj, scale);
					}
					if (data.isScalar())
						column.Append((Scalar) data);
					else
						column.Append((Vector) data);
				}
			} catch (Exception e) {
				throw new SQLException(e);
			}
		} else {
			bufferArea[paramIndex - 1] = new BindValue(obj, false);
		}
	}

	private int getDataIndexBySQLIndex(int paramIndex) throws SQLException {
		int index = paramIndex - 1;
		if (this.sqlDmlType == Utils.DML_INSERT) {
			if (paramIndex < 1 || paramIndex > insertTotalParameterCount)
				throw new SQLException("paramIndex is out of range");
			if (insertIndexSQLToDDB.isEmpty())
				return index % Math.max(insertValueCountPerRow, 1);
			if (!insertIndexSQLToDDB.containsKey(index))
				throw new SQLException("paramIndex is out of range");
			return insertIndexSQLToDDB.get(index);
		}
		if (insertIndexSQLToDDB.size() != 0) {
			if(!insertIndexSQLToDDB.containsKey(index))
				throw new SQLException("paramIndex is out of range");
			index = insertIndexSQLToDDB.get(index);
		}

		return index;
	}

	private void bindNull(int paramIndex) throws SQLException {
		int index = getDataIndexBySQLIndex(paramIndex);
		if (this.sqlDmlType == Utils.DML_INSERT) {
			Vector column = this.columnBindValues.get(index).getBindValues();
			try {
				int typeValue = column.getDataType().getValue();
				if (typeValue < 65) {
					column.Append((Scalar)TypeCast.nullScalar(columnBindValues.get(index).getType()));
				}else{
					Vector tmp = BasicEntityFactory.instance().createVectorWithDefaultValue(Entity.DATA_TYPE.valueOf(typeValue - 64), 0, 0);
					column.Append(tmp);
				}
			} catch (Exception e){
				throw new SQLException(e);
			}
		} else {
			bufferArea[paramIndex - 1] = new BindValue(TypeCast.nullScalar(columnBindValues.get(index).getType()), false);
		}
	}

	private boolean isStandardDML() {
		return sqlDmlType == Utils.DML_UPDATE ||
		       sqlDmlType == Utils.DML_DELETE;
	}

	private int expectedInsertRows(boolean isBatch) {
		return (isBatch ? batchSize : 1) * insertRowsPerExecution;
	}

	private void appendNullToColumn(ColumnBindValue column) throws SQLException {
		Vector columnCol = column.getBindValues();
		try {
			if (columnCol.getDataType().getValue() < 65)
				columnCol.Append((Scalar) BasicEntityFactory.createScalar(columnCol.getDataType(), null, column.getScale()));
			else
				columnCol.Append((Vector) BasicEntityFactory.createScalar(columnCol.getDataType(), null, column.getScale()));
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private void combineOneRowData(boolean isBatch) throws SQLException {
		if (sqlDmlType == Utils.DML_INSERT) {
			checkInsertBindsLegal(isBatch);
			if(isBatch) {
				int expectedRows = expectedInsertRows(true);
				padMissingColumnsWithNull(expectedRows);
			}
		} else {
			this.sqlBuffer.add(generateSQL());
		}
	}

	private void padMissingColumnsWithNull(int expectedRows) throws SQLException {
		for (ColumnBindValue column : columnBindValues) {
			if (nowColumnIndexes.contains(column.getIndex()))
				continue;
			while (column.getBindValues().rows() < expectedRows) {
				appendNullToColumn(column);
			}
		}
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		try{
			if (isPreparedStatement) {
				combineOneRowData(false);
				if (sqlDmlType == Utils.DML_SELECT || sqlDmlType == Utils.DML_EXEC) {
					return executeFinalizedQuery();
				} else {
					return super.executeQuery(sqlBuffer.get(0));
				}
			} else {
				return super.executeQuery(sqlBuffer.get(0));
			}
		} finally {
			if (isPreparedStatement)
				clearBatch();
		}
	}

	private void checkInsertBindsLegal(boolean isBatch) throws SQLException {
		int rows = expectedInsertRows(isBatch);
		Set<Integer> placeholderColumns = new HashSet<>(insertIndexSQLToDDB.values());
		if (placeholderColumns.isEmpty() && insertTotalParameterCount == 0) {
			return;
		}
		if (placeholderColumns.isEmpty()) {
			for (ColumnBindValue bindValue : columnBindValues) {
				if (nowColumnIndexes.contains(bindValue.getIndex()))
					continue;
				if (bindValue.getBindValues().rows() != rows)
					throw new SQLException("The column " + bindValue.getColName() + " is not set.");
			}
			return;
		}
		for (Integer ddbIndex : placeholderColumns) {
			if (this.columnBindValues.get(ddbIndex).getBindValues().rows() != rows)
				throw new SQLException("The column " + this.columnBindValues.get(ddbIndex).getColName() + " is not set.");
		}
	}

	@Override
	public int executeUpdate() throws SQLException {
		try {
			if (isPreparedStatement) {
				combineOneRowData(false);
				if (this.sqlDmlType == Utils.DML_INSERT) {
					return tableAppend(false)[0];
				} else if (this.sqlDmlType == Utils.DML_SELECT) {
					ResultSet rs = executeFinalizedQuery();
					objectQueue.offer(rs);
					return 0;
				} else if (this.sqlDmlType == Utils.DML_EXEC) {
					throw new SQLException("Can not issue SELECT or EXEC via executeUpdate().");
				} else if (isStandardDML()) {
					return executeFinalizedUpdate(0);
				} else {
					connection.run(sqlBuffer.get(0));
					return 0;
				}
			} else {
				return super.executeUpdate(sqlBuffer.get(0));
			}
		} catch (Exception e) {
			throw new SQLException(e);
		} finally {
			clearBatch();
		}
	}

	private int[] tableAppend(boolean isBatch) throws SQLException {
		int expectedRows = expectedInsertRows(isBatch);
		if (!isBatch) {
			padMissingColumnsWithNull(expectedRows);
		}
		fillNowColumns(expectedRows);
		List<Vector> arguments = columnBindValues.stream()
				.map(ColumnBindValue::getBindValues)
				.collect(Collectors.toList());
		List<String> colNames = new ArrayList<>();
		columnBindValues.forEach(e -> colNames.add(e.getColName()));
		BasicTable basicTable = new BasicTable(colNames, arguments);
		List<Entity> param = new ArrayList<>();
		param.add(basicTable);
		try {
			int size = ((Scalar)connection.run("tableInsert{" + tableName + "}", param)).getNumber().intValue();
			if (isBatch) {
				int[] value = new int[batchSize];
				Arrays.fill(value, SUCCESS_NO_INFO);
				return value;
			} else {
				return new int[]{size};
			}
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private void fillNowColumns(int expectedRows) throws SQLException {
		if (nowColumnIndexes == null || nowColumnIndexes.isEmpty())
			return;
		Entity serverNow;
		try {
			serverNow = connection.run("now()");
		} catch (IOException e) {
			throw new SQLException(e);
		}
		for (Integer colIdx : nowColumnIndexes) {
			ColumnBindValue column = columnBindValues.get(colIdx);
			Scalar nowScalar = convertServerNowToColumnType(serverNow, column);
			Vector columnCol = column.getBindValues();
			try {
				while (columnCol.rows() < expectedRows) {
					columnCol.Append(nowScalar);
				}
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
	}

	private Scalar convertServerNowToColumnType(Entity serverNow, ColumnBindValue column) throws SQLException {
		Entity.DATA_TYPE type = column.getType();
		LocalDateTime dateTime;
		try {
			if (serverNow instanceof BasicTimestamp) {
				dateTime = ((BasicTimestamp) serverNow).getTimestamp();
			} else if (serverNow instanceof BasicDateTime) {
				dateTime = ((BasicDateTime) serverNow).getDateTime();
			} else if (serverNow instanceof BasicNanoTimestamp) {
				Temporal temporal = ((BasicNanoTimestamp) serverNow).getTemporal();
				dateTime = (LocalDateTime) temporal;
			} else if (serverNow instanceof Scalar) {
				Temporal temporal = ((Scalar) serverNow).getTemporal();
				if (temporal instanceof LocalDateTime) {
					dateTime = (LocalDateTime) temporal;
				} else {
					throw new SQLException("Unsupported server now() type: " + serverNow.getDataType());
				}
			} else {
				throw new SQLException("Unsupported server now() type: " + serverNow.getClass().getName());
			}
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
		if (dateTime == null) {
			throw new SQLException("server now() returned null");
		}
		switch (type) {
			case DT_DATETIME:
				return new BasicDateTime(dateTime);
			case DT_TIMESTAMP:
				return new BasicTimestamp(dateTime);
			case DT_NANOTIMESTAMP:
				return new BasicNanoTimestamp(dateTime);
			default:
				throw new SQLException("Column " + column.getColName()
						+ " does not support now(); expected DATETIME, TIMESTAMP or NANOTIMESTAMP, got " + type);
		}
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		if (this.sqlDmlType == Utils.DML_INSERT) {
			bindNull(parameterIndex);
		} else {
			Object bindValue;
			switch (sqlType) {
				case Types.VARCHAR:
					bindValue = "";
					break;
				case Types.OTHER:
					bindValue = new Void();
					break;
				default:
					bindValue = new Void();
			}
			bufferArea[parameterIndex - 1] = new BindValue(bindValue, true);
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
		bind(parameterIndex, x);
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
		if (Objects.nonNull(bufferArea)) {
			Arrays.fill(bufferArea, null);
		}
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		bind(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		bind(parameterIndex, x);
	}
	@Override
	public boolean execute() throws SQLException {
		try {
			switch (this.sqlDmlType){
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
					if (isPreparedStatement)
						combineOneRowData(false);
					String finalSql = sqlBuffer.get(0);
					String lastStatement = getLastStatement(finalSql);
					if (isNonSqlMatchedRowCountStatement(lastStatement)) {
						objectQueue.offer(executeUpdateWithRowCount(finalSql));
					} else {
						Entity entity = connection.run(finalSql);
						Integer updateCount = extractNonSqlUpdateCount(finalSql, entity);
						if (updateCount != null) {
							objectQueue.offer(updateCount);
						} else if (entity instanceof BasicTable) {
							ResultSet resultSet_ = new JDBCResultSet(connection, this, entity, finalSql, this.getMaxRows());
							resultSets.offerLast(resultSet_);
							objectQueue.offer(resultSet_);
						}
					}
					clearBatch();	
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
		this.batchSize++;
		combineOneRowData(true);
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
		if (this.sqlDmlType != Utils.DML_INSERT) {
			throw new SQLException("setBlob is not supported for non-INSERT prepared statements in literal SQL path.");
		}
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
		if (x instanceof DolphinDBArray) {
			bind(parameterIndex, x);
		} else {
            throw new SQLException("setArray method only supports DolphinDBArray parameter.");
		}
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
		Driver.unused("setNClob not implemented");
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		Driver.unused("setSQLXML not implemented");
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
		this.preProcessedSql = null;
		this.bufferArea = null;
	}

	private String preProcessSql(String sql, JDBCConnection con) {
		sql = Utils.changeCase(sql, con);
		sql = sql.trim();
		while (sql.endsWith(";"))
			sql = sql.substring(0, sql.length() - 1);
		sql = sql.trim();
		if(sql.equals("select 1"))
			sql = "select 1 as val";

		return sql;
	}

	private String generateSQL() throws SQLException {
		if(Utils.isEmpty(this.preProcessedSql))
			throw new SQLException("preProcessedSql is null. ");
		String tpl = this.preProcessedSql;
		if (this.sqlDmlType == Utils.DML_SELECT || this.sqlDmlType == Utils.DML_EXEC) {
			tpl = Utils.oracleToDolphin(Utils.outerJoinToFullJoin(tpl));
		}
		String[] sqlSplitByQuestionMark = tpl.split("\\?");
		StringBuilder stringBuilder = new StringBuilder();
		if(this.bufferArea.length > sqlSplitByQuestionMark.length)
			throw new SQLException("error size of bufferArea. ");

		if (this.bufferArea.length != 0) {
			for (int i = 0; i < this.bufferArea.length; i++) {
				if (this.bufferArea[i] == null || this.bufferArea[i].getValue() == null)
					throw new SQLException("No value specified for parameter " + (i + 1));

				stringBuilder.append(sqlSplitByQuestionMark[i]);
				stringBuilder.append(TypeCast.castDbString(this.bufferArea[i].getValue()));
			}

			if (sqlSplitByQuestionMark.length > this.bufferArea.length && Objects.nonNull(sqlSplitByQuestionMark[this.bufferArea.length]))
				stringBuilder.append(sqlSplitByQuestionMark[this.bufferArea.length]);
		} else {
			stringBuilder.append(sqlSplitByQuestionMark[0]);
		}

		return stringBuilder.toString();
	}

	private ResultSet executeFinalizedQuery() throws SQLException {
		if (super.getQueryTimeout() > 0) {
			Future<ResultSet> future = executorService.submit(() -> executeFinalizedQueryInternal());
			try {
				return future.get(super.getQueryTimeout(), TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				future.cancel(true);
				cancelJobOperation();
				throw new SQLTimeoutException("PrepareStatement execute query timed out after " + super.getQueryTimeout() + " seconds.", e);
			} catch (Exception e) {
				throw new SQLException(e);
			}
		} else {
			return executeFinalizedQueryInternal();
		}
	}

	private int executeFinalizedUpdate(int index) throws SQLException {
		if (super.getQueryTimeout() > 0) {
			Future<Integer> future = executorService.submit(() -> super.executeUpdateWithRowCount(sqlBuffer.get(index)));
			try {
				return future.get(super.getQueryTimeout(), TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				future.cancel(true);
				cancelJobOperation();
				throw new SQLTimeoutException("PrepareStatement execute update timed out after " + super.getQueryTimeout() + " seconds.", e);
			} catch (Exception e) {
				throw new SQLException(e);
			}
		} else {
			return super.executeUpdateWithRowCount(sqlBuffer.get(index));
		}
	}

	private ResultSet executeFinalizedQueryInternal() throws SQLException {
		try {
			String finalSql = sqlBuffer.get(0);
			Entity entity;
			if(super.getFetchSize() != 0) {
				if (super.getFetchSize() < 8192) {
					throw new SQLException("The fetchSize param must be greater than 8192.");
				}
				entity = connection.run(finalSql, super.getFetchSize());
			} else {
				entity = connection.run(finalSql);
			}

			if (entity instanceof BasicTable || entity.getDataForm() == Entity.DATA_FORM.DF_SCALAR
					|| entity.getDataForm() == Entity.DATA_FORM.DF_VECTOR || entity.getDataForm() == Entity.DATA_FORM.DF_MATRIX) {
				resultSet = new JDBCResultSet(connection, this, entity, finalSql, super.getMaxRows());
				return resultSet;
			} else if(entity instanceof EntityBlockReader) {
				resultSet = new JDBCResultSet(connection, this, (EntityBlockReader) entity, finalSql, super.getMaxRows());
				return resultSet;
			} else {
				throw new SQLException("The given SQL statement produces anything other than a single ResultSet object.");
			}
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
