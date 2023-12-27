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
import java.util.*;
import java.util.stream.Collectors;


public class JDBCPrepareStatement extends JDBCStatement implements PreparedStatement {
	private String preProcessedSql;
	private String tableName;
	private final int sqlDmlType;
	private List<ColumnBindValue> columnBindValues;
	private Map<Integer, Integer> insertIndexSQLToDDB;
	private Map<Integer, Integer> deleteIndexSQLToDDB;
	private BindValue[] bufferArea;
	private int batchSize;
	private List<String> sqlBuffer;
	private int deleteSqlCombinedNum;
	private List<String> varNames;
	private List<Vector> deleteSqlMakeKeyStrategyVarVectorList;
	private PrepareStatementDeleteStrategy deleteExecuteBatchStrategy;

	public JDBCPrepareStatement(JDBCConnection conn, String sql) throws SQLException {
		super(conn);
		this.batchSize = 0;
		this.connection = conn;
		this.preProcessedSql = preProcessSql(sql, conn);
		String[] sqlSplit = preProcessedSql.split(";");
		String lastStatement  = sqlSplit.length == 0 ? "" : sqlSplit[sqlSplit.length - 1].trim();
		this.sqlDmlType = Utils.getDml(lastStatement);
		this.sqlBuffer = new ArrayList<>();
		this.deleteSqlCombinedNum = 0;
		this.deleteSqlMakeKeyStrategyVarVectorList = new ArrayList<>();
		this.varNames = new ArrayList<>();
		this.insertIndexSQLToDDB = new HashMap<>();
		this.deleteIndexSQLToDDB = new HashMap<>();
		if (this.sqlDmlType == Utils.DML_INSERT) {
			if (sqlSplit.length != 1)
				throw new SQLException("The INSERT statement must be a standalone statement.");
			this.tableName = Utils.getTableName(preProcessedSql, true);
			initColumnBindValues(this.tableName);
			Utils.checkInsertSQLValid(preProcessedSql, columnBindValues.size());

			Map<String, Integer> columnParamInSql = Utils.getInsertColumnParamInSql(preProcessedSql);
			for(ColumnBindValue value : columnBindValues){
				String colName = value.getColName();
				if (columnParamInSql.containsKey(colName)) {
					insertIndexSQLToDDB.put(columnParamInSql.get(colName), value.getIndex());
					columnParamInSql.remove(colName);
				}
			}
			if (columnParamInSql.size() != 0) {
				for (String key : columnParamInSql.keySet())
					throw new SQLException("The column name " + key + " does not exist in table. ");
			}

			this.bufferArea = new BindValue[this.columnBindValues.size()];
		} else if (this.sqlDmlType == Utils.DML_DELETE) {
			this.tableName = Utils.getTableName(preProcessedSql, true);
			initColumnBindValues(this.tableName);

			Map<String, Integer> columnParamInSql = Utils.getDeleteColumnParamInSql(preProcessedSql);
			for(ColumnBindValue value : columnBindValues){
				String colName = value.getColName();
				if (columnParamInSql.containsKey(colName)) {
					deleteIndexSQLToDDB.put(columnParamInSql.get(colName), value.getIndex());
					columnParamInSql.remove(colName);
				}
			}

			if (columnParamInSql.size() != 0) {
				for (String key : columnParamInSql.keySet())
					throw new SQLException("The column name " + key + " does not exist in table. ");
			}

			this.deleteExecuteBatchStrategy = Utils.getPrepareStmtDeleteSqlExecuteBatchStrategy(this.sqlDmlType, this.preProcessedSql, this.deleteIndexSQLToDDB);
			int size = 0;
			for (int i = 0; i < preProcessedSql.length(); i++) {
				char ch = preProcessedSql.charAt(i);
				if(ch == '?')
					size++;
			}

			this.bufferArea = new BindValue[size];
		} else {
			int size = 0;
			for (int i = 0; i < preProcessedSql.length(); i++) {
				char ch = preProcessedSql.charAt(i);
				if(ch == '?')
					size++;
			}

			bufferArea = new BindValue[size];
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
		this.deleteSqlCombinedNum = 0;
		if (!varNames.isEmpty())
			this.varNames.clear();
		if (!deleteSqlMakeKeyStrategyVarVectorList.isEmpty())
			this.deleteSqlMakeKeyStrategyVarVectorList.clear();
		if(this.sqlBuffer != null)
			this.sqlBuffer.clear();
		if(this.columnBindValues != null)
			this.columnBindValues.forEach(ColumnBindValue::clear);
	}

	@Override
	public int[] executeBatch() throws SQLException {
		int[] executeRes;
		try {
			if (this.sqlDmlType == Utils.DML_INSERT) {
				executeRes = tableAppend();
			} else if (this.sqlDmlType == Utils.DML_DELETE) {
				executeRes = new int[this.sqlBuffer.size()];
				if (!this.varNames.isEmpty()
						&& !this.deleteSqlMakeKeyStrategyVarVectorList.isEmpty() && this.varNames.size() == this.deleteSqlMakeKeyStrategyVarVectorList.size()) {
					Map<String, Entity> map = new HashMap<>();
					for (int i = 0; i < this.varNames.size(); i++)
						map.put(this.varNames.get(i), this.deleteSqlMakeKeyStrategyVarVectorList.get(i));

					this.connection.upload(map);
				}

				for (int i = 0; i < this.sqlBuffer.size(); i++ ) {
					try {
						executeRes[i] = super.executeUpdate(sqlBuffer.get(i));
					} catch (Exception e) {
						throw new BatchUpdateException(e.getMessage(), Arrays.copyOf(executeRes, i));
					}
				}
			} else {
				executeRes = new int[this.batchSize];
				for (int i = 0; i < this.batchSize; i++) {
					try {
						executeRes[i] = super.executeUpdate(sqlBuffer.get(i));
					} catch (Exception e) {
						throw new BatchUpdateException(e.getMessage(), Arrays.copyOf(executeRes, i));
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			clearBatch();
		}

		return executeRes;
	}

	private void bind(int paramIndex, Object obj) throws SQLException {
		if (this.sqlDmlType == Utils.DML_INSERT ||
				(this.sqlDmlType == Utils.DML_DELETE && Objects.nonNull(this.deleteExecuteBatchStrategy)
						&& (this.deleteExecuteBatchStrategy.equals(PrepareStatementDeleteStrategy.COMBINE_SQL_WITH_MAKEKEY)
						|| this.deleteExecuteBatchStrategy.equals(PrepareStatementDeleteStrategy.COMBINE_SQL_WITH_IN)))) {
			int index = getDataIndexBySQLIndex(paramIndex);
			if(index >= this.columnBindValues.size())
				throw new SQLException("The index of columnBindValues is out of range.");

			Vector column = this.columnBindValues.get(index).getBindValues();
			try {
				Entity data = BasicEntityFactory.createScalar(column.getDataType(), obj, this.columnBindValues.get(index).getScale());
				if (data.isScalar())
					column.Append((Scalar)data);
				else
					column.Append((Vector) data);
			} catch (Exception e) {
				throw new SQLException(e);
			}

			bufferArea[paramIndex - 1] = new BindValue(obj, false);
		} else {
			bufferArea[paramIndex - 1] = new BindValue(obj, false);
		}
	}

	private int getDataIndexBySQLIndex(int paramIndex) throws SQLException {
		int index = paramIndex - 1;
		if (this.sqlDmlType == Utils.DML_INSERT) {
			if (insertIndexSQLToDDB.size() != 0) {
				if(!insertIndexSQLToDDB.containsKey(index))
					throw new SQLException("paramIndex is out of range");
				index = insertIndexSQLToDDB.get(index);
			}
		} else if (this.sqlDmlType == Utils.DML_DELETE) {
			if (deleteIndexSQLToDDB.size() != 0) {
				if(!deleteIndexSQLToDDB.containsKey(index))
					throw new SQLException("paramIndex is out of range");
				index = deleteIndexSQLToDDB.get(index);
			}
		}

		return index;
	}

	private void bindNull(int paramIndex) throws SQLException {
		int index = getDataIndexBySQLIndex(paramIndex);
		if (this.sqlDmlType == Utils.DML_INSERT || this.sqlDmlType == Utils.DML_DELETE) {
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

	private void combineOneRowData(boolean isBatch) throws Exception {
		if (sqlDmlType == Utils.DML_INSERT) {
			checkInsertBindsLegal(isBatch);
			if(isBatch) {
				for (ColumnBindValue column : columnBindValues) {
					if (column.getBindValues().rows() != batchSize) {
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
				}
			}
		} else if (sqlDmlType == Utils.DML_DELETE) {
			if (isBatch && Objects.nonNull(this.deleteExecuteBatchStrategy)) {
				String[] splitSqls = null;
				if (this.deleteExecuteBatchStrategy.equals(PrepareStatementDeleteStrategy.CONCAT_SQL_CONDITION_WITH_OR)) {
					StringBuilder stringBuilder = new StringBuilder();
					splitSqls = this.preProcessedSql.split("where");
					// if this.preProcessedSql has sub SQL, every time a value is binded, one SQL statement is produced.
					if (splitSqls.length > 2) {
						this.deleteExecuteBatchStrategy = PrepareStatementDeleteStrategy.DEFAULT_DELETE_SQL_EXECUTE_STRATEGY;
						this.sqlBuffer.add(generate_single_execute_delete_sql());
						return;
					}

					if (!this.sqlBuffer.isEmpty()) {
						String preDeleteSql = this.sqlBuffer.get(this.sqlBuffer.size() - 1);
						stringBuilder.append(preDeleteSql);
						combineBindValueWithConditionSql(stringBuilder, splitSqls[1], splitSqls[0], false);
					} else {
						stringBuilder.append(splitSqls[0]);
						combineBindValueWithConditionSql(stringBuilder, splitSqls[1], splitSqls[0], true);
					}
				} else if (this.deleteExecuteBatchStrategy.equals(PrepareStatementDeleteStrategy.DEFAULT_DELETE_SQL_EXECUTE_STRATEGY)) {
					this.sqlBuffer.add(generate_single_execute_delete_sql());
				} else {
					splitSqls = this.preProcessedSql.split("where");
					List<String> sqlColNames = new ArrayList<>();
					StringBuilder builder = new StringBuilder();
					if (this.deleteExecuteBatchStrategy.equals(PrepareStatementDeleteStrategy.COMBINE_SQL_WITH_IN)) {
						builder.append(splitSqls[0]).append(" where ");
						String colName = Utils.getDeleteColumnParamInSql(preProcessedSql).keySet().iterator().next();
						builder.append(colName);
						builder.append(" in ");

						if (deleteSqlCombinedNum == 0) {
							for (ColumnBindValue bindValue : this.columnBindValues) {
								if (bindValue.getColName().equals(colName)) {
									deleteSqlMakeKeyStrategyVarVectorList.add(bindValue.getBindValues());
									String tempVar = "javaapi" + System.currentTimeMillis() + "_var" + 0;
									varNames.add(tempVar);
									builder.append(tempVar);
								}
							}
						}
					} else {
						builder.append(splitSqls[0]).append(" where makeKey(");
						List<String> partsList = Arrays.stream(this.preProcessedSql.split("\\s*(?=[><=]|between|and|or|in)\\s*|\\s*(?<=[><=]|between|and|or|in)\\s*"))
								.filter(str -> !str.isEmpty())
								.collect(Collectors.toList());
						for (int i = 0; i < partsList.size(); i++ ) {
							String sqlPart = partsList.get(i);
							sqlPart = sqlPart.trim();
							if (!sqlPart.equals("?") && !sqlPart.equals("=") && !sqlPart.equals("and")) {
								if (!sqlColNames.isEmpty())
									builder.append(", ");
								String[] words = sqlPart.split("\\s+");
								String lastWord = words[words.length - 1];
								sqlColNames.add(lastWord);
								builder.append(lastWord);
							}
						}
						builder.append(") in makeKey(");

						if (deleteSqlCombinedNum == 0) {
							for (ColumnBindValue column : columnBindValues) {
								if (column.getBindValues().rows() != 0)
									deleteSqlMakeKeyStrategyVarVectorList.add(column.getBindValues());
							}

							for (int i = 0; i < deleteSqlMakeKeyStrategyVarVectorList.size(); i ++) {
								String tempVar = "javaapi" + System.currentTimeMillis() + "_var" + i;
								varNames.add(tempVar);
								builder.append(tempVar);
								if (i != deleteSqlMakeKeyStrategyVarVectorList.size() - 1) {
									builder.append(", ");
								} else {
									builder.append(");");
								}
							}
						}
					}

					if (deleteSqlCombinedNum == 0)
						this.sqlBuffer.add(builder.toString());
					deleteSqlCombinedNum ++;
				}
			} else {
				this.sqlBuffer.add(generate_single_execute_delete_sql());
			}
		} else {
			this.sqlBuffer.add(generateSQL());
		}
	}

	private void combineBindValueWithConditionSql(StringBuilder stringBuilder, String conditionSqlPart, String dmlSqlPart, boolean newSqlTag) throws SQLException {
		StringBuilder tempBuilder = new StringBuilder();
		String[] conditionSqlSplitByQuestionMark = conditionSqlPart.replaceAll(";", "").trim().split("\\?");
		if (this.bufferArea.length != 0) {
			tempBuilder.append(" (");
			for (int i = 0; i < this.bufferArea.length; i++) {
				if (this.bufferArea[i] == null || this.bufferArea[i].getValue() == null)
					throw new SQLException("No value specified for parameter " + (i + 1));

				tempBuilder.append(conditionSqlSplitByQuestionMark[i]);
				tempBuilder.append(TypeCast.castDbString(this.bufferArea[i].getValue()));

				if (conditionSqlSplitByQuestionMark.length > this.bufferArea.length
						&& i == this.bufferArea.length - 1 && Objects.nonNull(conditionSqlSplitByQuestionMark[i+1]))
					tempBuilder.append(conditionSqlSplitByQuestionMark[i+1]);
			}

			tempBuilder.append(") ");

			// check if combined sql's length rather than 65535;
			if (stringBuilder.toString().length() + tempBuilder.toString().length() <= 65535) {
				if (newSqlTag)
					this.sqlBuffer.add(stringBuilder.append("where ").append(tempBuilder).toString());
				else
					this.sqlBuffer.set(this.sqlBuffer.size() - 1, stringBuilder.append(" or ").append(tempBuilder).toString());
			} else if (dmlSqlPart.length() + tempBuilder.toString().length() > 65535) {
				throw new RuntimeException("The delete sql's length rather than 65535 and where condition part is too long, cannot split into multi sqls to run.");
			} else if (stringBuilder.toString().length() + tempBuilder.toString().length() > 65535) {
				StringBuilder newSqlBuilder = new StringBuilder();
				newSqlBuilder.append(dmlSqlPart);
				combineBindValueWithConditionSql(newSqlBuilder, conditionSqlPart, dmlSqlPart, true);
			}
		} else {
			// no placeholder
			stringBuilder.append(" where ").append(conditionSqlSplitByQuestionMark[0]);
		}
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		try {
			combineOneRowData(false);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		try{
			return super.executeQuery(sqlBuffer.get(0));
		}finally{
			clearBatch();
		}
	}

	private void checkInsertBindsLegal(boolean isBatch) throws SQLException {
		int rows = isBatch ? batchSize : 1;
		if (insertIndexSQLToDDB.size() == 0) {
			for (ColumnBindValue bindValue : columnBindValues){
				if(bindValue.getBindValues().rows() != rows)
					throw new SQLException("The column " + bindValue.getColName() + " is not set.");
			}
		}else {
			for (Integer index : insertIndexSQLToDDB.keySet()) {
				if (this.columnBindValues.get(insertIndexSQLToDDB.get(index)).getBindValues().rows() != rows)
					throw new SQLException("The column " + this.columnBindValues.get(insertIndexSQLToDDB.get(index)).getColName() + " is not set.");
			}
		}
	}

	@Override
	public int executeUpdate() throws SQLException {
		try {
			combineOneRowData(false);
			if (this.sqlDmlType == Utils.DML_INSERT) {
				int[] ret = tableAppend();
				if (ret[0] == SUCCESS_NO_INFO)
					return 1;
				else
					return 0;
			} else {
				return super.executeUpdate(sqlBuffer.get(0));
			}
		} catch (Exception e) {
			throw new SQLException(e);
		} finally {
			clearBatch();
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
			int[] value = new int[arguments.get(0).rows()];
			if (arguments.get(0).rows() != size)
				Arrays.fill(value, EXECUTE_FAILED);
			else
				Arrays.fill(value, SUCCESS_NO_INFO);
			return value;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private List<Vector> createDFSArguments() {
		return columnBindValues.stream()
				.map(ColumnBindValue::getBindValues)
				.collect(Collectors.toList());
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		if (this.sqlDmlType == Utils.DML_INSERT || this.sqlDmlType == Utils.DML_DELETE) {
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
					combineOneRowData(false);
					Entity entity = connection.run(sqlBuffer.get(0));
					if (entity instanceof BasicTable) {
						ResultSet resultSet_ = new JDBCResultSet(connection, this, entity, sqlBuffer.get(0), this.getMaxRows());
						resultSets.offerLast(resultSet_);
						objectQueue.offer(resultSet_);
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
		try {
			combineOneRowData(true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
		String[] sqlSplitByQuestionMark = this.preProcessedSql.split("\\?");
		StringBuilder stringBuilder = new StringBuilder();
		if(this.bufferArea.length > sqlSplitByQuestionMark.length)
			throw new SQLException("error size of bufferArea. ");

		if (this.bufferArea.length != 0) {
			for (int i = 0; i < this.bufferArea.length; i++) {
				if (this.bufferArea[i] == null || this.bufferArea[i].getValue() == null)
					throw new SQLException("No value specified for parameter " + (i + 1));

				stringBuilder.append(sqlSplitByQuestionMark[i]);
				stringBuilder.append(TypeCast.castDbString(this.bufferArea[i].getValue()));

				if (sqlSplitByQuestionMark.length > this.bufferArea.length
						&& i == this.bufferArea.length - 1 && Objects.nonNull(sqlSplitByQuestionMark[i+1]))
					stringBuilder.append(sqlSplitByQuestionMark[i+1]);
			}
		} else {
			// no placeholder
			stringBuilder.append(sqlSplitByQuestionMark[0]);
		}

		return stringBuilder.toString();
	}

	/**
	 * generate prepareStatement to-execute single delete sql
	 *
	 * @return
	 * @throws SQLException
	 */
	private String generate_single_execute_delete_sql() throws SQLException {
		if(Utils.isEmpty(this.preProcessedSql))
			throw new SQLException("preProcessedSql is null. ");
		String[] sqlSplitByQuestionMark = this.preProcessedSql.split("\\?");
		StringBuilder stringBuilder = new StringBuilder();
		int size = 0;
		for (int i = 0; i < preProcessedSql.length(); i++) {
			char ch = preProcessedSql.charAt(i);
			if(ch == '?')
				size++;
		}

		// generate new bufferArea
		BindValue[] newBufferArea = new BindValue[size];
		System.arraycopy(this.bufferArea, 0, newBufferArea, 0, size);
		if(newBufferArea.length > sqlSplitByQuestionMark.length)
			throw new SQLException("error size of bufferArea. ");

		if (newBufferArea.length != 0) {
			for (int i = 0; i < newBufferArea.length; i++) {
				if (newBufferArea[i] == null || newBufferArea[i].getValue() == null)
					throw new SQLException("No value specified for parameter " + (i + 1));

				stringBuilder.append(sqlSplitByQuestionMark[i]);
				stringBuilder.append(TypeCast.castDbString(newBufferArea[i].getValue()));

				if (sqlSplitByQuestionMark.length > newBufferArea.length
						&& i == newBufferArea.length - 1 && Objects.nonNull(sqlSplitByQuestionMark[i+1]))
					stringBuilder.append(sqlSplitByQuestionMark[i+1]);
			}
		} else {
			// no placeholder
			stringBuilder.append(sqlSplitByQuestionMark[0]);
		}

		return stringBuilder.toString();
	}

	public enum PrepareStatementDeleteStrategy {
		CONCAT_SQL_CONDITION_WITH_OR("CONCAT_SQL_CONDITION_WITH_OR", 0),
		COMBINE_SQL_WITH_IN("COMBINE_SQL_WITH_IN", 1),
		COMBINE_SQL_WITH_MAKEKEY("COMBINE_SQL_WITH_MAKEKEY", 2),
		DEFAULT_DELETE_SQL_EXECUTE_STRATEGY("DEFAULT_DELETE_SQL_EXECUTE_STRATEGY", 3);

		private String name;
		private Integer code;

		PrepareStatementDeleteStrategy(String name, Integer code) {
			this.name = name;
			this.code = code;
		}

		public String getName() {
			return name;
		}

		public Integer getCode() {
			return code;
		}

		public static PrepareStatementDeleteStrategy getByName(String name) {
			for (PrepareStatementDeleteStrategy strategy : PrepareStatementDeleteStrategy.values()) {
				if (strategy.getName().equals(name)) {
					return strategy;
				}
			}
			throw new IllegalArgumentException("No matching PrepareStatementDeleteStrategy constant found for name: " + name);
		}
	}
}
