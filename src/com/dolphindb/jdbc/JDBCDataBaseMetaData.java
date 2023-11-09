package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class JDBCDataBaseMetaData implements DatabaseMetaData {

    private static final String DATABASE_NAME = "DolphinDB";
    private static final String DRIVER_NAME = "DolphinDB JDBC Driver";
    private static final String DRIVER_VERSION = "dolphindb-connector-java-2.0";
    private static final String DATABASE = "database";
    private final JDBCConnection connection;
    private final JDBCStatement statement;
    private static ResultSet TypeInfo;
    private static ResultSet Catalogs;
    private static ResultSet Schemas;
    public JDBCDataBaseMetaData(JDBCConnection connection, JDBCStatement statement){
        this.connection = connection;
        this.statement = statement;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<String> colNames = new ArrayList<>(Collections.singletonList("TABLE_CAT"));
        List<Vector> cols = new ArrayList<>();

        try {
            BasicStringVector dbs = (BasicStringVector) connection.run("getClusterDFSDatabases()");
            cols.add(dbs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Catalogs = new JDBCResultSet(connection, statement, new BasicTable(colNames, cols), "");
        return Catalogs;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public String getCatalogTerm() {
        return DATABASE;
    }

    @Override
    public ResultSet getClientInfoProperties() {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        if (Objects.isNull(tableNamePattern) && tableNamePattern.isEmpty())
            throw new SQLException("The param 'tableNamePattern' cannot be null.");

        BasicTable colDefs = null;
        // get columns origin meta data.
        colDefs = getColumnsOriginMetaData(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        // reassemble some columns' data.
        assembleColumnsMetaData(colDefs, catalog, tableNamePattern, columnNamePattern);

        return new JDBCResultSet(connection,statement, colDefs,"");
    }

    private BasicTable getColumnsOriginMetaData(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        BasicTable colDefs = null;
        if (Objects.nonNull(catalog) && !catalog.isEmpty()) {
            // specify tableName for dfs table
            String script;
            try {
                // specify columnNamePattern for dfs table
                if (Objects.nonNull(columnNamePattern) && !columnNamePattern.isEmpty() && !columnNamePattern.equals("%")) {
                    script = "handle=loadTable(\"" + catalog + "\", `" + tableNamePattern + "); " +
                            "select * from schema(handle).colDefs where name = '%s'";
                    script = String.format(script, columnNamePattern);
                    colDefs = (BasicTable) connection.run(script);
                } else {
                    script = "handle=loadTable(\"" + catalog + "\", `" + tableNamePattern + "); schema(handle);";
                    BasicDictionary schema = (BasicDictionary) connection.run(script);
                    colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (tableNamePattern.matches("%+")) {
            // get all tables of catalog's.
            try {
                String script = "getClusterDFSTables();";
                BasicStringVector allTablesVec = (BasicStringVector) connection.run(script);

                for (int i = 0; i < allTablesVec.rows(); i ++) {
                    String tempDbAndTableName = allTablesVec.getString(i);
                    // tempTableName
                    int lastSlashIndex = tempDbAndTableName.lastIndexOf("/");
                    if (lastSlashIndex != -1) {
                        String dbName = tempDbAndTableName.substring(0, lastSlashIndex);
                        String tempTableName = tempDbAndTableName.substring(lastSlashIndex + 1);
                        String dfsTableHandle = "handle=loadTable(\"" + dbName + "\", `" + tempTableName + "); schema(handle);";
                        BasicDictionary schema = (BasicDictionary) connection.run(dfsTableHandle);
                        BasicTable tempColDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                        if (Objects.nonNull(colDefs))
                            colDefs = colDefs.combine(tempColDefs);
                        else
                            colDefs = tempColDefs;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // memory table
            try {
                // specify columnNamePattern for mem table.
                if (Objects.nonNull(columnNamePattern) && !columnNamePattern.isEmpty()) {
                    String script = "select * from schema(" + tableNamePattern + ").colDefs where name = '" + columnNamePattern + "';";
                    colDefs = (BasicTable) connection.run(script);
                    if (colDefs.getColumn(0).rows() == 0)
                        throw new RuntimeException("The column: '" + columnNamePattern + "' doesn't exist in table: '" + tableNamePattern + "'.");
                } else {
                    // get all columns for specify mem table
                    BasicDictionary schema = (BasicDictionary) connection.run("schema(" + tableNamePattern + ");");
                    colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return colDefs;
    }

    private void assembleColumnsMetaData(BasicTable colDefs, String catalog, String tableNamePattern, String columnNamePattern) {
        List<String> columnIndexList = new ArrayList<>();
        try {
            BasicDictionary schema = null;
            if (Objects.nonNull(catalog) && !catalog.isEmpty())
                schema = (BasicDictionary) connection.run("schema(handle);");
            else
                schema = (BasicDictionary) connection.run("schema(" + tableNamePattern + ");");

            Entity columnNameEntity = schema.get("partitionColumnName");

            // get 'partitonColumn'
            if (Objects.nonNull(columnNameEntity)) {
                if (columnNameEntity.isScalar()) {
                    BasicString columnName = (BasicString) columnNameEntity;
                    columnIndexList.add(columnName.getString());
                } else if (columnNameEntity.isVector()) {
                    BasicStringVector columnNameVec = (BasicStringVector) columnNameEntity;
                    columnIndexList.addAll(Arrays.asList(columnNameVec.getdataArray()));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<String> newColumnNames = new ArrayList<>();
        newColumnNames.add("COLUMN_NAME");
        newColumnNames.add("TYPE_NAME");
        newColumnNames.add("DATA_TYPE");
        newColumnNames.add("EXTRA");
        newColumnNames.add("REMARKS");
        colDefs.setColName(newColumnNames);

        // set 'IS_NULLABLE'
        List<String> isNullableStrList = new ArrayList<>();
        BasicStringVector nameColumn = (BasicStringVector) colDefs.getColumn(0);
        String[] nameArr = nameColumn.getdataArray();
        Arrays.stream(nameArr)
                .map(str -> columnIndexList.contains(str) ? "NO" : "YES")
                .forEach(isNullableStrList::add);
        colDefs.addColumn("IS_NULLABLE", new BasicStringVector(isNullableStrList));

        // set 'ORDINAL_POSITION'
        if (Objects.nonNull(columnNamePattern) && !columnNamePattern.isEmpty() && !columnNamePattern.equals("%")) {
            // specify 'columnNamePattern'
            try {
                String script = null;
                if (Objects.nonNull(catalog) && !catalog.isEmpty())
                    // dfs
                    script = String.format("schema(loadTable(\"%s\", `%s)).colDefs;", catalog, tableNamePattern);
                else
                    // mem
                    script = String.format("schema(%s).colDefs;", tableNamePattern);
                BasicTable tempColDefs = (BasicTable) connection.run(script);
                BasicStringVector tempNameColumn = (BasicStringVector) tempColDefs.getColumn(0);
                List<String> nameColumnList = Arrays.asList(tempNameColumn.getdataArray());
                int pos = nameColumnList.indexOf(columnNamePattern);
                List<Integer> ordinalPositionList =  new ArrayList<>();
                ordinalPositionList.add(pos + 1);
                colDefs.addColumn("ORDINAL_POSITION", new BasicIntVector(ordinalPositionList));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // get all cols.
            BasicIntVector posColVector = new BasicIntVector(IntStream.rangeClosed(1, colDefs.getColumn(0).rows())
                    .boxed()
                    .collect(Collectors.toList()));
            colDefs.addColumn("ORDINAL_POSITION", posColVector);
        }

        // transfer 'DATA_TYPE' to java.sql.Types
        try {
            BasicStringVector typeStringColumn = (BasicStringVector) colDefs.getColumn(1);
            BasicIntVector typeIntColumn = (BasicIntVector) colDefs.getColumn(2);
            for (int i = 0; i < typeStringColumn.rows(); i ++)
                typeIntColumn.set(i, new BasicInt(Utils.transferColDefsTypesToSqlTypes(typeStringColumn.get(i).getString())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // set 'SQL_DATA_TYPES'
        BasicStringVector typeStringColumn = (BasicStringVector) colDefs.getColumn(1);
        List<Integer> sqlDataTypesList = Arrays.stream(typeStringColumn.getdataArray()).map(Utils::transferColDefsTypesToSqlTypes).collect(Collectors.toList());
        BasicIntVector sqlDataTypesColumn = new BasicIntVector(sqlDataTypesList);
        colDefs.addColumn("SQL_DATA_TYPES", sqlDataTypesColumn);
    }



    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
        return null;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0;
    }


    @Override
    public String getDatabaseProductName() {
        return DATABASE_NAME;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return 0;
    }

    @Override
    public int getDriverMajorVersion() {
        return Driver.V;
    }

    @Override
    public int getDriverMinorVersion() {
        return Driver.v;
    }

    @Override
    public String getDriverName() {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() {
        return DRIVER_VERSION;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        return null;
    }

    @Override
    public String getExtraNameCharacters() {
        return "@";
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) {
        return null;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "`";
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
        return null;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public int getResultSetHoldability() {
//    	return 0;
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateXOpen;
    }

    @Override
    public long getMaxLogicalLobSize() {
        return 0;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        return null;
    }

    @Override
    public ResultSet getSchemas() throws SQLException{
        int index = 0;
        try {
            List<String> colNames = Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
            List<Vector> cols = new ArrayList<>();
            String db = connection.getDatabase();
            if (db == null){
                List<String> table = new ArrayList<>();
                List<String> database = new ArrayList<>();
                Vector dbs = (BasicStringVector) connection.run("getClusterDFSDatabases()");
                for (index = 0; index<dbs.rows(); index++){
                    BasicTable tb = (BasicTable) connection.run("listTables(\"" + dbs.getString(index) + "\")");
                    Vector tbs = tb.getColumn("tableName");
                    for (int j = 0; j < tbs.rows() ;j++){
                        table.add(tbs.getString(j));
                        database.add(dbs.getString(index));
                    }
                }
                Vector tables = new BasicStringVector(table);
                Vector databases = new BasicStringVector(database);
                cols.add(tables);
                cols.add(databases);
            }else {
                List<String> database = new ArrayList<>();
                database.add(db);
                Vector databases = new BasicStringVector(database);
                BasicTable table = (BasicTable)connection.run("listTables(" + db + ")");
                Vector tables = table.getColumn("tableName");
                cols.add(tables);
                cols.add(databases);
            }
            BasicTable basicTable = new BasicTable(colNames, cols);
            Schemas =  new JDBCResultSet(connection,statement,basicTable,"");
        }catch (IOException e){
            throw new SQLException(e.toString());
        }
        return Schemas;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        BasicTable colDefs;
        List<String> colNames = new ArrayList<>(Arrays.asList("TABLE_CAT", "TABLE_NAME", "TABLE_SCHEM", "TABLE_TYPE", "REMARKS"));
        List<Vector> cols = new ArrayList<>();

        if (Objects.nonNull(catalog) && !catalog.isEmpty()) {
            try {
                if (Utils.isNotEmpty(tableNamePattern) && !tableNamePattern.matches("%+")) {
                    // specify tableName for dfs table
                    String dfsTableHandle = "handle=loadTable(\"" + catalog + "\", `" + tableNamePattern + "); handle;";
                    connection.run(dfsTableHandle);
                    String[] values = {catalog, tableNamePattern, null, "TABLE", null};
                    for (String value : values)
                        cols.add(new BasicStringVector(new String[]{value}));
                } else {
                    // not specify tableName for dfs table
                    String script = "handle=database(\"" + catalog + "\"); getTables(handle);";
                    BasicStringVector vector = (BasicStringVector) connection.run(script);
                    List<String[]> valuesList = new ArrayList<>();
                    for (int i = 0; i < vector.rows(); i++) {
                        String tableName = vector.getString(i);
                        String dfsTableHandle = "handle=loadTable(\"" + catalog + "\", `" + tableName + "); handle;";
                        connection.run(dfsTableHandle);

                        String[] values = {catalog, tableName, null, "TABLE", null};
                        valuesList.add(values);
                    }

                    for (int j = 0; j < valuesList.get(0).length; j++) {
                        List<String> columnValues = new ArrayList<>();
                        for (String[] values : valuesList)
                            columnValues.add(values[j]);

                        cols.add(new BasicStringVector(columnValues));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (tableNamePattern.matches("%+")) {
            // get all tables of catalog's.
            try {
                // get all dfs table
                String script = "getClusterDFSTables();";
                BasicStringVector allTables = (BasicStringVector) connection.run(script);

                List<String> tableCatVal = new ArrayList<>();
                List<String> tableNameVal = new ArrayList<>();
                List<String> tableSchemVal = new ArrayList<>();
                List<String> tableTypeVal = new ArrayList<>();
                List<String> remarksVal = new ArrayList<>();

                for (int i = 0; i < allTables.rows(); i ++) {
                    String tempDbAndTableName = allTables.getString(i);
                    // tempTableName
                    int lastSlashIndex = tempDbAndTableName.lastIndexOf("/");
                    if (lastSlashIndex != -1) {
                        String dbName = tempDbAndTableName.substring(0, lastSlashIndex);
                        String tempTableName = tempDbAndTableName.substring(lastSlashIndex + 1);
                        String dfsTableHandle = "handle=loadTable(\"" + dbName + "\", `" + tempTableName + "); handle;";
                        connection.run(dfsTableHandle);

                        tableCatVal.add(dbName);
                        tableNameVal.add(tempTableName);
                        tableSchemVal.add(null);
                        tableTypeVal.add("TABLE");
                        remarksVal.add(null);
                    }
                }

                // get all mem table
                BasicTable memTables = (BasicTable) connection.run("objs(true)");
                BasicStringVector name = (BasicStringVector) memTables.getColumn("name");
                if (Objects.nonNull(name)) {
                    for (int i = 0; i < name.rows(); i ++) {
                        BasicString memTableName = (BasicString) name.get(i);
                        tableNameVal.add(memTableName.getString());
                        tableCatVal.add(null);
                        tableSchemVal.add(null);
                        remarksVal.add(null);
                    }
                }

                BasicSymbolVector form = (BasicSymbolVector) memTables.getColumn("form");
                if (Objects.nonNull(form)) {
                    for (int i = 0; i < form.rows(); i ++) {
                        BasicString memForm = (BasicString) form.get(i);
                        tableTypeVal.add(memForm.getString());
                    }
                }

                Stream.of(tableCatVal, tableNameVal, tableSchemVal, tableTypeVal, remarksVal)
                        .map(BasicStringVector::new)
                        .forEach(cols::add);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // memory table
            try {
                BasicTable memTable = (BasicTable) connection.run(tableNamePattern + ";");
                Stream.of(new String[]{null}, new String[]{tableNamePattern}, new String[]{null}, new String[]{"TABLE"}, new String[]{null})
                        .map(BasicStringVector::new)
                        .forEach(cols::add);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        colDefs = new BasicTable(colNames, cols);
        return new JDBCResultSet(connection,statement, colDefs,"");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException{
        try {
            String[] tableTypes = new String[]{"IN-MEMORY TABLE","SEGMENTED TABLE"};
            BasicStringVector basicStringVector = new BasicStringVector(tableTypes);
            List<String> colNames = Collections.singletonList("TABLE_TYPE");
            List<Vector> cols = Collections.singletonList(basicStringVector);
            BasicTable basicTable = new BasicTable(colNames,cols);
            return new JDBCResultSet(connection,statement,basicTable,"");
        }catch (Exception e){
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException{
        if(TypeInfo == null) {
            List<String> colName = Arrays.asList("TYPE_NAME","SQL_DATA_TYPE","BYTES");
            String[] typeNameArr = new String[]{"VOID", "BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH", "TIME", "MINUTE", "SECOND", "DATETIME", "TIMESTAMP", "NANOTIME", "NANOTIMESTAMP", "FLOAT", "DOUBLE", "SYMBOL", "STRING", "ANY"};
            int[] sqlDateTypeArr = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 24};
            int[] bytesArr = new int[]{1, 1, 1, 2, 4, 8, 4, 4, 4, 4, 4, 4, 8, 8, 8, 4, 8, 4, 0, 0};
            BasicStringVector typeName = new BasicStringVector(typeNameArr);
            BasicIntVector sqlDateType = new BasicIntVector(sqlDateTypeArr);
            BasicIntVector bytes = new BasicIntVector(bytesArr);
            List<Vector> cols = Arrays.asList(typeName, sqlDateType, bytes);
            BasicTable table = new BasicTable(colName, cols);
            TypeInfo = new JDBCResultSet(connection, statement, table, "");
        }
        return TypeInfo;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        return null;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return null;
    }

    @Override
    public String getDatabaseProductVersion() {
        return DRIVER_VERSION + Driver.SYSTEM_PROPS.getProperty("os.name") + Driver.SYSTEM_PROPS.getProperty("os.version");
    }

    @Override
    public String getNumericFunctions() {
        return "abs,acos,acosh,add,asin,asinh,atan,atanh,cbrt,ceil,cos,cosh,deg2rad,div,exp,exp2,expm1,floor,log,log2,log10,lshift,mod,mul,neg,pow,prod,ratio,reciprocal,rshift,round,sin,sinh,sqrt,square,sub,tan,tanh";
    }

    @Override
    public String getProcedureTerm() {
        return "def";
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getSQLKeywords() {
        return "context,pivot";
    }

    @Override
    public String getStringFunctions() {
        return "charAt,concat,convertEncode,crc32,decimalFormat,endsWith,format,fromUTF8,hex,ilike,isAlNum,isAlpha,isDecimal,isDigit,isLower,isNumeric,isSpace,isTitle,isUpper,left,like,lower,lpad,ltrim,md5,regexCount,regexFind,regexReplace,repeat,right,rpad,rtrim,split,startsWith,strlen,strlenu,strip,strpos,strReplace,substr,substru,toUTF8,trim,upper,wc";
    }

    @Override
    public String getSystemFunctions() {
        return "backup,defs,free,getActiveMaster,getBackupMeta,getOS,getOSBit,license,getBackupList,loadBackup,login,objs,mem,now,restore,shell,syntax";
    }

    @Override
    public String getTimeDateFunctions() {
        return "convertTZ,date,datetime,datetimeParse,gmtime,hour,localtime,minute,month,monthStart,monthEnd,nanotime,nanotimestamp,second,temporalAdd,temporalParse,time,timestamp,year,weekday";
    }

    @Override
    public String getURL() {
        return connection.getUrl();
    }

    @Override
    public String getUserName() {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
