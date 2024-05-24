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
            BasicStringVector allCatalogStringVector = (BasicStringVector) connection.run("getAllCatalogs()");
            cols.add(allCatalogStringVector);
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
        if (!Utils.checkServerVersionIfSupportCatalog(connection))
            return null;

        BasicTable colDefs = null;
        String dbUrl = null;
        colDefs = getColumnsOriginMetaData(catalog, schemaPattern, tableNamePattern, columnNamePattern, dbUrl);
        assembleColumnsMetaData(colDefs, catalog, dbUrl, tableNamePattern, columnNamePattern);

        return new JDBCResultSet(connection,statement, colDefs,"");
    }

    private BasicTable getColumnsOriginMetaData(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern, String dbUrl) {
        BasicTable colDefs = null;
        if (Utils.isNotEmpty(catalog) && !catalog.equals("%") && Utils.isNotEmpty(schemaPattern) && !schemaPattern.equals("%")
                && Utils.isNotEmpty(tableNamePattern) && Utils.isNotEmpty(columnNamePattern) && columnNamePattern.equals("%")) {
            try {
                connection.run(catalog + "." + schemaPattern + "." + tableNamePattern);
                BasicTable schemas = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\")");
                if (schemas.rows() != 0) {
                    int pos = -1;
                    BasicStringVector schemaVector = (BasicStringVector) schemas.getColumn("schema");
                    for (int i = 0; i < schemas.rows(); i++) {
                        if (schemaVector.getString(i).equals(schemaPattern))
                            pos = i;
                    }

                    if (pos != -1) {
                        BasicStringVector dbUrlVector = (BasicStringVector) schemas.getColumn("dbUrl");
                        dbUrl = dbUrlVector.getString(pos);

                        String script = "handle=loadTable(\"" + dbUrl + "\", `" + tableNamePattern + "); schema(handle);";
                        BasicDictionary schema = (BasicDictionary) connection.run(script);
                        colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                        if (colDefs.getColumn(0).rows() == 0)
                            throw new RuntimeException("The column: '" + columnNamePattern + "' doesn't exist in table: '" + tableNamePattern + "'.");
                    } else {
                        throw new RuntimeException("schema" + schemaPattern + "doesn't exist in " + catalog + ".");
                    }
                } else {
                    throw new RuntimeException("Current catalog " + catalog + " doesn't has any schema.");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (Utils.isEmpty(catalog) && Utils.isEmpty(schemaPattern) && Utils.isNotEmpty(tableNamePattern) && !tableNamePattern.equals("%")) {
            BasicDictionary schema = null;
            try {
                schema = (BasicDictionary) connection.run("schema(" + tableNamePattern + ");");
                colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return colDefs;
    }

    private void assembleColumnsMetaData(BasicTable colDefs, String catalog, String dbUrl, String tableNamePattern, String columnNamePattern) {
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
                    AbstractVector columnNameVec = (AbstractVector) columnNameEntity;
                    if (columnNameVec instanceof BasicStringVector)
                        columnIndexList.addAll(Arrays.asList(((BasicStringVector) columnNameVec).getdataArray()));
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
        AbstractVector nameColumn = (AbstractVector) colDefs.getColumn(0);
        String[] nameArr = null;
        if (nameColumn instanceof BasicStringVector) {
            nameArr = ((BasicStringVector) nameColumn).getdataArray();
            Arrays.stream(nameArr)
                    .map(str -> columnIndexList.contains(str) ? "NO" : "YES")
                    .forEach(isNullableStrList::add);
            colDefs.addColumn("IS_NULLABLE", new BasicStringVector(isNullableStrList));
        }

        // set 'ORDINAL_POSITION'
        if (Objects.nonNull(columnNamePattern) && !columnNamePattern.isEmpty() && !columnNamePattern.equals("%")) {
            // specify 'columnNamePattern'
            try {
                String script = null;
                if (Objects.nonNull(catalog) && !catalog.isEmpty())
                    // dfs
                    script = String.format("schema(loadTable(\"%s\", `%s)).colDefs;", dbUrl, tableNamePattern);
                else
                    // mem
                    script = String.format("schema(%s).colDefs;", tableNamePattern);
                BasicTable tempColDefs = (BasicTable) connection.run(script);
                AbstractVector tempNameColumn = (AbstractVector) tempColDefs.getColumn(0);
                List<String> nameColumnList = null;
                if (tempNameColumn instanceof BasicStringVector) {
                    nameColumnList = Arrays.asList(((BasicStringVector) tempNameColumn).getdataArray());
                    int pos = nameColumnList.indexOf(columnNamePattern);
                    List<Integer> ordinalPositionList =  new ArrayList<>();
                    ordinalPositionList.add(pos + 1);
                    colDefs.addColumn("ORDINAL_POSITION", new BasicIntVector(ordinalPositionList));
                }
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
            AbstractVector typeStringColumn = (AbstractVector) colDefs.getColumn(1);
            BasicIntVector typeIntColumn = (BasicIntVector) colDefs.getColumn(2);
            for (int i = 0; i < typeStringColumn.rows(); i ++)
                typeIntColumn.set(i, new BasicInt(Utils.transferColDefsTypesToSqlTypes(typeStringColumn.get(i).getString())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // set 'SQL_DATA_TYPES'
        AbstractVector typeStringColumn = (AbstractVector) colDefs.getColumn(1);
        List<Integer> sqlDataTypesList = null;
        if (typeStringColumn instanceof BasicStringVector)
            sqlDataTypesList = Arrays.stream(((BasicStringVector) typeStringColumn).getdataArray()).map(Utils::transferColDefsTypesToSqlTypes).collect(Collectors.toList());
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
        return " ";
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
        if (!Utils.checkServerVersionIfSupportCatalog(connection))
            return null;

        List<String> colNames = Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
        List<Vector> cols = new ArrayList<>();
        BasicStringVector schemaVec = new BasicStringVector(0);
        BasicStringVector catalogVec = new BasicStringVector(0);

        try {
            BasicStringVector catalogsVec = (BasicStringVector) connection.run("getAllCatalogs();");
            if (catalogsVec.rows() != 0) {
                for (int i = 0; i < catalogsVec.rows(); i ++) {
                    String curCatalog = catalogsVec.getString(i);
                    BasicTable schemasMapTb = (BasicTable) connection.run("getSchemaByCatalog(\"" + curCatalog + "\");");
                    BasicStringVector curSchemaVec = (BasicStringVector) schemasMapTb.getColumn("schema");
                    schemaVec.Append(curSchemaVec);
                    catalogVec.Append(new BasicStringVector(new ArrayList<>(Collections.nCopies(curSchemaVec.rows(), "\"" + curCatalog + "\""))));
                }

                cols.add(schemaVec);
                cols.add(catalogVec);
                Schemas = new JDBCResultSet(connection, statement, new BasicTable(colNames, cols),"");
            } else {
                Schemas = new JDBCResultSet(connection, statement, (Entity) null,"");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Schemas;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern)  throws SQLException {
        if (!Utils.checkServerVersionIfSupportCatalog(connection))
            return null;

        List<String> colNames = Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
        List<Vector> cols = new ArrayList<>();
        BasicStringVector schemaVec = new BasicStringVector(0);
        BasicStringVector catalogVec = new BasicStringVector(0);
        if (Utils.isNotEmpty(catalog) && schemaPattern.equals("%")) {
            try {
                BasicTable schemasMapTb = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\");");
                BasicStringVector curSchemaVec = (BasicStringVector) schemasMapTb.getColumn("schema");
                schemaVec.Append(curSchemaVec);
                catalogVec.Append(new BasicStringVector(new ArrayList<>(Collections.nCopies(curSchemaVec.rows(), "\"" + catalog + "\""))));
                cols.add(schemaVec);
                cols.add(catalogVec);
                Schemas = new JDBCResultSet(connection, statement, new BasicTable(colNames, cols),"");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException("Illegal param in getShemas");
        }

        return Schemas;
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
        if (!Utils.checkServerVersionIfSupportCatalog(connection))
            return null;

        BasicTable colDefs;
        List<String> colNames = new ArrayList<>(Arrays.asList("TABLE_CAT", "TABLE_NAME", "TABLE_SCHEM", "TABLE_TYPE", "REMARKS"));
        List<Vector> cols = new ArrayList<>();
        List<String> tableCatVal = new ArrayList<>();
        List<String> tableNameVal = new ArrayList<>();
        List<String> tableSchemVal = new ArrayList<>();
        List<String> tableTypeVal = new ArrayList<>();
        List<String> remarksVal = new ArrayList<>();

        if (!Utils.isEmpty(catalog) && !catalog.trim().equals("%")) {
            if (Utils.isNotEmpty(schemaPattern) && !schemaPattern.trim().equals("%")) {
                try {
                    BasicTable schemas = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\")");
                    if (schemas.rows() != 0) {
                        int pos = -1;
                        BasicStringVector schemaVector = (BasicStringVector) schemas.getColumn("schema");
                        for (int i = 0; i < schemas.rows(); i++) {
                            if (schemaVector.getString(i).equals(schemaPattern))
                                pos = i;
                        }

                        if (pos != -1) {
                            BasicStringVector dbUrlVector = (BasicStringVector) schemas.getColumn("dbUrl");
                            String dbUrl = dbUrlVector.getString(pos);
                            String script = "handle=database(\"" + dbUrl + "\"); getTables(handle);";
                            AbstractVector tableNameVec = (AbstractVector) connection.run(script);
                            for (int i = 0; i < tableNameVec.rows(); i++) {
                                tableCatVal.add(catalog);
                                tableNameVal.add(tableNameVec.getString(i));
                                tableSchemVal.add(schemaPattern);
                                tableTypeVal.add("TABLE");
                                remarksVal.add(null);
                            }
                        } else {
                            throw new RuntimeException("schema" + schemaPattern + "doesn't exist in " + catalog + ".");
                        }
                    } else {
                        throw new RuntimeException("Current catalog " + catalog + " doesn't has any schema.");
                    }

                    Stream.of(tableCatVal, tableNameVal, tableSchemVal, tableTypeVal, remarksVal)
                            .map(BasicStringVector::new)
                            .forEach(cols::add);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if (schemaPattern.trim().equals("%")) {
                try {
                    BasicTable schemas = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\")");
                    if (schemas.rows() != 0) {
                        BasicStringVector schemaVector = (BasicStringVector) schemas.getColumn("schema");
                        BasicStringVector dbUrlVector = (BasicStringVector) schemas.getColumn("dbUrl");
                        for (int i = 0; i < dbUrlVector.rows(); i ++) {
                            String dbUrl = dbUrlVector.getString(i);
                            String script = "handle=database(\"" + dbUrl + "\"); getTables(handle);";
                            AbstractVector tableNameVec = (AbstractVector) connection.run(script);
                            for (int j = 0; j < tableNameVec.rows(); j++) {
                                // 针对表维度组装数据
                                tableCatVal.add(catalog);
                                tableNameVal.add(tableNameVec.getString(j));
                                tableSchemVal.add(schemaVector.getString(i));
                                tableTypeVal.add("TABLE");
                                remarksVal.add(null);
                            }
                        }
                    } else {
                        throw new RuntimeException("Current catalog " + catalog + " doesn't has any schema.");
                    }

                    Stream.of(tableCatVal, tableNameVal, tableSchemVal, tableTypeVal, remarksVal)
                            .map(BasicStringVector::new)
                            .forEach(cols::add);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (Utils.isEmpty(catalog) && Utils.isEmpty(schemaPattern) && tableNamePattern.equals("%")) {
            try {
                // get all mem table
                BasicTable memTables = (BasicTable) connection.run("select * from objs(true) where form =\"TABLE\";");
                AbstractVector name = (AbstractVector) memTables.getColumn("name");
                if (Objects.nonNull(name)) {
                    for (int i = 0; i < name.rows(); i ++) {
                        BasicString memTableName = (BasicString) name.get(i);
                        tableNameVal.add(memTableName.getString());
                        tableCatVal.add(null);
                        tableSchemVal.add(null);
                        remarksVal.add(null);
                    }
                }

                AbstractVector form = (AbstractVector) memTables.getColumn("form");
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
        } else if (catalog.matches("%+")) {
            // todo get all tables; no means
            throw new IllegalArgumentException("Invalid params in getTables, not support get all tables with no specific catalog and schema.");
        } else {
            throw new IllegalArgumentException("Invalid params in getTables.");
        }

        colDefs = new BasicTable(colNames, cols);
        return new JDBCResultSet(connection, statement, colDefs,"");
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
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
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
        return true;
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
