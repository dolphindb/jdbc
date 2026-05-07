package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        BasicStringVector allCatalogStringVector;
        try {
            if (connection.isCatalogSupported()) {
                allCatalogStringVector = (BasicStringVector) connection.run("getAllCatalogs()");
            } else {
                allCatalogStringVector = new BasicStringVector(new String[]{DATABASE_NAME});
            }
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
        List<ColumnSource> columnSources = resolveColumnSources(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        BasicTable colDefs = buildColumnsTable(columnSources, columnNamePattern);

        return new JDBCResultSet(connection,statement, colDefs,"");
    }

    private List<ColumnSource> resolveColumnSources(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        if (isSpecifiedPattern(catalog) && isSpecifiedPattern(schemaPattern)
                && Utils.isNotEmpty(tableNamePattern) && isWildcardPattern(columnNamePattern)) {
            return resolveDfsColumnSources(catalog, schemaPattern, tableNamePattern);
        } else if (Utils.isEmpty(catalog) && Utils.isEmpty(schemaPattern) && isSpecifiedPattern(tableNamePattern)) {
            return resolveMemoryColumnSources(tableNamePattern);
        }

        return Collections.emptyList();
    }

    private boolean isSpecifiedPattern(String value) {
        return Utils.isNotEmpty(value) && !"%".equals(value);
    }

    private boolean isWildcardPattern(String value) {
        return value != null && "%".equals(value);
    }

    private boolean isTrimmedWildcardPattern(String value) {
        return value != null && "%".equals(value.trim());
    }

    private String unescapeMetadataIdentifier(String value) {
        if (value == null || value.indexOf("\\_") < 0) {
            return value;
        }

        StringBuilder unescaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '\\' && i + 1 < value.length() && value.charAt(i + 1) == '_') {
                unescaped.append('_');
                i++;
            } else {
                unescaped.append(ch);
            }
        }
        return unescaped.toString();
    }

    private List<ColumnSource> resolveDfsColumnSources(String catalog, String schemaPattern, String tableNamePattern) {
        try {
            if (connection.isCatalogSupported()) {
                return resolveCatalogColumnSources(catalog, schemaPattern, tableNamePattern);
            } else {
                return resolveLegacyColumnSources(catalog, schemaPattern, tableNamePattern);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ColumnSource> resolveCatalogColumnSources(String catalog, String schemaPattern, String tableNamePattern) throws IOException {
        if (!((BasicBoolean) connection.run("existsCatalog(\"" + catalog + "\")")).getBoolean())
            throw new RuntimeException("The catalog '" + catalog + "' doesn't exist.");

        BasicTable schemas = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\")");
        String schemaName = unescapeMetadataIdentifier(schemaPattern);
        int pos = findSchemaPosition(schemas, schemaPattern);
        if (pos == -1) {
            throw new RuntimeException("Schema '" + schemaName + "' doesn't exist in catalog '" + catalog + "'.");
        }

        BasicStringVector dbUrlVector = (BasicStringVector) schemas.getColumn("dbUrl");
        BasicStringVector schemaVector = (BasicStringVector) schemas.getColumn("schema");
        String dbUrl = dbUrlVector.getString(pos);
        schemaName = schemaVector.getString(pos);
        if (isTrimmedWildcardPattern(tableNamePattern)) {
            return loadAllDfsColumnSources(catalog, schemaName, dbUrl);
        }
        String tableName = unescapeMetadataIdentifier(tableNamePattern);
        return Collections.singletonList(loadDfsColumnSource(catalog, schemaName, dbUrl, tableName));
    }

    private List<ColumnSource> resolveLegacyColumnSources(String catalog, String schemaPattern, String tableNamePattern) throws IOException {
        String schemaName = unescapeMetadataIdentifier(schemaPattern);
        BasicBoolean schemaExists = (BasicBoolean) connection.run("in (\"" + schemaName + "\", substr(distinct(getClusterDFSTables().regexReplace(\"/[^/]*$\",\"\")), 6))");
        if (!schemaExists.getBoolean()) {
            throw new RuntimeException("The database '" + schemaName + "' doesn't exist.");
        }

        String dbUrl = "dfs://" + schemaName;
        if (isTrimmedWildcardPattern(tableNamePattern)) {
            return loadAllDfsColumnSources(catalog, schemaName, dbUrl);
        }
        String tableName = unescapeMetadataIdentifier(tableNamePattern);
        return Collections.singletonList(loadDfsColumnSource(catalog, schemaName, dbUrl, tableName));
    }

    private List<ColumnSource> loadAllDfsColumnSources(String catalog, String schemaName, String dbUrl) throws IOException {
        List<ColumnSource> columnSources = new ArrayList<>();
        AbstractVector tableNameVec = (AbstractVector) connection.run("getTables(database(\"" + dbUrl + "\"))");
        for (int i = 0; i < tableNameVec.rows(); i++) {
            String tableName = tableNameVec.getString(i);
            columnSources.add(loadDfsColumnSource(catalog, schemaName, dbUrl, tableName));
        }
        return columnSources;
    }

    private ColumnSource loadDfsColumnSource(String catalog, String schemaName, String dbUrl, String tableName) throws IOException {
        String script = "loadTable(\"" + dbUrl + "\", `" + tableName + ").schema();";
        BasicDictionary schema = (BasicDictionary) connection.run(script);
        return new ColumnSource(catalog, schemaName, tableName, schema);
    }

    private List<ColumnSource> resolveMemoryColumnSources(String tableNamePattern) {
        try {
            String tableName = unescapeMetadataIdentifier(tableNamePattern);
            BasicDictionary schema = (BasicDictionary) connection.run("schema(" + tableName + ");");
            return Collections.singletonList(new ColumnSource(null, null, tableName, schema));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BasicTable buildColumnsTable(List<ColumnSource> columnSources, String columnNamePattern) {
        BasicTable colDefs = null;

        for (int i = 0; i < columnSources.size(); i++) {
            BasicTable curTable = buildColumnTable(columnSources.get(i), columnNamePattern);

            if (i == 0)
                colDefs = curTable;
            else
                colDefs = colDefs.combine(curTable);
        }

        return colDefs;
    }

    private BasicTable buildColumnTable(ColumnSource columnSource, String columnNamePattern) {
        List<String> partitionColumnNames = getPartitionColumnNames(columnSource.schema);
        BasicTable colDefs = columnSource.colDefs;
        BasicTable curTable = buildColumnIdentityTable(columnSource, colDefs.rows());

        renameColumnDefinitionColumns(colDefs);
        appendColumnDefinitionColumns(curTable, colDefs);
        applyDisplayTypeNames(curTable);
        appendPrecisionAndScaleColumns(curTable, columnSource.extra);
        appendNullableColumn(curTable, partitionColumnNames);
        appendAutoIncrementColumn(curTable);
        appendOrdinalPositionColumn(curTable, columnSource, columnNamePattern);
        convertDataTypeColumn(curTable);
        appendSqlDataTypesColumn(curTable);
        return curTable;
    }

    private List<String> getPartitionColumnNames(BasicDictionary schema) {
        List<String> partitionColumnNames = new ArrayList<>();
        Entity columnNameEntity = schema.get("partitionColumnName");

        if (Objects.nonNull(columnNameEntity)) {
            if (columnNameEntity.isScalar()) {
                BasicString columnName = (BasicString) columnNameEntity;
                partitionColumnNames.add(columnName.getString());
            } else if (columnNameEntity.isVector()) {
                AbstractVector columnNameVec = (AbstractVector) columnNameEntity;
                if (columnNameVec instanceof BasicStringVector)
                    partitionColumnNames.addAll(Arrays.asList(((BasicStringVector) columnNameVec).getdataArray()));
            }
        }
        return partitionColumnNames;
    }

    private BasicTable buildColumnIdentityTable(ColumnSource columnSource, int rowCount) {
        List<String> columnNames = new ArrayList<>();
        List<Vector> columns = new ArrayList<>();
        columnNames.add("TABLE_CAT");
        columnNames.add("TABLE_SCHEM");
        columnNames.add("TABLE_NAME");

        String catalogName = (Objects.nonNull(columnSource.catalog) && !columnSource.catalog.trim().equals("%")) ? columnSource.catalog : null;
        columns.add(new BasicStringVector(new ArrayList<>(Collections.nCopies(rowCount, catalogName))));

        String schemaName = (Objects.nonNull(columnSource.schemaName) && !columnSource.schemaName.trim().equals("%")) ? columnSource.schemaName : null;
        columns.add(new BasicStringVector(new ArrayList<>(Collections.nCopies(rowCount, schemaName))));

        columns.add(new BasicStringVector(new ArrayList<>(Collections.nCopies(rowCount, columnSource.tableName))));
        return new BasicTable(columnNames, columns);
    }

    private void renameColumnDefinitionColumns(BasicTable colDefs) {
        List<String> newColumnNames = new ArrayList<>();
        newColumnNames.add("COLUMN_NAME");
        newColumnNames.add("TYPE_NAME");
        newColumnNames.add("DATA_TYPE");
        newColumnNames.add("COLUMN_SIZE");
        newColumnNames.add("REMARKS");
        if (colDefs.columns() == 6)
            newColumnNames.add("sensitive");
        colDefs.setColName(newColumnNames);
    }

    private void appendColumnDefinitionColumns(BasicTable targetTable, BasicTable colDefs) {
        int addColumnsNum = colDefs.columns() == 6 ? colDefs.columns() -1 : colDefs.columns();
        for (int j = 0; j < addColumnsNum; j++)
            targetTable.addColumn(colDefs.getColumnName(j), colDefs.getColumn(j));
    }

    private void applyDisplayTypeNames(BasicTable curTable) {
        BasicStringVector typeNameColumn = (BasicStringVector) curTable.getColumn(4);
        for (int j = 0; j < typeNameColumn.rows(); j++) {
            typeNameColumn.setString(j, getDisplayTypeName(typeNameColumn.getString(j)));
        }
    }

    private void appendPrecisionAndScaleColumns(BasicTable curTable, BasicIntVector extraVec) {
        AbstractVector typeStringColumn = (AbstractVector) curTable.getColumn(4);
        BasicIntVector columnSize = (BasicIntVector) curTable.getColumn(6);
        BasicIntVector decimalDigits = new BasicIntVector(typeStringColumn.rows());
        for (int j = 0; j < typeStringColumn.rows(); j ++) {
            String dataType = getBaseTypeName(typeStringColumn.get(j).getString());
            Integer precision = getColumnSize(dataType);
            Integer scale = getDecimalDigits(dataType, extraVec, j);
            if (precision == null) {
                columnSize.setNull(j);
            } else {
                columnSize.setInt(j, precision);
            }
            if (scale == null) {
                decimalDigits.setNull(j);
            } else {
                decimalDigits.setInt(j, scale);
            }
        }

        curTable.addColumn("DECIMAL_DIGITS", decimalDigits);
    }

    private void appendNullableColumn(BasicTable curTable, List<String> partitionColumnNames) {
        List<String> isNullableStrList = new ArrayList<>();
        AbstractVector nameColumn = (AbstractVector) curTable.getColumn(3);
        if (nameColumn instanceof BasicStringVector) {
            Arrays.stream(((BasicStringVector) nameColumn).getdataArray())
                    .map(str -> partitionColumnNames.contains(str) ? "NO" : "YES")
                    .forEach(isNullableStrList::add);
            curTable.addColumn("IS_NULLABLE", new BasicStringVector(isNullableStrList));
        }
    }

    private void appendAutoIncrementColumn(BasicTable curTable) {
        List<String> autoIncrementList = new ArrayList<>(Collections.nCopies(curTable.rows(), ""));
        BasicStringVector autoIncrementVec = new BasicStringVector(autoIncrementList);
        curTable.addColumn("IS_AUTOINCREMENT", autoIncrementVec);
    }

    private void appendOrdinalPositionColumn(BasicTable curTable, ColumnSource columnSource, String columnNamePattern) {
        if (Objects.nonNull(columnNamePattern) && !columnNamePattern.isEmpty() && !columnNamePattern.equals("%")) {
            AbstractVector nameColumn = (AbstractVector) columnSource.colDefs.getColumn(0);
            if (nameColumn instanceof BasicStringVector) {
                List<String> nameColumnList = Arrays.asList(((BasicStringVector) nameColumn).getdataArray());
                int pos = nameColumnList.indexOf(columnNamePattern);
                List<Integer> ordinalPositionList =  new ArrayList<>();
                ordinalPositionList.add(pos + 1);
                curTable.addColumn("ORDINAL_POSITION", new BasicIntVector(ordinalPositionList));
            }
        } else {
            BasicIntVector posColVector = new BasicIntVector(IntStream.rangeClosed(1, curTable.getColumn(0).rows())
                    .boxed()
                    .collect(Collectors.toList()));
            curTable.addColumn("ORDINAL_POSITION", posColVector);
        }
    }

    private void convertDataTypeColumn(BasicTable curTable) {
        AbstractVector typeStringColumn = (AbstractVector) curTable.getColumn(4);
        BasicIntVector typeIntColumn = (BasicIntVector) curTable.getColumn(5);
        for (int j = 0; j < typeStringColumn.rows(); j ++)
            typeIntColumn.setInt(j, Utils.transferColDefsTypesToSqlTypes(typeStringColumn.get(j).getString()));
    }

    private void appendSqlDataTypesColumn(BasicTable curTable) {
        AbstractVector typeStringColumn = (AbstractVector) curTable.getColumn(4);
        List<Integer> sqlDataTypesList = null;
        if (typeStringColumn instanceof BasicStringVector)
            sqlDataTypesList = Arrays.stream(((BasicStringVector) typeStringColumn).getdataArray()).map(Utils::transferColDefsTypesToSqlTypes).collect(Collectors.toList());
        BasicIntVector sqlDataTypesColumn = new BasicIntVector(sqlDataTypesList);
        curTable.addColumn("SQL_DATA_TYPES", sqlDataTypesColumn);
    }

    private static class ColumnSource {
        final String catalog;
        final String schemaName;
        final String tableName;
        final BasicDictionary schema;
        final BasicTable colDefs;
        final BasicIntVector extra;

        ColumnSource(String catalog, String schemaName, String tableName, BasicDictionary schema) {
            this.catalog = catalog;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.schema = schema;
            this.colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
            this.extra = (BasicIntVector) colDefs.getColumn("extra");
        }
    }

    private String getBaseTypeName(String typeName) {
        if (typeName == null) {
            return "";
        }
        return typeName.replaceAll("\\(.*?\\)", "").replaceAll("\\[\\]$", "");
    }

    private String getDisplayTypeName(String typeName) {
        if (typeName == null) {
            return null;
        }

        boolean isArray = typeName.endsWith("[]");
        String baseTypeName = getBaseTypeName(typeName);
        String displayTypeName;
        switch (baseTypeName) {
            case "TIME":
                displayTypeName = "TIME(3)";
                break;
            case "TIMESTAMP":
                displayTypeName = "TIMESTAMP(3)";
                break;
            case "NANOTIME":
                displayTypeName = "NANOTIME(9)";
                break;
            case "NANOTIMESTAMP":
                displayTypeName = "NANOTIMESTAMP(9)";
                break;
            default:
                return typeName;
        }
        return isArray ? displayTypeName + "[]" : displayTypeName;
    }

    private Integer getColumnSize(String dataType) {
        switch (dataType) {
            case "TIME":
            case "TIMESTAMP":
                return 3;
            case "NANOTIME":
            case "NANOTIMESTAMP":
                return 9;
            case "DECIMAL32":
                return 9;
            case "DECIMAL64":
                return 18;
            case "DECIMAL128":
                return 38;
            case "BOOL":
            case "CHAR":
            case "SHORT":
            case "INT":
            case "LONG":
            case "DATE":
            case "MONTH":
            case "MINUTE":
            case "SECOND":
            case "DATETIME":
            case "FLOAT":
            case "DOUBLE":
            case "SYMBOL":
            case "STRING":
            case "UUID":
            case "DATEHOUR":
            case "IPADDR":
            case "INT128":
            case "BLOB":
            case "COMPLEX":
            case "POINT":
            case "ANY":
                return -1;
            default:
                return null;
        }
    }

    private Integer getDecimalDigits(String dataType, BasicIntVector extraVec, int row) {
        switch (dataType) {
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                return extraVec == null || extraVec.isNull(row) ? null : extraVec.getInt(row);
            case "TIME":
            case "TIMESTAMP":
            case "NANOTIME":
            case "NANOTIMESTAMP":
            case "BOOL":
            case "CHAR":
            case "SHORT":
            case "INT":
            case "LONG":
            case "DATE":
            case "MONTH":
            case "MINUTE":
            case "SECOND":
            case "DATETIME":
            case "FLOAT":
            case "DOUBLE":
            case "SYMBOL":
            case "STRING":
            case "UUID":
            case "DATEHOUR":
            case "IPADDR":
            case "INT128":
            case "BLOB":
            case "COMPLEX":
            case "POINT":
            case "ANY":
            default:
                return null;
        }
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
        try {
            List<SchemaRef> schemaRefs = getAllSchemaRefs();
            Schemas = new JDBCResultSet(connection, statement, buildSchemasTable(schemaRefs),"");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Schemas;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern)  throws SQLException {
        if (schemaPattern == null) {
            schemaPattern = "%";
        }

        if (catalog == null && "%".equals(schemaPattern)) {
            return getSchemas();
        }

        if (Utils.isNotEmpty(catalog) && "%".equals(schemaPattern)) {
            try {
                Schemas = new JDBCResultSet(connection, statement, buildSchemasTable(getSchemaRefsForCatalog(catalog)),"");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException("Illegal param in getSchemas");
        }

        return Schemas;
    }

    private List<SchemaRef> getAllSchemaRefs() throws IOException {
        if (connection.isCatalogSupported()) {
            BasicStringVector catalogsVec = (BasicStringVector) connection.run("getAllCatalogs();");
            if (catalogsVec.rows() == 0) {
                return Collections.emptyList();
            }

            List<SchemaRef> schemaRefs = new ArrayList<>();
            for (int i = 0; i < catalogsVec.rows(); i ++) {
                schemaRefs.addAll(getCatalogSchemaRefs(catalogsVec.getString(i)));
            }
            return schemaRefs;
        }

        return getLegacySchemaRefs();
    }

    private List<SchemaRef> getSchemaRefsForCatalog(String catalog) throws IOException {
        if (connection.isCatalogSupported()) {
            return getCatalogSchemaRefs(catalog);
        }

        if (DATABASE_NAME.equals(catalog)) {
            return getLegacySchemaRefs();
        }

        throw new IllegalArgumentException("Catalog must be \"DolphinDB\" and schemaPattern must be \"%\".");
    }

    private List<SchemaRef> getCatalogSchemaRefs(String catalog) throws IOException {
        List<SchemaRef> schemaRefs = new ArrayList<>();
        BasicTable schemasMapTb = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\");");
        BasicStringVector schemaVec = (BasicStringVector) schemasMapTb.getColumn("schema");
        for (int i = 0; i < schemaVec.rows(); i ++) {
            schemaRefs.add(new SchemaRef(schemaVec.getString(i), catalog));
        }
        return schemaRefs;
    }

    private List<SchemaRef> getLegacySchemaRefs() throws IOException {
        List<SchemaRef> schemaRefs = new ArrayList<>();
        BasicStringVector schemaVec = (BasicStringVector) connection.run("substr(distinct(getClusterDFSTables().regexReplace(\"/[^/]*$\",\"\")), 6)");
        for (int i = 0; i < schemaVec.rows(); i ++) {
            schemaRefs.add(new SchemaRef(schemaVec.getString(i), DATABASE_NAME));
        }
        return schemaRefs;
    }

    private BasicTable buildSchemasTable(List<SchemaRef> schemaRefs) {
        List<String> schemaVal = new ArrayList<>();
        List<String> catalogVal = new ArrayList<>();
        for (SchemaRef schemaRef : schemaRefs) {
            schemaVal.add(schemaRef.schema);
            catalogVal.add(schemaRef.catalog);
        }

        List<String> colNames = Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
        List<Vector> cols = Arrays.asList(new BasicStringVector(schemaVal), new BasicStringVector(catalogVal));
        return new BasicTable(colNames, cols);
    }

    private static class SchemaRef {
        final String schema;
        final String catalog;

        SchemaRef(String schema, String catalog) {
            this.schema = schema;
            this.catalog = catalog;
        }
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
        List<TableRef> tableRefs = resolveTableRefs(catalog, schemaPattern, tableNamePattern);
        BasicTable colDefs = buildTablesTable(tableRefs);
        return new JDBCResultSet(connection, statement, colDefs,"");
    }

    private List<TableRef> resolveTableRefs(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        if (Utils.isNotEmpty(catalog) && !catalog.trim().equals("%")) {
            if (Utils.isNotEmpty(schemaPattern) && !schemaPattern.trim().equals("%")) {
                return getTablesWithCatalogAndSchema(catalog, schemaPattern);
            } else if (Objects.isNull(schemaPattern) || schemaPattern.trim().equals("%")) {
                return getTablesWithCatalogOnly(catalog);
            }
        } else if (Utils.isEmpty(catalog) && Utils.isEmpty(schemaPattern) && isPercent(tableNamePattern)) {
            return getAllMemoryTables();
        } else if (isPercent(catalog)) {
            throw new IllegalArgumentException("Invalid params in getTables, not support get all tables with no specific catalog and schema.");
        }

        throw new IllegalArgumentException("Invalid params in getTables.");
    }

    private boolean isPercent(String value) {
        return Objects.nonNull(value) && value.equals("%");
    }

    private List<TableRef> getTablesWithCatalogAndSchema(String catalog, String schemaPattern) throws SQLException {
        List<TableRef> tableRefs = new ArrayList<>();

        try {
            if (connection.isCatalogSupported()) {
                BasicTable schemas = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\")");
                if (schemas.rows() != 0) {
                    String schemaName = unescapeMetadataIdentifier(schemaPattern);
                    int pos = findSchemaPosition(schemas, schemaPattern);

                    if (pos != -1) {
                        BasicStringVector dbUrlVector = (BasicStringVector) schemas.getColumn("dbUrl");
                        BasicStringVector schemaVector = (BasicStringVector) schemas.getColumn("schema");
                        String dbUrl = dbUrlVector.getString(pos);
                        appendCatalogDfsTables(tableRefs, catalog, schemaVector.getString(pos), dbUrl);
                    } else {
                        throw new RuntimeException("Schema '" + schemaName + "' doesn't exist in catalog '" + catalog + "'.");
                    }
                } else {
                    throw new RuntimeException("Current catalog '" + catalog + "' doesn't have any schema.");
                }
            } else if (DATABASE_NAME.equals(catalog)) {
                String schemaName = unescapeMetadataIdentifier(schemaPattern);
                BasicBoolean schemaExists = (BasicBoolean) connection.run("in (\"" + schemaName + "\", substr(distinct(getClusterDFSTables().regexReplace(\"/[^/]*$\",\"\")), 6))");
                if (!schemaExists.getBoolean()) {
                    throw new SQLException("The database " + schemaName + " does not exist or contains no tables.");
                }

                appendLegacyDfsTables(tableRefs, schemaName);
            } else {
                throw new IllegalArgumentException("Catalog must be \"DolphinDB\" and schemaPattern must be a valid database name.");
            }
            return tableRefs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<TableRef> getTablesWithCatalogOnly(String catalog) throws SQLException {
        List<TableRef> tableRefs = new ArrayList<>();

        try {
            if (connection.isCatalogSupported()) {
                BasicTable schemas = (BasicTable) connection.run("getSchemaByCatalog(\"" + catalog + "\")");
                if (schemas.rows() != 0) {
                    BasicStringVector schemaVector = (BasicStringVector) schemas.getColumn("schema");
                    BasicStringVector dbUrlVector = (BasicStringVector) schemas.getColumn("dbUrl");
                    for (int i = 0; i < dbUrlVector.rows(); i ++) {
                        String dbUrl = dbUrlVector.getString(i);
                        appendCatalogDfsTables(tableRefs, catalog, schemaVector.getString(i), dbUrl);
                    }
                } else {
                    throw new RuntimeException("Current catalog '" + catalog + "' doesn't have any schema.");
                }
            } else if (DATABASE_NAME.equals(catalog)) {
                BasicStringVector databases = (BasicStringVector) connection.run("substr(distinct(getClusterDFSTables().regexReplace(\"/[^/]*$\",\"\")), 6)");
                for (int i = 0; i < databases.rows(); i++) {
                    appendLegacyDfsTables(tableRefs, databases.getString(i));
                }
            } else {
                throw new IllegalArgumentException("Catalog must be \"DolphinDB\" for old version servers.");
            }
            return tableRefs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int findSchemaPosition(BasicTable schemas, String schemaPattern) {
        String schemaName = unescapeMetadataIdentifier(schemaPattern);
        BasicStringVector schemaVector = (BasicStringVector) schemas.getColumn("schema");
        for (int i = 0; i < schemas.rows(); i++) {
            if (schemaVector.getString(i).equals(schemaName))
                return i;
        }
        return -1;
    }

    private void appendCatalogDfsTables(List<TableRef> tableRefs, String catalog, String schema, String dbUrl) throws IOException {
        String script = "handle=database(\"" + dbUrl + "\"); getTables(handle);";
        AbstractVector tableNameVec = (AbstractVector) connection.run(script);
        appendTableRefs(tableRefs, catalog, schema, tableNameVec);
    }

    private void appendLegacyDfsTables(List<TableRef> tableRefs, String schema) throws IOException {
        BasicStringVector tableNameVec = (BasicStringVector) connection.run("getTables(database(\"dfs://" + schema + "\"))");
        appendTableRefs(tableRefs, DATABASE_NAME, schema, tableNameVec);
    }

    private void appendTableRefs(List<TableRef> tableRefs, String catalog, String schema, AbstractVector tableNameVec) {
        for (int i = 0; i < tableNameVec.rows(); i++) {
            tableRefs.add(new TableRef(catalog, schema, tableNameVec.getString(i), "TABLE", null));
        }
    }

    private List<TableRef> getAllMemoryTables() throws SQLException {
        List<TableRef> tableRefs = new ArrayList<>();

        try {
            BasicTable memTables = (BasicTable) connection.run("select * from objs(true) where form =\"TABLE\";");
            AbstractVector name = (AbstractVector) memTables.getColumn("name");
            AbstractVector form = (AbstractVector) memTables.getColumn("form");
            if (Objects.nonNull(name) && Objects.nonNull(form)) {
                for (int i = 0; i < name.rows(); i ++) {
                    BasicString memTableName = (BasicString) name.get(i);
                    BasicString memForm = (BasicString) form.get(i);
                    tableRefs.add(new TableRef(null, null, memTableName.getString(), memForm.getString(), null));
                }
            }

            return tableRefs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BasicTable buildTablesTable(List<TableRef> tableRefs) {
        List<String> tableCatVal = new ArrayList<>();
        List<String> tableSchemVal = new ArrayList<>();
        List<String> tableNameVal = new ArrayList<>();
        List<String> tableTypeVal = new ArrayList<>();
        List<String> remarksVal = new ArrayList<>();

        for (TableRef tableRef : tableRefs) {
            tableCatVal.add(tableRef.catalog);
            tableSchemVal.add(tableRef.schema);
            tableNameVal.add(tableRef.tableName);
            tableTypeVal.add(tableRef.tableType);
            remarksVal.add(tableRef.remarks);
        }

        List<String> colNames = new ArrayList<>(Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"));
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicStringVector(tableCatVal));
        cols.add(new BasicStringVector(tableSchemVal));
        cols.add(new BasicStringVector(tableNameVal));
        cols.add(new BasicStringVector(tableTypeVal));
        cols.add(new BasicStringVector(remarksVal));
        return new BasicTable(colNames, cols);
    }

    private static class TableRef {
        final String catalog;
        final String schema;
        final String tableName;
        final String tableType;
        final String remarks;

        TableRef(String catalog, String schema, String tableName, String tableType, String remarks) {
            this.catalog = catalog;
            this.schema = schema;
            this.tableName = tableName;
            this.tableType = tableType;
            this.remarks = remarks;
        }
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
        return true;
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
