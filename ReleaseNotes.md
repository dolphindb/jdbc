# JDBC Release Notes

## New Features

- Added the following new methods to the `JDBCDataBaseMetaData` class(**1.30.22.5**):
  - `setCatalog` to set the database catalog name to select a subspace of the Connection object's database in which to work.
  - `getCatalogs` to get the database catalogs.
  - `getTables` to get information of specific tables.
  - `getColumns` to get information of specific columns. 

- Added `getBigDecimal` method to the `JDBCResult` class to retrieve the value of specified column as BigDecimal type. It can be called in two ways(**1.30.22.5**):

  ```
  BigDecimal getBigDecimal(int columnIndex) throws SQLException;
  BigDecimal getBigDecimal(String columnLabel) throws SQLException;
  ```

- Added method `setMaxRows` to the `JDBCStatement` class to set the upper limit for the number of records that a `ResultSet` object can contain.(**1.30.22.5**)
- Added method `getMaxRows` to the `JDBCStatement` class to get the specified upper limit for the number of records applied to the `ResultSet` object.(**1.30.22.5**)
- The `insert into` clause of `JDBCPrepareStatement` class now supports inserting data to specific columns, and null values are written to the rest columns.(**1.30.22.5**)
- Added support for returning data of primitive Java types. (**1.30.22.4**)
- Added configuration parameter _tableAlias_ to access tables via aliases. (**1.30.22.2**)
- The configuration parameter _highAvailablitySites_ now supports input values separated by comma (",") delimiters. (**1.30.22.2**)
- Added support for DECIMAL128 data type. (**1.30.22.1**)
- Added connection property *sqlStd* for SQL parsing. (**1.30.22.1**)
- The method `getObject` now supports temporal types including LocalDate, LocalTime, LocalDateTime, java.util.Date, and Timestamp. (**1.30.21.4**)
- Added connection property *tableName* to load the specified table during connection. (**1.30.21.1**)
- Added connection property *enableHighAvailability* for connection strings, and the original *highAvailability* can be used as an alias. Configuration conflicts are reported if inconsistencies occur. (**1.30.21.1**)

## Improvements

- The `insert into` clause of `JDBCPrepareStatement` class now writes data in batches instead of by record.(**1.30.22.5**)
- The `commit()` and `rollback()` methods of `JDBCConnection` class do not support transactions at user level and return null by default.(**1.30.22.5**)
- Reduced the JAR file size for DolphinDB JDBC dependencies. (**1.30.22.4**)
- Added support for JDBC driver 4.0 version. Users no longer need to specify `Class.forName("com.dolphindb.jdbc.Driver")` when establishing connections. (**1.30.22.4**)
- If only one node of a cluster is connected and high availability is not enabled, automatic reconnection is attempted in case of connection failure or disconnection caused by network issues. (**1.30.21.1**)

## Issues Fixed

- Reconnection may result in incorrect query results if the parameters database and tableName were specified for connection. (**1.30.21.4**)
- Error occurred when using `setString` of JDBC `PrepareStatement` for string conversion. (**1.30.21.4**)
- When executing SQL statements, the uppercase strings that matched SQL keywords were converted to lowercase. (**1.30.21.3**)
- When using JDBC plugin to connect the server, if the specified *databasePath* contained tables that cannot be accessed, an error was reported and the connection failed.(**1.30.21.1**)
