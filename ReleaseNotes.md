# JDBC Release Notes

## New Features

- The method `getObject` now supports temporal types including LocalDate, LocalTime, LocalDateTime, java.util.Date, and Timestamp. (**1.30.21.4**)
- Added connection property *tableName* to load the specified table during connection. (**1.30.21.1**)
- Added connection property *enableHighAvailability* for connection strings, and the original *highAvailability* can be used as an alias. Configuration conflicts are reported if inconsistencies occur. (**1.30.21.1**)

## Improvements

- If only one node of a cluster is connected and high availability is not enabled, automatic reconnection is attempted in case of connection failure or disconnection caused by network issues. (**1.30.21.1**)

## Issues Fixed

- Reconnection may result in incorrect query results if the parameters database and tableName were specified for connection. (**1.30.21.4**)
- Error occurred when using `setString` of JDBC `PrepareStatement` for string conversion. (**1.30.21.4**)
- When executing SQL statements, the uppercase strings that matched SQL keywords were converted to lowercase. (**1.30.21.3**)
- When using JDBC plugin to connect the server, if the specified *databasePath* contained tables that cannot be accessed, an error was reported and the connection failed.(**1.30.21.1**)
