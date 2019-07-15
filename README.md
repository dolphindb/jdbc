#  DolphinDB JDBC

用于执行SQL语句以及DolphinDB语句的Java API，可以为多种关系数据库提供统一访问：

* 支持SQL语句
* 支持DolphinDB语句


 
### 1. 创建内存表
使用DolphinDB语句savetable()将创建好的表格保存在数据库中
saveTable中需要有Db,Data,Table name;

* Db: 保存在数据库中的位置
* Data：需要保存的表格
* Table name：表格保存的名字 	
* host & port: 连接的域名和端口号

下面是创建表格用到的语句，可写成文件然后再需要的时候读取

```
bool = [1b, 0b];
char = [97c, 'A'];
short = [122h, 123h];
int = [21, 22];
long = [22l, 23l];
float  = [2.1f, 2.2f];
double = [2.1, 2.2];
string= [`Hello, `world];
date = [2013.06.13, 2013.06.14];
month = [2016.06M, 2016.07M];
time = [13:30:10.008, 13:30:10.009];
minute = [13:30m, 13:31m];
second = [13:30:10, 13:30:11];
datetime = [2012.06.13 13:30:10, 2012.06.13 13:30:10];
timestamp = [2012.06.13 13:30:10.008, 2012.06.13 13:30:10.009];
nanotime = [13:30:10.008007006, 13:30:10.008007007];
nanotimestamp = [2012.06.13 13:30:10.008007006, 2012.06.13 13:30:10.008007007];
t1= table(bool,char,short,int,long,float,double,string,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp);
```

将所有的语句拼成一个String，最后由DBconnection连接端口并且运行。

```
 public static void CreateTable(String file, String savePath, String tableName, String host, String port) {
		DBConnection db = null;
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
			StringBuilder sb = new StringBuilder();
			String s = null;
			while ((s = bufferedReader.readLine()) != null) {
				sb.append(s + "\n");
			}
			sb.append(Driver.DB + " =( \"" + savePath + "\")\n ");
			sb.append("saveTable(").append("C:/DolphinDB/Data“).append(", t1, `").append(tableName).append(");\n");
			db = new DBConnection();
			db.connect(host, Integer.parseInt(port));
			db.run(sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (db != null)
				db.close();
		}
	}
```

### 2. 内存表的增删改查

#### 2.1. 内存表的增加
对上面的表格增加新的内容，在“？”处填相应的新内容

```java
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			
			//SQL insert语句
			stmt = conn.prepareStatement("insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			
			//load数据库中的表格
			stmt.execute("t1 = loadTable(\"C:/DolphinDB/Data\“);
			
			//读取表格检查是否增加
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
		} catch (Exception e) {
			e.printStackTrace();
			} finally {
				//释放
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException se2) {
			}			
			//释放
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
```

#### 2.2. 内存表的删除

对上面的表格进行内容删除，在“？”处填相应的的删除条件

```java
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			
			//SQL delete语句
			stmt = conn.prepareStatement("delete from t1 where char = ?");
			
			//load数据库中的表格
			stmt.execute("t1 = loadTable(\"C:/DolphinDB/Data\“);
			
			//读取表格检查是否删除
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
		} catch (Exception e) {
			e.printStackTrace();
			} finally {
				//释放
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException se2) {
			}			
			//释放
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
```

#### 2.3. 内存表的更改
对上面的表格进行内容更改，在“？”处填相应的的更改内容及条件

```java
	
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			
			//SQL update语句
			stmt = conn.prepareStatement("update t1 set bool = ? where char = ?");
			
			//load数据库中的表格
			stmt.execute("t1 = loadTable(\"C:/DolphinDB/Data\“);
			
			//读取表格检查是否删除
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
		} catch (Exception e) {
			e.printStackTrace();
			} finally {
				//释放
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException se2) {
			}			
			//释放
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
```
 
### 3. 分区表的增删改查 
与分区表连接的时候可以在URL中加入path以及相应内容，这样getConnection()时会预先加载分区表的table

##### Example：
```
"jdbc:dolphindb://172.16.95.128:8921?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2016.12M";
```

#### 3.1. 创建分区表
将创建分区表的语句拼成一个String，最后由DBconnection连接端口并且运行。
这里创建的是一个VALUE分区表。[其他的分区表](https://github.com/dolphindb/Tutorials_CN/blob/master/database.md) 

```java
public static void CreateValueTable(String host, String port) {
			DBConnection db = null;
			StringBuilder sb = new StringBuilder();
			sb.append("n=1000000\n");
			sb.append("month=take(2000.01M..2016.12M, n)\n");
			sb.append("x=rand(1.0, n)\n");
			sb.append("t=table(month, x)\n");
			sb.append("db=database(\"C:/DolphinDB/Data/valuedb\", VALUE, 2000.01M..2016.12M)\n");
			sb.append("pt = db.createPartitionedTable(t, `pt, `month)\n");
			sb.append("pt.append!(t)\n");	
			db = new DBConnection();
			
			try {
				db.connect(host, Integer.parseInt(port));
				db.run(sb.toString());
			} catch (NumberFormatException | IOException e) {
				e.printStackTrace();
			}finally {
				if (db != null)
					db.close();
			}
						
	}
```
#### 3.2. 分区表的增加
对建立的分区表的内容进行增加，在“？”处放入相应的object

```java
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			
			//dfs下会预先load table
			conn = DriverManager.getConnection(url);
			
			//SQL insert语句
			stmt = conn.prepareStatement(“insert into pt values(?, ?)”);
			
			//读取表格检查是否删除
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
		} catch (Exception e) {
			e.printStackTrace();
			} finally {
				//释放
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException se2) {
			}			
			//释放
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
```
#### 3.3. 分区表的删除
对建立的分区表的内容进行删除操作，在“？”放入删除条件

```java
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			
			//dfs下会预先load table
			conn = DriverManager.getConnection(url);
			
			//SQL delete语句
			stmt = conn.prepareStatement(“delete from pt where x = ?”);
			
			//读取表格检查是否删除
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
		} catch (Exception e) {
			e.printStackTrace();
			} finally {
				//释放
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException se2) {
			}			
			//释放
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
```
#### 3.4. 分区表的更改
对建立的分区表的内容进行更改操作，在“？”放入相应条件

```java
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			
			//dfs下会预先load table
			conn = DriverManager.getConnection(url);
			
			//SQL update语句
			stmt = conn.prepareStatement(“update pt set x = ? where month = ?”);
			
			//读取表格检查是否删除
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
		} catch (Exception e) {
			e.printStackTrace();
			} finally {
				//释放
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException se2) {
			}			
			//释放
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
```
### 4. SQL query
DolphinDB的JDBC接口支持SQL语句，
#### 4.1. Regular sql query 
如上面所示的一样可以用executeQuery method来运行SQL语句

#### 4.2. Join
在DolphinDB的JDBC中除了SQL的Join外还可以使用DolphinDB的join脚本

Example
例如T1，T2两个表格

```
t1= table(1 2 3 3 as id, 7.8 4.6 5.1 0.1 as value)
t2 = table(5 3 1 as id,  300 500 800 as qty);

>t1;
id value
-- -----  
1  7.8    
2  4.6    
3  5.1    
3  0.1    

>t2;
id qty
-- ---
5  300
3  500
1  800

```

#### 4.3. Full join 
返回等连接函数中的所有行以及左表或右表中未匹配的行

```
>fj(t1, t2, `id);

id value t2_id qty
-- ----- ----- ---
1  7.8   1     800
2  4.6
3  5.1   3     500
3  0.1   3     500
         5     300

>select * from fj(t1, t2, `id) where id=3;

id value t2_id qty
-- ----- ----- ---
3  5.1   3     500
3  0.1   3     500
```

#### 4.4. Left join 
返回左表中的所有行，右表中的匹配行。 没有匹配时结果为NULL。 如果右表中有多个匹配记录，则默认采用第一个记录。 left join始终返回与左表相同的行数

```
//Table T2 join in T1
>select id, value, qty from lj(t1, t2, `id);

id value qty
-- ----- ---
1  7.8   800
2  4.6
3  5.1   500
3  0.1   500
```

#### 4.5. Equal join 
只要匹配列上存在匹配项，就从两个表中选择所有行

```
>select id, value, qty from ej(t1, t2,`id);

id value qty
-- ----- ---
1  7.8   800
3  5.1   500
3  0.1   500

>select id, value, qty from ej(t2, t1,`id);
id value qty
-- ----- ---
3  5.1   500
3  0.1   500
1  7.8   800

>select id, value, qty from ej(t2, t1,`id) where id=3;
>
id value qty
-- ----- ---
3  5.1   500
3  0.1   500

>select id, value, qty from sej(t1,t2,`id);

id value qty
-- ----- ---
1  7.8   800
3  5.1   500
3  0.1   500
```
使用不是连接列的公共变量equal join两个表：

```
>select id, value, qty, x from ej(t1, t2, `id);  
id value qty x
-- ----- --- -
1  7.8   800 4
3  5.1   500 2
3  0.1   500 1
// 先试用T1中的变量，如果失败了用T2中的.

>select id, value, qty, t2.x from ej(t1, t2, `id);
id value qty x
-- ----- --- --
1  7.8   800 88
3  5.1   500 66
3  0.1   500 66
// 这里我们选用了T2的变量

>ej(t1, t2, `id);
id value x qty t2_x
-- ----- - --- ----
1  7.8   4 800 88
3  5.1   2 500 66
3  0.1   1 500 66
```
#### 4.6. Asof Join
通过时间来连接列表，asof join函数类似于左连接函数。 他们的区别是：

* 对于左表中具有时间t的行，如果右表中没有匹配，则右表中的行对应于时间t之前的最近时间。
* 如果只有一个连接列，则asof连接函数假定右表在连接列上排序。 如果有多个连接列，则asof连接函数假定右表在其他连接列定义的每个组中的最后一个连接列上排序。 右表不需要按其他连接列排序。 如果不满足这些条件，我们可能会看到预期外的结果。 左表不需要排序。
* 如果右表中有多个匹配记录，则默认采用最后一个值。

```
t1 = table(2015.01.01+(0 31 59 90 120) as date, 1.2 7.8 4.6 5.1 9.5 as value)
t2 = table(2015.02.01+(0 15 89 89) as date, 1..4 as qty);

>t1;
date       value
---------- -----
2015.01.01 1.2
2015.02.01 7.8
2015.03.01 4.6
2015.04.01 5.1
2015.05.01 9.5

>t2;
date       qty
---------- ---
2015.02.01 1
2015.02.16 2
2015.05.01 3
2015.05.01 4

>select * from lj(t1, t2, `date);
date       value qty
---------- ----- ---
2015.01.01 1.2
2015.02.01 7.8   1
2015.03.01 4.6
2015.04.01 5.1
2015.05.01 9.5   3

>select * from aj(t1, t2, `date);
date       value qty
---------- ----- ---
2015.01.01 1.2
2015.02.01 7.8   1
2015.03.01 4.6   2
2015.04.01 5.1   2
2015.05.01 9.5   4

>select * from aj(t1, t2, `date) where t1.date>=2015.03.01;
date       value qty
---------- ----- ---
2015.03.01 4.6   2
2015.04.01 5.1   2
2015.05.01 9.5   4
```
连接的常见用法是加入时间字段以检索最新信息。 假设我们有以下3个表，其中数据全部按列分钟排序。

```
minute = 09:30m 09:32m 09:33m 09:35m
price = 174.1 175.2 174.8 175.2
t1 = table(minute, price)

minute = 09:30m 09:31m 09:33m 09:34m
price = 29.2 28.9 29.3 30.1
t2 = table(minute, price)

minute =09:30m 09:31m 09:34m 09:36m
price = 51.2 52.4 51.9 52.8
t3 = table(minute, price);

>t1;
minute price
------ -----
09:30m 174.1
09:32m 175.2
09:33m 174.8
09:35m 175.2

>t2;
minute price
------ -----
09:30m 29.2
09:31m 28.9
09:33m 29.3
09:34m 30.1

>t3;
minute price
------ -----
09:30m 51.2
09:31m 52.4
09:34m 51.9
09:36m 52.8

>t2 = aj(t2, t3, `minute);

>t2;
minute price t3_price
------ ----- --------
09:30m 29.2  51.2
09:31m 28.9  52.4
09:33m 29.3  52.4
09:34m 30.1  51.9

>aj(t1, t2, `minute);
minute price t2_price t3_price
------ ----- ------   --------
09:30m 174.1 29.2     51.2
09:32m 175.2 28.9     52.4
09:33m 174.8 29.3     52.4
09:35m 175.2 30.1     51.9
```
[更多信息](http://www.dolphindb.com/help/index.html?FunctionReferences.html) 


