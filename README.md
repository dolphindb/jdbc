<<<<<<< HEAD
# dolphindb-jdbcnknkn

## 测试代码
// JDBC 驱动名及数据库 URL
    private static final String JDBC_DRIVER = "com.xxdb.jdbc.Driver";
    //data目录有数据库文件
    /*
        sym = `C`MS`MS`MS`IBM`IBM`C`C`C$symbol;

        price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29;

        qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800;

        timestamp = [09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12];

        t1 = table(timestamp, sym, qty, price);

        db=database("D:/dolphinDB/data/data01");

        saveTable(db,t1,`t1);

     */
    //使用时要修改路径

    private static final String DB_URL = "jdbc:dolphindb://databasePath=D:/dolphinDB/data/data01/t1";
//CreateTable("D:\\dolphinDB\\data\\data1.java");
        TestStatement(new String[]{"select * from t1",
                "recordNum=1..9;select 1..9 as recordNum, sym from t1",
                "select 3 as portfolio, sym from t1;",
                "def f(a):a+100;select f(qty) as newQty, sym from t1;",
                "select last price from t1 group by sym;",
                "select top 3 * from t1;",
                "select * from t1 where sym=`IBM;",
                "select * from t1 where sym==`IBM;",
                "select * from t1 where sym=`IBM and qty>=2000 or timestamp>09:37:00;",
                "select * from t1 where qty>=2000, timestamp.minute()>=09:36m;",
                "select * from t1 where qty>=2000 and timestamp.minute()>=09:36m;",
                "select * from t1 where qty>=2000 && timestamp.minute()>=09:36m;",
                "select * from t1 where price>avg(price);",
                "select * from t1 where price>contextby(avg, price, sym) order by sym, price;",
                "select * from t1 order by sym, timestamp;",
                "select * from t1 where sym in `C`IBM order by sym, timestamp desc;",
                "select count(sym) as counts from t1 group by sym; ",
                "select avg(qty) from t1 group by sym;",
                "select wavg(price, qty) as vwap, sum(qty) from t1 group by sym;",
                "select wsum(price, qty) as dollarVolume, sum(qty) from t1 group by minute(timestamp) as ts;",
                "select sum(qty) from t1 group by sym, timestamp.minute() as minute;",
                "select sum(qty) from t1 group by sym, timestamp.minute() as minute order by minute;",
                "select wavg(price,qty) as wvap, sum(qty) as totalqty from t1 group by sym;",
                "select sym, price, qty, wavg(price,qty) as wvap, sum(qty) as totalqty from t1 context by sym;",
                "select sym, timestamp, price, eachPre(\\,price)-1.0 as ret from t1 context by sym;",
                "select *, cumsum(qty) from t1 context by sym, timestamp.minute();",
                "select top 2 * from t1 context by sym;",
                "select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym;",
                "select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym order by timestamp;",
                "select sum(qty) as totalqty from t1 group by sym having sum(qty)>10000;",
                "select * from t1 context by sym having count(sym)>2 and sum(qty)>10000;",
                "select price from t1 pivot by timestamp, sym;",
                "select last(price) from t1 pivot by timestamp.minute(), sym;",
                "update t1 set price=price+0.5, qty=qty-50 where sym=`C;t1;",
                "update t1 set price=price-avg(price) context by sym;t1",
                "item = table(1..10 as id, 10+rand(100,10) as qty, 1.0+rand(10.0,10) as price);promotion = table(1..10 as id, rand(0b 1b, 10) as flag, 0.5+rand(0.4,10) as discount);update item set price = price*discount from ej(item, promotion, `id) where flag=1;item",
                "exec price as p from t1;"
        });

        TestPreparedStatement("select * from t1 where string = ? and price > ? ",new Object[]{"MS",30.0});

## 测试结果

TestStatement begin
连接数据库...
databasePath=D:/dolphinDB/data/data01/t1
localhost8848
socket create
run script : t1 = loadTable("D:/dolphinDB/data/data01",`t1)
send connected command
read header
read msgOK
 实例化Statement对象...
socket create
run script : select * from t1
send connected command
read header
read msgOK
read readShortOK
9  4
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    
timestamp: 09:34:26,    sym: C,    qty: 2500,    price: 50.32,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
socket create
run script : recordNum=1..9;select 1..9 as recordNum, sym from t1
send connected command
read header
read msgOK
read readShortOK
9  2
recordNum: 1,    sym: C,    
recordNum: 2,    sym: MS,    
recordNum: 3,    sym: MS,    
recordNum: 4,    sym: MS,    
recordNum: 5,    sym: IBM,    
recordNum: 6,    sym: IBM,    
recordNum: 7,    sym: C,    
recordNum: 8,    sym: C,    
recordNum: 9,    sym: C,    
socket create
run script : select 3 as portfolio, sym from t1;
send connected command
read header
read msgOK
read readShortOK
9  2
portfolio: 3,    sym: C,    
portfolio: 3,    sym: MS,    
portfolio: 3,    sym: MS,    
portfolio: 3,    sym: MS,    
portfolio: 3,    sym: IBM,    
portfolio: 3,    sym: IBM,    
portfolio: 3,    sym: C,    
portfolio: 3,    sym: C,    
portfolio: 3,    sym: C,    
socket create
run script : def f(a):a+100;select f(qty) as newQty, sym from t1;
send connected command
read header
read msgOK
read readShortOK
9  2
newQty: 2300,    sym: C,    
newQty: 2000,    sym: MS,    
newQty: 2200,    sym: MS,    
newQty: 3300,    sym: MS,    
newQty: 6900,    sym: IBM,    
newQty: 5500,    sym: IBM,    
newQty: 1400,    sym: C,    
newQty: 2600,    sym: C,    
newQty: 8900,    sym: C,    
socket create
run script : select last price from t1 group by sym;
send connected command
read header
read msgOK
read readShortOK
3  2
sym: C,    last_price: 51.29,    
sym: MS,    last_price: 30.02,    
sym: IBM,    last_price: 175.23,    
socket create
run script : select top 3 * from t1;
send connected command
read header
read msgOK
read readShortOK
3  4
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
socket create
run script : select * from t1 where sym=`IBM;
send connected command
read header
read msgOK
read readShortOK
2  4
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
socket create
run script : select * from t1 where sym==`IBM;
send connected command
read header
read msgOK
read readShortOK
2  4
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
socket create
run script : select * from t1 where sym=`IBM and qty>=2000 or timestamp>09:37:00;
send connected command
read header
read msgOK
read readShortOK
3  4
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
socket create
run script : select * from t1 where qty>=2000, timestamp.minute()>=09:36m;
send connected command
read header
read msgOK
read readShortOK
3  4
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
socket create
run script : select * from t1 where qty>=2000 and timestamp.minute()>=09:36m;
send connected command
read header
read msgOK
read readShortOK
3  4
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
socket create
run script : select * from t1 where qty>=2000 && timestamp.minute()>=09:36m;
send connected command
read header
read msgOK
read readShortOK
3  4
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
socket create
run script : select * from t1 where price>avg(price);
send connected command
read header
read msgOK
read readShortOK
2  4
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
socket create
run script : select * from t1 where price>contextby(avg, price, sym) order by sym, price;
send connected command
read header
read msgOK
read readShortOK
4  4
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    
socket create
run script : select * from t1 order by sym, timestamp;
send connected command
read header
read msgOK
read readShortOK
9  4
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    
timestamp: 09:34:26,    sym: C,    qty: 2500,    price: 50.32,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    
socket create
run script : select * from t1 where sym in `C`IBM order by sym, timestamp desc;
send connected command
read header
read msgOK
read readShortOK
6  4
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
timestamp: 09:34:26,    sym: C,    qty: 2500,    price: 50.32,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
socket create
run script : select count(sym) as counts from t1 group by sym; 
send connected command
read header
read msgOK
read readShortOK
3  2
sym: C,    counts: 4,    
sym: MS,    counts: 3,    
sym: IBM,    counts: 2,    
socket create
run script : select avg(qty) from t1 group by sym;
send connected command
read header
read msgOK
read readShortOK
3  2
sym: C,    avg_qty: 3700,    
sym: MS,    avg_qty: 2400,    
sym: IBM,    avg_qty: 6100,    
socket create
run script : select wavg(price, qty) as vwap, sum(qty) from t1 group by sym;
send connected command
read header
read msgOK
read readShortOK
3  3
sym: C,    vwap: 50.828378,    sum_qty: 14800,    
sym: IBM,    vwap: 175.085082,    sum_qty: 12200,    
sym: MS,    vwap: 29.726389,    sum_qty: 7200,    
socket create
run script : select wsum(price, qty) as dollarVolume, sum(qty) from t1 group by minute(timestamp) as ts;
send connected command
read header
read msgOK
read readShortOK
5  3
ts: 09:32m,    dollarVolume: 1.189796E6,    sum_qty: 6800,    
ts: 09:34m,    dollarVolume: 300908,    sum_qty: 6000,    
ts: 09:35m,    dollarVolume: 946242,    sum_qty: 5400,    
ts: 09:36m,    dollarVolume: 214030,    sum_qty: 7200,    
ts: 09:38m,    dollarVolume: 451352,    sum_qty: 8800,    
socket create
run script : select sum(qty) from t1 group by sym, timestamp.minute() as minute;
send connected command
read header
read msgOK
read readShortOK
5  3
sym: C,    minute: 09:34m,    sum_qty: 6000,    
sym: MS,    minute: 09:36m,    sum_qty: 7200,    
sym: IBM,    minute: 09:32m,    sum_qty: 6800,    
sym: IBM,    minute: 09:35m,    sum_qty: 5400,    
sym: C,    minute: 09:38m,    sum_qty: 8800,    
socket create
run script : select sum(qty) from t1 group by sym, timestamp.minute() as minute order by minute;
send connected command
read header
read msgOK
read readShortOK
5  3
sym: IBM,    minute: 09:32m,    sum_qty: 6800,    
sym: C,    minute: 09:34m,    sum_qty: 6000,    
sym: IBM,    minute: 09:35m,    sum_qty: 5400,    
sym: MS,    minute: 09:36m,    sum_qty: 7200,    
sym: C,    minute: 09:38m,    sum_qty: 8800,    
socket create
run script : select wavg(price,qty) as wvap, sum(qty) as totalqty from t1 group by sym;
send connected command
read header
read msgOK
read readShortOK
3  3
sym: C,    wvap: 50.828378,    totalqty: 14800,    
sym: IBM,    wvap: 175.085082,    totalqty: 12200,    
sym: MS,    wvap: 29.726389,    totalqty: 7200,    
socket create
run script : select sym, price, qty, wavg(price,qty) as wvap, sum(qty) as totalqty from t1 context by sym;
send connected command
read header
read msgOK
read readShortOK
9  5
sym: C,    price: 49.6,    qty: 2200,    wvap: 50.828378,    totalqty: 14800,    
sym: C,    price: 50.76,    qty: 1300,    wvap: 50.828378,    totalqty: 14800,    
sym: C,    price: 50.32,    qty: 2500,    wvap: 50.828378,    totalqty: 14800,    
sym: C,    price: 51.29,    qty: 8800,    wvap: 50.828378,    totalqty: 14800,    
sym: IBM,    price: 174.97,    qty: 6800,    wvap: 175.085082,    totalqty: 12200,    
sym: IBM,    price: 175.23,    qty: 5400,    wvap: 175.085082,    totalqty: 12200,    
sym: MS,    price: 29.46,    qty: 1900,    wvap: 29.726389,    totalqty: 7200,    
sym: MS,    price: 29.52,    qty: 2100,    wvap: 29.726389,    totalqty: 7200,    
sym: MS,    price: 30.02,    qty: 3200,    wvap: 29.726389,    totalqty: 7200,    
socket create
run script : select sym, timestamp, price, eachPre(\,price)-1.0 as ret from t1 context by sym;
send connected command
read header
read msgOK
read readShortOK
9  4
sym: C,    timestamp: 09:34:07,    price: 49.6,    ret: ,    
sym: C,    timestamp: 09:34:16,    price: 50.76,    ret: 0.023387,    
sym: C,    timestamp: 09:34:26,    price: 50.32,    ret: -0.008668,    
sym: C,    timestamp: 09:38:12,    price: 51.29,    ret: 0.019277,    
sym: IBM,    timestamp: 09:32:47,    price: 174.97,    ret: ,    
sym: IBM,    timestamp: 09:35:26,    price: 175.23,    ret: 0.001486,    
sym: MS,    timestamp: 09:36:42,    price: 29.46,    ret: ,    
sym: MS,    timestamp: 09:36:51,    price: 29.52,    ret: 0.002037,    
sym: MS,    timestamp: 09:36:59,    price: 30.02,    ret: 0.016938,    
socket create
run script : select *, cumsum(qty) from t1 context by sym, timestamp.minute();
send connected command
read header
read msgOK
read readShortOK
9  5
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    cumsum_qty: 2200,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    cumsum_qty: 3500,    
timestamp: 09:34:26,    sym: C,    qty: 2500,    price: 50.32,    cumsum_qty: 6000,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    cumsum_qty: 8800,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    cumsum_qty: 6800,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    cumsum_qty: 5400,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    cumsum_qty: 1900,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    cumsum_qty: 4000,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    cumsum_qty: 7200,    
socket create
run script : select top 2 * from t1 context by sym;
send connected command
read header
read msgOK
read readShortOK
6  4
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
socket create
run script : select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym;
send connected command
read header
read msgOK
read readShortOK
9  5
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    fittedPrice: 50.282221,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    fittedPrice: 50.156053,    
timestamp: 09:34:26,    sym: C,    qty: 2500,    price: 50.32,    fittedPrice: 50.324277,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    fittedPrice: 51.207449,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    fittedPrice: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    fittedPrice: 175.23,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    fittedPrice: 29.447279,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    fittedPrice: 29.535034,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    fittedPrice: 30.017687,    
socket create
run script : select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym order by timestamp;
send connected command
read header
read msgOK
read readShortOK
9  5
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    fittedPrice: 174.97,    
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    fittedPrice: 50.282221,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    fittedPrice: 50.156053,    
timestamp: 09:34:26,    sym: C,    qty: 2500,    price: 50.32,    fittedPrice: 50.324277,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    fittedPrice: 175.23,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    fittedPrice: 29.447279,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    fittedPrice: 29.535034,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    fittedPrice: 30.017687,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    fittedPrice: 51.207449,    
socket create
run script : select sum(qty) as totalqty from t1 group by sym having sum(qty)>10000;
send connected command
read header
read msgOK
read readShortOK
2  2
sym: C,    totalqty: 14800,    
sym: IBM,    totalqty: 12200,    
socket create
run script : select * from t1 context by sym having count(sym)>2 and sum(qty)>10000;
send connected command
read header
read msgOK
read readShortOK
4  4
timestamp: 09:34:07,    sym: C,    qty: 2200,    price: 49.6,    
timestamp: 09:34:16,    sym: C,    qty: 1300,    price: 50.76,    
timestamp: 09:34:26,    sym: C,    qty: 2500,    price: 50.32,    
timestamp: 09:38:12,    sym: C,    qty: 8800,    price: 51.29,    
socket create
run script : select price from t1 pivot by timestamp, sym;
send connected command
read header
read msgOK
read readShortOK
9  4
timestamp: 09:32:47,    C: ,    IBM: 174.97,    MS: ,    
timestamp: 09:34:07,    C: 49.6,    IBM: ,    MS: ,    
timestamp: 09:34:16,    C: 50.76,    IBM: ,    MS: ,    
timestamp: 09:34:26,    C: 50.32,    IBM: ,    MS: ,    
timestamp: 09:35:26,    C: ,    IBM: 175.23,    MS: ,    
timestamp: 09:36:42,    C: ,    IBM: ,    MS: 29.46,    
timestamp: 09:36:51,    C: ,    IBM: ,    MS: 29.52,    
timestamp: 09:36:59,    C: ,    IBM: ,    MS: 30.02,    
timestamp: 09:38:12,    C: 51.29,    IBM: ,    MS: ,    
socket create
run script : select last(price) from t1 pivot by timestamp.minute(), sym;
send connected command
read header
read msgOK
read readShortOK
5  4
minute_timestamp: 09:32m,    C: ,    IBM: 174.97,    MS: ,    
minute_timestamp: 09:34m,    C: 50.32,    IBM: ,    MS: ,    
minute_timestamp: 09:35m,    C: ,    IBM: 175.23,    MS: ,    
minute_timestamp: 09:36m,    C: ,    IBM: ,    MS: 30.02,    
minute_timestamp: 09:38m,    C: 51.29,    IBM: ,    MS: ,    
socket create
run script : update t1 set price=price+0.5, qty=qty-50 where sym=`C;t1;
send connected command
read header
read msgOK
read readShortOK
9  4
timestamp: 09:34:07,    sym: C,    qty: 2150,    price: 50.1,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: 29.46,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: 29.52,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 30.02,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: 174.97,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 175.23,    
timestamp: 09:34:16,    sym: C,    qty: 1250,    price: 51.26,    
timestamp: 09:34:26,    sym: C,    qty: 2450,    price: 50.82,    
timestamp: 09:38:12,    sym: C,    qty: 8750,    price: 51.79,    
socket create
run script : update t1 set price=price-avg(price) context by sym;t1
send connected command
read header
read msgOK
read readShortOK
9  4
timestamp: 09:34:07,    sym: C,    qty: 2150,    price: -0.8925,    
timestamp: 09:36:42,    sym: MS,    qty: 1900,    price: -0.206667,    
timestamp: 09:36:51,    sym: MS,    qty: 2100,    price: -0.146667,    
timestamp: 09:36:59,    sym: MS,    qty: 3200,    price: 0.353333,    
timestamp: 09:32:47,    sym: IBM,    qty: 6800,    price: -0.13,    
timestamp: 09:35:26,    sym: IBM,    qty: 5400,    price: 0.13,    
timestamp: 09:34:16,    sym: C,    qty: 1250,    price: 0.2675,    
timestamp: 09:34:26,    sym: C,    qty: 2450,    price: -0.1725,    
timestamp: 09:38:12,    sym: C,    qty: 8750,    price: 0.7975,    
socket create
run script : item = table(1..10 as id, 10+rand(100,10) as qty, 1.0+rand(10.0,10) as price);promotion = table(1..10 as id, rand(0b 1b, 10) as flag, 0.5+rand(0.4,10) as discount);update item set price = price*discount from ej(item, promotion, `id) where flag=1;item
send connected command
read header
read msgOK
read readShortOK
10  3
id: 1,    qty: 25,    price: 3.378022,    
id: 2,    qty: 46,    price: 5.196276,    
id: 3,    qty: 87,    price: 5.571555,    
id: 4,    qty: 75,    price: 3.165544,    
id: 5,    qty: 103,    price: 4.616506,    
id: 6,    qty: 89,    price: 5.993583,    
id: 7,    qty: 28,    price: 7.062255,    
id: 8,    qty: 87,    price: 5.304236,    
id: 9,    qty: 74,    price: 1.331726,    
id: 10,    qty: 45,    price: 5.338015,    
socket create
run script : exec price as p from t1;
send connected command
read header
read msgOK
read readShortOK
9  1
p: -0.8925,    
p: -0.206667,    
p: -0.146667,    
p: 0.353333,    
p: -0.13,    
p: 0.13,    
p: 0.2675,    
p: -0.1725,    
p: 0.7975,    
TestStatement end
TestStatement begin
连接数据库...
databasePath=D:/dolphinDB/data/data01/t1
localhost8848
socket create
run script : t1 = loadTable("D:/dolphinDB/data/data01",`t1)
send connected command
read header
read msgOK
 实例化Statement对象...
socket create
run script : select * from t1 where string = `MS and price > 30.0
send connected command
read header
read msgOK
read readShortOK
0  4
TestPreparedStatement end

Process finished with exit code 0
=======
# dolphindb-jdbc
>>>>>>> old-origin/master
