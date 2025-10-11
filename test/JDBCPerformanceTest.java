public class JDBCPerformanceTest {
    //    public static boolean CreateDfs() throws IOException {
//        Boolean success = false;
//        String script = " if(existsDatabase('dfs://select')) dropDatabase('dfs://select');\n"+
//                "db = database('dfs://select', RANGE, 0 10000 20000 30001,,'TSDB');\n" +
//                "n=1000000;\n" +
//                "id = int(take(1..30000, n));\n" +
//                "boolv = bool(rand([true, false, NULL], n));\n" +
//                "charv = char(rand(rand(-100..100, 1000) join take(char(), 4), n));\n" +
//                "shortv = short(rand(rand(-100..100, 1000) join take(short(), 4), n));\n" +
//                "intv = int(rand(rand(-1000..1000, 1000) join take(int(), 4), n));\n" +
//                "longv = long(rand(rand(-100..100, 1000) join take(long(), 4), n));\n" +
//                "doublev = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n));\n" +
//                "floatv = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n));\n" +
//                "datev = date(rand(rand(-100..100, 1000) join take(date(), 4), n));\n" +
//                "monthv = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n));\n" +
//                "timev = time(rand(rand(0..100, 1000) join take(time(), 4), n));\n" +
//                "minutev = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n));\n" +
//                "secondv = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n));\n" +
//                "datetimev = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n));\n" +
//                "timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n));\n" +
//                "nanotimev = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n));\n" +
//                "nanotimestampv = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n));\n" +
//                "symbolv = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
//                "stringv = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
//                "uuidv = rand(rand(uuid(), 1000) join take(uuid(), 4), n);\n" +
//                "datehourv = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n));\n" +
//                "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n);\n" +
//                "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n" +
//                "blobv = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n)));\n" +
//                "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
//                "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
//                "decimal32v = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
//                "decimal64v = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
//                "t = table(id,boolv, charv,shortv, intv,  longv, floatv, doublev, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, symbolv, stringv, uuidv, datehourv, ippaddrv, int128v, blobv);\n" +
//                "db.createPartitionedTable(t,'t1', 'id',,'id').append!(t); \n";
//        DBConnection db = new DBConnection();
//        db.connect(HOST,PORT,"admin","123456");
//        try{
//        db.run(script);
//        success = true;
//    }catch(Exception e){
//        e.printStackTrace();
//        success = false;
//    }finally{
//        if(db != null){
//            db.close();
//        }
//        return success;
//        }
//    }
//    @Test
//    public void test_PreparedStatement_select() throws SQLException, IOException {
//        CreateDfs();
//        BasicTable bt1 = null;
//        long startTime1 = System.currentTimeMillis();
//        for(int i=0;i<10;i++){
//            PreparedStatement ps1 = conn.prepareStatement("select * from loadTable('dfs://select','t1') ");
//            JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery();
//            bt1 = (BasicTable) rs1.getResult();
//            System.out.println("查询次数：" + i);
//        }
//        long elapsedTime1 = System.currentTimeMillis() - startTime1;
//        System.out.println("Timeout after " + elapsedTime1 + " ms");
//        System.out.println(bt1.rows());
//        org.junit.Assert.assertEquals(1000000,bt1.rows());
//    }
//    @Test
//    public void test_JDBCStatement_select() throws SQLException, IOException, ClassNotFoundException {
//        CreateDfs();
//        BasicTable bt1 = null;
//        JDBCStatement stm = null;
//        stm = (JDBCStatement) conn.createStatement();
//        long startTime1 = System.currentTimeMillis();
//        for(int i=0;i<10;i++){
//            JDBCResultSet rs1 = (JDBCResultSet)stm.executeQuery("select * from loadTable('dfs://select','t1') ");
//            bt1 = (BasicTable) rs1.getResult();
//            System.out.println("查询次数：" + i);
//        }
//        long elapsedTime1 = System.currentTimeMillis() - startTime1;
//        System.out.println("Timeout after " + elapsedTime1 + " ms");
//        System.out.println(bt1.rows());
//        org.junit.Assert.assertEquals(1000000,bt1.rows());
//    }
//    @Test
//    public void Test_select() throws Exception {
//        DBConnection conn = new DBConnection();
//        conn.connect("192.168.0.9",8848,"admin","123456");
//        CreateDfs();
//        long startTime1 = System.currentTimeMillis();
//        for(int i=0;i<10;i++) {
//            BasicTable re = (BasicTable) conn.run("select * from loadTable('dfs://select','t1')");
//            System.out.println("查询次数：" + i);
//        }
//        long elapsedTime1 = System.currentTimeMillis() - startTime1;
//        System.out.println("Timeout after " + elapsedTime1 + " ms");
//    }
//
//    @Test
//    public void test_PreparedStatement_delete() throws SQLException, IOException {
//        BasicTable bt1 = null;
//        Long elapsedTimeavg = 0l;
//        for(int i=0;i<10;i++){
//            CreateDfs();
//            long startTime1 = System.currentTimeMillis();
//            PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://select','t1') ");
//            ps1.execute();
//            System.out.println("查询次数：" + i);
//            long elapsedTime1 = System.currentTimeMillis() - startTime1;
//            System.out.println("Timeout after " + elapsedTime1 + " ms");
//            elapsedTimeavg = elapsedTimeavg + elapsedTime1;
//        }
//        System.out.println("Timeout after " + elapsedTimeavg + " ms");
//    }
//    @Test
//    public void test_JDBCStatement_delete() throws SQLException, IOException, ClassNotFoundException {
//        Long elapsedTimeavg = 0l;
//        BasicTable bt1 = null;
//        JDBCStatement stm = null;
//        stm = (JDBCStatement) conn.createStatement();
//        for(int i=0;i<10;i++){
//            CreateDfs();
//            long startTime1 = System.currentTimeMillis();
//            stm.execute("delete from loadTable('dfs://select','t1') ");
//            System.out.println("查询次数：" + i);
//            long elapsedTime1 = System.currentTimeMillis() - startTime1;
//            System.out.println("Timeout after " + elapsedTime1 + " ms");
//            elapsedTimeavg = elapsedTimeavg + elapsedTime1;
//        }
//        System.out.println("Timeout after " + elapsedTimeavg + " ms");
//    }
//    @Test
//    public void Test_delete() throws Exception {
//        DBConnection conn = new DBConnection();
//        conn.connect("192.168.0.9",8848,"admin","123456");
//        Long elapsedTimeavg = 0l;
//        for(int i=0;i<10;i++) {
//            CreateDfs();
//            conn.run("sleep(200)");
//            long startTime1 = System.currentTimeMillis();
//            conn.run("delete from loadTable('dfs://select','t1')");
//            System.out.println("查询次数：" + i);
//            long elapsedTime1 = System.currentTimeMillis() - startTime1;
//            System.out.println("Timeout after " + elapsedTime1 + " ms");
//            elapsedTimeavg = elapsedTimeavg + elapsedTime1;
//        }
//        System.out.println("Timeout after " + elapsedTimeavg + " ms");
//    }
}
