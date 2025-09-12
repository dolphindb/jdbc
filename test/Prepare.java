import com.xxdb.DBConnection;

import java.io.IOException;
import java.util.ResourceBundle;

public class Prepare {
    static ResourceBundle bundle = ResourceBundle.getBundle("setup/settings");
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;

    public static void clear_env() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\ttry{stopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)}catch(ex){}\n" +
                "}");
//        conn.run("res = getStreamingSQLStatus()\n" +
//                "    for(sqlStream in res){\n" +
//                "        try{unsubscribeStreamingSQL(, sqlStream.queryId)}catch(ex){print ex}\n" +
//                "        try{revokeStreamingSQL(sqlStream.queryId)}catch(ex){print ex}\n" +
//                "    }\n" +
//                "    go;\n" +
//                "    try{revokeStreamingSQLTable(`t1)}catch(ex){print ex}\n" +
//                "    try{revokeStreamingSQLTable(`t2)}catch(ex){print ex}\n" +
//                "    try{revokeStreamingSQLTable(`bondFilter)}catch(ex){print ex}\n" +
//                "    try{revokeStreamingSQLTable(`bestBondQuotation)}catch(ex){print ex}\n" );
        conn.run("def getAllShare(){\n" +
                "\treturn select name from objs(true) where shared=1\n" +
                "\t}\n" +
                "\n" +
                "def clearShare(){\n" +
                "\tlogin(`admin,`123456)\n" +
                "\tallShare=exec name from pnodeRun(getAllShare)\n" +
                "\tfor(i in allShare){\n" +
                "\t\ttry{\n" +
                "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
                "\t\t\t}catch(ex1){}\n" +
                "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
                "\t}\n" +
                "\ttry{\n" +
                "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
                "\t}catch(ex1){}\n" +
                "}\n" +
                "clearShare()");
        conn.run("try{dropStreamEngine(\"serInput\");\n}catch(ex){\n}\n");
    }
}
