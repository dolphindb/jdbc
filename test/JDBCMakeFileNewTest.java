import com.xxdb.DBConnection;
import com.xxdb.data.BasicDictionary;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.ResourceBundle;

public class JDBCMakeFileNewTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("setup/settings");
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    static String WORK_DIR = bundle.getString("WORK_DIR");
    static String DATA_DIR = bundle.getString("DATA_DIR");
    static ArrayList<String> colTypeString = null;
    Connection conn;
    Statement stm ;

    @Before
    public void Setup(){
        JDBCTestUtil.LOGININFO.put("user", "admin");
        JDBCTestUtil.LOGININFO.put("password", "123456");
        conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        try {
            stm = conn.createStatement();
        }catch (SQLException ex){

        }
    }

    public static void dataPrepare() {
        DBConnection db = null;

        StringBuilder sb = new StringBuilder();
        sb = new StringBuilder();
        sb.append("t = table((2013.06.13 2013.06.14 2013.06.15) as T, (2012.06.13 13:30:10 2012.06.14 13:30:10 2012.06.15 13:30:10 )as DT, (2012.06.13 13:30:10.008 2012.06.14 13:30:10.008 2012.06.15 13:30:10.008 )as TS );\n");
        sb.append("share t as trade");
        db = new DBConnection();
        try {
            db.connect(HOST, PORT);
            db.run(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getColTypes() {
        BasicDictionary schema = null;
        DBConnection db = new DBConnection();
        StringBuilder sb = new StringBuilder();
        sb.append("schema(trade)\n");

        try {
            db.connect(HOST, PORT,"admin","123456");
            schema = (BasicDictionary) db.run(sb.toString());

            BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
            BasicStringVector typeString = (BasicStringVector) colDefs.getColumn("typeString");
            int size = typeString.rows();
            colTypeString = new ArrayList<String>();
            for (int i = 0; i < size; i++) {
                colTypeString.add(typeString.getString(i).toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void Array2CSV(ArrayList<ArrayList<String>> data, String path)
    {
        try {
            BufferedWriter out =new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));
            for (int i = 0; i < data.size(); i++)
            {
                ArrayList<String> onerow=data.get(i);
                for (int j = 0; j < onerow.size(); j++)
                {
                    if(j == onerow.size() - 1 ) {
                        out.write(onerow.get(j));
                    }else {
                        out.write(onerow.get(j));
                        out.write(",");
                    }
                }
                out.newLine();
            }
            out.flush();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void makeFile(ResultSet rs) throws SQLException {
        getColTypes();
        ArrayList<ArrayList<String>> alldata=new ArrayList<ArrayList<String>>();
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int len = resultSetMetaData.getColumnCount();

        ArrayList<String> colsName = new ArrayList<String>();
        for (int i = 1; i <= len; ++i) {
            colsName.add(resultSetMetaData.getColumnName(i).toString());
        }
        alldata.add(colsName);

        while (rs.next()) {
            ArrayList<String> tmp = new ArrayList<String>();
            for (int i = 1; i <= len; ++i) {
                if(colTypeString.get(i-1).equals("DATE")) {
                    tmp.add(String.valueOf(rs.getDate(i)));
                }if(colTypeString.get(i-1).equals("SYMBOL")) {
                    tmp.add(rs.getString(i).toString());
                }if(colTypeString.get(i-1).equals("DOUBLE")) {
                    tmp.add(String.valueOf(rs.getDouble(i)));
                }if(colTypeString.get(i-1).equals("INT")) {
                    tmp.add(String.valueOf(rs.getInt(i)));
                }if(colTypeString.get(i-1).equals("DATETIME")) {
                    tmp.add(String.valueOf( rs.getTimestamp(i)));
                }if(colTypeString.get(i-1).equals("TIMESTAMP")) {
                    tmp.add(String.valueOf( rs.getTimestamp(i)));
                }if(colTypeString.get(i-1).equals("TIME")) {
                    tmp.add(String.valueOf( rs.getTime(i)));
                }if(colTypeString.get(i-1).equals("LONG")) {
                    tmp.add(String.valueOf( rs.getLong(i)));
                }if(colTypeString.get(i-1).equals("MONTH")) {
                    tmp.add(String.valueOf( rs.getDate(i)));
                }if(colTypeString.get(i-1).equals("BOOL")) {
                    tmp.add(String.valueOf( rs.getBoolean(i)));
                }if(colTypeString.get(i-1).equals("CHAR")) {
                    tmp.add(String.valueOf( rs.getString(i)));
                }if(colTypeString.get(i-1).equals("SHORT")) {
                    tmp.add(String.valueOf( rs.getShort(i)));
                }if(colTypeString.get(i-1).equals("MINUTE")) {
                    tmp.add(String.valueOf( rs.getTime(i)));
                }if(colTypeString.get(i-1).equals("SECOND")) {
                    tmp.add(String.valueOf( rs.getTime(i)));
                }if(colTypeString.get(i-1).equals("NANOTIME")) {
                    tmp.add(String.valueOf( rs.getTime(i)));
                }if(colTypeString.get(i-1).equals("NANOTIMESTAMP")) {
                    tmp.add(String.valueOf( rs.getTimestamp(i)));
                }if(colTypeString.get(i-1).equals("ANY")) {
                    tmp.add(String.valueOf( rs.getObject(i)));
                }
//				tmp.add(rs.getObject(i).toString());
            }
            alldata.add(tmp);
        }
        Array2CSV(alldata,WORK_DIR+"/JDBC_file_test.csv");
    }

    @Test
    public void testMakeFile() throws SQLException, IOException {
        dataPrepare();
        ResultSet rs =stm.executeQuery("select * from trade");
        makeFile(rs);
        String script = "tt=loadText('"+WORK_DIR+"/JDBC_file_test.csv"+"')\n" +
                "share tt as trade_re";
        DBConnection db = new DBConnection();
        try {
            db.connect(HOST, PORT);
            db.run(script);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BasicTable re = (BasicTable)db.run(String.format("select * from trade_re"));
        org.junit.Assert.assertEquals(re.rows(),3);
    }
}