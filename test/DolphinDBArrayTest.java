import com.dolphindb.jdbc.DolphinDBArray;
import com.xxdb.data.BasicArrayVector;
import com.xxdb.data.BasicDate;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicIntVector;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DolphinDBArrayTest {
    @Test
    public void Test_DolphinDBArray_getBaseTypeName() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        BasicArrayVector biav = new BasicArrayVector(new int[]{3,5,9,11},biv);
        DolphinDBArray array1 = new DolphinDBArray(biav);
        Assert.assertEquals("INT[]", array1.getBaseTypeName());
    }

    @Test
    public void Test_DolphinDBArray_getBaseType() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        System.out.println(array1.getBaseTypeName());
        System.out.println(array1.getBaseType());
        Assert.assertEquals("INT", array1.getBaseTypeName());
        Assert.assertEquals(4, array1.getBaseType());
    }

    @Test
    public void Test_DolphinDBArray_getArray_map_not_null() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        BasicArrayVector biav = new BasicArrayVector(new int[]{3,5,9,11},biv);
        DolphinDBArray array1 = new DolphinDBArray(biav);
        Map<String, Class<?>> typeMap = new HashMap<>();
        // 向Map中添加键值对，键是字符串，值是Class对象
        typeMap.put("integer", Integer.class);
        String re = null;
        try{
            array1.getArray(typeMap);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Type mapping is not supported in getArray(Map)", re);
    }

    @Test
    public void Test_DolphinDBArray_getArray_map_null() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        Map<String, Class<?>> typeMap = new HashMap<>();
        Object subArray = array1.getArray(1,1,typeMap);
        Object[] tags = (Object[]) subArray;
        Assert.assertEquals("3", tags[0].toString());
    }

    @Test
    public void Test_DolphinDBArray_getArray_index_less_1() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        BasicArrayVector biav = new BasicArrayVector(new int[]{3,5,9,11},biv);
        DolphinDBArray array1 = new DolphinDBArray(biav);
        String re = null;
        try{
            Object subArray = array1.getArray(0,2);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Index out of bounds: 0", re);
    }

    @Test
    public void Test_DolphinDBArray_getArray_index_more_then_rows() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        BasicArrayVector biav = new BasicArrayVector(new int[]{3,5,9,11},biv);
        DolphinDBArray array1 = new DolphinDBArray(biav);
        String re = null;
        try{
            Object subArray = array1.getArray(6,2);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Index out of bounds: 6", re);
    }

    @Test
    public void Test_DolphinDBArray_getArray_count_less_0() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        BasicArrayVector biav = new BasicArrayVector(new int[]{3,5,9,11},biv);
        DolphinDBArray array1 = new DolphinDBArray(biav);
        String re = null;
        try{
            Object subArray = array1.getArray(1,-1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Count cannot be negative: -1", re);
    }

    @Test
    public void Test_DolphinDBArray_getArray_index_count() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        Object subArray = array1.getArray(4,2);
        Object[] tags = (Object[]) subArray;
        Assert.assertEquals("7", tags[0].toString());
        Assert.assertEquals("8", tags[1].toString());
    }

    @Test
    public void Test_DolphinDBArray_getArray_index_count_map_not_null() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("integer", Integer.class);
        String re = null;
        try{
            array1.getArray(1,1,typeMap);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Type mapping is not supported in getArray(long, int, Map)", re);
    }

    @Test
    public void Test_DolphinDBArray_getArray_index_count_map_null() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        Map<String, Class<?>> typeMap = new HashMap<>();
        Object subArray = array1.getArray(4,2,typeMap);
        Object[] tags = (Object[]) subArray;
        Assert.assertEquals("7", tags[0].toString());
        Assert.assertEquals("8", tags[1].toString());
    }

    @Test
    public void Test_DolphinDBArray_getResultSet() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        String re = null;
        try{
            array1.getResultSet();
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The current method is not supported.", re);
    }

    @Test
    public void Test_DolphinDBArray_getResultSet_map() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("integer", Integer.class);
        String re = null;
        try{
            array1.getResultSet(typeMap);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The current method is not supported.", re);
    }

    @Test
    public void Test_DolphinDBArray_getResultSet_index_count() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        String re = null;
        try{
            array1.getResultSet(1,1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The current method is not supported.", re);
    }

    @Test
    public void Test_DolphinDBArray_getResultSet_index_count_map() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("integer", Integer.class);
        String re = null;
        try{
            array1.getResultSet(1,1,typeMap);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The current method is not supported.", re);
    }

    @Test
    public void Test_DolphinDBArray_free() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        DolphinDBArray array1 = new DolphinDBArray(biv);
        array1.free();
        String re = null;
        try{
            array1.getArray();
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("This DolphinDBArray object has been freed.", re);
    }
}
