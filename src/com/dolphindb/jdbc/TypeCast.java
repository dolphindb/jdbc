package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.*;

public class TypeCast {

    private static final String PACKAGE_NAME = "com.xxdb.data.";

    private static final String VECTOR = "Vector";

    private static final String BASIC_ANY_VECTOR = PACKAGE_NAME + "BasicAny" + VECTOR;

    // dateTime
    public static final String BASIC_MONTH = PACKAGE_NAME + "BasicMonth";
    public static final String BASIC_DATE = PACKAGE_NAME + "BasicDate";
    public static final String BASIC_TIME = PACKAGE_NAME + "BasicTime";
    public static final String BASIC_MINUTE = PACKAGE_NAME + "BasicMinute";
    public static final String BASIC_SECOND= PACKAGE_NAME + "BasicSecond";
    public static final String BASIC_NANOTIME = PACKAGE_NAME + "BasicNanoTime";
    public static final String BASIC_TIMESTAMP = PACKAGE_NAME + "BasicTimestamp";
    public static final String BASIC_DATETIME = PACKAGE_NAME + "BasicDateTime";
    public static final String BASIC_NANOTIMESTAMP = PACKAGE_NAME + "BasicNanoTimestamp";

    public static final String BASIC_MONTH_VECTOR = BASIC_MONTH + VECTOR;
    public static final String BASIC_DATE_VECTOR = BASIC_DATE + VECTOR;
    public static final String BASIC_TIME_VECTOR = BASIC_TIME + VECTOR;
    public static final String BASIC_MINUTE_VECTOR = BASIC_MINUTE + VECTOR;
    public static final String BASIC_SECOND_VECTOR = BASIC_SECOND + VECTOR;
    public static final String BASIC_NANOTIME_VECTOR = BASIC_NANOTIME + VECTOR;
    public static final String BASIC_TIMESTAMP_VECTOR = BASIC_TIMESTAMP + VECTOR;
    public static final String BASIC_DATETIME_VECTOR = BASIC_DATETIME + VECTOR;
    public static final String BASIC_NANOTIMESTAMP_VECTOR = BASIC_NANOTIMESTAMP + VECTOR;

    public static final String DATE = "java.sql.Date";
    public static final String TIME = "java.sql.Time";
    public static final String TIMESTAMP = "java.sql.Timestamp";
    public static final String LOCAL_DATE = "java.time.LocalDate";
    public static final String LOCAL_TIME = "java.time.LocalTime";
    public static final String LOCAL_DATETIME = "java.time.LocalDateTime";
    public static final String YEAR_MONTH = "java.time.YearMonth";

    public static final int YEAR = 1970;
    public static final int MONTH = 1;
    public static final int DAY = 1;
    public static final int HOUR = 0;
    public static final int MINUTE = 0;
    public static final int SECOND = 0;
    public static final int NANO = 0;

    public static final YearMonth YEARMONTH = YearMonth.of(YEAR,MONTH);
    public static final LocalTime LOCALTIME = LocalTime.of(HOUR,MINUTE,SECOND,NANO);
    public static final LocalDate LOCALDATE = LocalDate.of(YEAR,MONTH,DAY);
    public static final LocalDateTime LOCALDATETIME = LocalDateTime.of(LOCALDATE,LOCALTIME);


    //basicType
    public static final String BASIC_VOID = PACKAGE_NAME + "Void";
    public static final String BASIC_BOOLEAN = PACKAGE_NAME + "BasicBoolean";
    public static final String BASIC_BYTE = PACKAGE_NAME + "BasicByte";
    public static final String BASIC_SHORT = PACKAGE_NAME + "BasicShort";
    public static final String BASIC_INT = PACKAGE_NAME + "BasicInt";
    public static final String BASIC_LONG= PACKAGE_NAME + "BasicLong";
    public static final String BASIC_FLOAT = PACKAGE_NAME + "BasicFloat";
    public static final String BASIC_DOUBLE = PACKAGE_NAME + "BasicDouble";
    public static final String BASIC_STRING = PACKAGE_NAME + "BasicString";
    public static final String BASIC_COMPLEX = PACKAGE_NAME + "BasicComplex";
    public static final String BASIC_DATEHOUR = PACKAGE_NAME + "BasicDateHour";
    public static final String BASIC_DURATION = PACKAGE_NAME + "BasicDuration";
    public static final String BASIC_INT128 = PACKAGE_NAME + "BasicInt128";
    public static final String BASIC_IPADDR = PACKAGE_NAME + "BasicIPAddr";
    public static final String BASIC_POINT = PACKAGE_NAME + "BasicPoint";
    public static final String BASIC_UUID = PACKAGE_NAME + "BasicUuid";
    public static final String BASIC_SYMBOL = PACKAGE_NAME + "BasicString";
    public static final String BASIC_BLOB = PACKAGE_NAME + "BasicString";

    public static final String BASIC_BOOLEAN_VECTOR = BASIC_BOOLEAN + VECTOR;
    public static final String BASIC_BYTE_VECTOR = BASIC_BYTE + VECTOR;
    public static final String BASIC_SHORT_VECTOR = BASIC_SHORT + VECTOR;
    public static final String BASIC_INT_VECTOR = BASIC_INT + VECTOR;
    public static final String BASIC_LONG_VECTOR= BASIC_LONG + VECTOR;
    public static final String BASIC_FLOAT_VECTOR = BASIC_FLOAT + VECTOR;
    public static final String BASIC_DOUBLE_VECTOR = BASIC_DOUBLE + VECTOR;
    public static final String BASIC_STRING_VECTOR = BASIC_STRING + VECTOR;

    public static final String BOOLEAN = "java.lang.Boolean";
    public static final String BYTE = "java.lang.Byte";
    public static final String CHAR = "java.lang.Character";
    public static final String SHORT = "java.lang.Short";
    public static final String INT = "java.lang.Integer";
    public static final String LONG = "java.lang.Long";
    public static final String FLOAT = "java.lang.Float";
    public static final String DOUBLE = "java.lang.Double";
    public static final String STRING = "java.lang.String";


    public static final String BOOLEANARR = "[Z";
    public static final String BYTEARR = "[B";
    public static final String CHARARR = "[C";
    public static final String SHORTARR = "[S";
    public static final String INTARR = "[I";
    public static final String LONGARR = "[J";
    public static final String FLOATARR = "[F";
    public static final String DOUBLEARR = "[D";



    public static final HashMap<Integer,String> TYPEINT2STRING = new LinkedHashMap<>();
	public static Scalar[] NULL = {new BasicBoolean(false), new BasicByte((byte) 0), new BasicShort((short) 0), new BasicInt(0),
            new BasicLong(0), new BasicFloat(0), new BasicDouble(0), new BasicString(""),
            new BasicDate(LocalDate.of(2020,1,1)), new BasicTimestamp(LocalDateTime.of(2020, 1, 1, 0, 0)),
            new BasicTime(LocalTime.of(0, 0)), new BasicNanoTime(LocalTime.of(0,0,0,0)),
            new BasicNanoTimestamp(LocalDateTime.of(2020,1,1,0,0,0,0)),
            new BasicString("", true), new BasicDateHour(LocalDateTime.of(2020,1,1,0,0,0)),
            new BasicComplex(0.0, 0.0), new BasicDuration(Entity.DURATION.NS, 1), new BasicInt128((long) 1,(long) 1),
            new BasicIPAddr((long)1, (long)1), new BasicPoint(1.0, 1.0), new BasicUuid((long)1, (long)1), new BasicMonth(2020, Month.JANUARY),
            new BasicSecond(LocalTime.of(0,0,0)), new BasicMinute(LocalTime.of(0,0,0)),
            new BasicDateTime(LocalDateTime.of(2020,1,1,0,0,0))};

    static {
        String[] arr = new String[]{
                BASIC_VOID,
                BASIC_BOOLEAN,
                BASIC_BYTE,
                BASIC_SHORT,
                BASIC_INT,
                BASIC_LONG,
                BASIC_DATE,
                BASIC_MONTH,
                BASIC_TIME,
                BASIC_MINUTE,
                BASIC_SECOND,
                BASIC_DATETIME,
                BASIC_TIMESTAMP,
                BASIC_NANOTIME,
                BASIC_NANOTIMESTAMP,
                BASIC_FLOAT,
                BASIC_DOUBLE,
                BASIC_SYMBOL,
                BASIC_STRING,
                BASIC_UUID,
                BASIC_DATEHOUR,
                BASIC_IPADDR,
                BASIC_INT128,
                BASIC_BLOB,
                BASIC_COMPLEX,
                BASIC_POINT,
                BASIC_DURATION,
        };
        Entity.DATA_TYPE[] datatypeArr = new Entity.DATA_TYPE[]{
                Entity.DATA_TYPE.DT_VOID,
                Entity.DATA_TYPE.DT_BOOL,
                Entity.DATA_TYPE.DT_BYTE,
                Entity.DATA_TYPE.DT_SHORT,
                Entity.DATA_TYPE.DT_INT,
                Entity.DATA_TYPE.DT_LONG,
                Entity.DATA_TYPE.DT_DATE,
                Entity.DATA_TYPE.DT_MONTH,
                Entity.DATA_TYPE.DT_TIME,
                Entity.DATA_TYPE.DT_MINUTE,
                Entity.DATA_TYPE.DT_SECOND,
                Entity.DATA_TYPE.DT_DATETIME,
                Entity.DATA_TYPE.DT_TIMESTAMP,
                Entity.DATA_TYPE.DT_NANOTIME,
                Entity.DATA_TYPE.DT_NANOTIMESTAMP,
                Entity.DATA_TYPE.DT_FLOAT,
                Entity.DATA_TYPE.DT_DOUBLE,
                Entity.DATA_TYPE.DT_SYMBOL,
                Entity.DATA_TYPE.DT_STRING,
                Entity.DATA_TYPE.DT_UUID,
                Entity.DATA_TYPE.DT_DATEHOUR,
                Entity.DATA_TYPE.DT_IPADDR,
                Entity.DATA_TYPE.DT_INT128,
                Entity.DATA_TYPE.DT_BLOB,
                Entity.DATA_TYPE.DT_COMPLEX,
                Entity.DATA_TYPE.DT_POINT,
                Entity.DATA_TYPE.DT_DURATION
        };
        for(int i = 0, len = datatypeArr.length; i < len; ++i) {
            TYPEINT2STRING.put(datatypeArr[i].getValue(), arr[i]);
        }
    	for (Scalar n : NULL) {
			n.setNull();
		}
    }

    public static Object nullScalar(Entity.DATA_TYPE type) throws SQLException {
    	Object x = null;
		switch (type) {
            case DT_BOOL: x = NULL[0]; break;
            case DT_BYTE: x = NULL[1]; break;
            case DT_SHORT: x = NULL[2]; break;
            case DT_INT: x = NULL[3]; break;
            case DT_LONG: x = NULL[4]; break;
            case DT_FLOAT: x = NULL[5]; break;
            case DT_DOUBLE: x = NULL[6]; break;
            case DT_SYMBOL:
            case DT_STRING: x = NULL[7]; break;
            case DT_DATE: x = NULL[8]; break;
            case DT_TIMESTAMP: x = NULL[9]; break;
            case DT_TIME: x = NULL[10]; break;
            case DT_NANOTIME: x = NULL[11]; break;
            case DT_NANOTIMESTAMP: x = NULL[12]; break;
            case DT_BLOB: x = NULL[13]; break;
            case DT_DATEHOUR: x = NULL[14]; break;
            case DT_COMPLEX: x = NULL[15]; break;
            case DT_DURATION: x = NULL[16]; break;
            case DT_INT128: x = NULL[17]; break;
            case DT_IPADDR: x = NULL[18]; break;
            case DT_POINT: x = NULL[19]; break;
            case DT_MONTH: x = NULL[21]; break;
            case DT_SECOND: x = NULL[22]; break;
            case DT_MINUTE: x = NULL[23]; break;
            case DT_UUID: x = NULL[20]; break;
            case DT_DATETIME: x = NULL[24]; break;
		    default: throw new SQLException("Unsupported type");
		}
		return x;
    }

    public static String castDbString(Object o){
        String srcClassName = o.getClass().getName();
        switch (srcClassName){
            case BASIC_STRING:
                return "\""+o+"\"";
            case CHAR:
                return "'"+ o +"'";
            case DATE:
                return new BasicDate(((Date) o).toLocalDate()).toString();
            case TIME:
                return new BasicNanoTime(((Time) o).toLocalTime()).toString();
            case TIMESTAMP:
                return new BasicNanoTimestamp(((Timestamp) o).toLocalDateTime()).toString();
            case YEAR_MONTH:
                return new BasicMonth((YearMonth) o).toString();
            case LOCAL_DATE:
                return new BasicDate((LocalDate) o).toString();
            case LOCAL_TIME:
                return new BasicNanoTime((LocalTime) o).toString();
            case LOCAL_DATETIME:
                return new BasicNanoTimestamp((LocalDateTime) o).toString();
            case STRING:
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BASIC_BOOLEAN:
            case BASIC_BYTE:
            case BASIC_SHORT:
            case BASIC_INT:
            case BASIC_LONG:
            case BASIC_DATE:
            case BASIC_MONTH:
            case BASIC_TIME:
            case BASIC_MINUTE:
            case BASIC_SECOND:
            case BASIC_DATETIME:
            case BASIC_TIMESTAMP:
            case BASIC_NANOTIME:
            case BASIC_NANOTIMESTAMP:
            case BASIC_FLOAT:
            case BASIC_DOUBLE:
            case BASIC_DURATION:
                return o.toString();
            case BASIC_DATEHOUR:
                int value = ((BasicDateHour)o).getInt();
                return "datehour(" + value + ")";
            case BASIC_UUID:
                return "uuid(\"" + o.toString() + "\")";
            case BASIC_IPADDR:
                return "ipaddr(\"" + o.toString() + "\")";
            case BASIC_INT128:
                return "int128(\"" + o.toString() + "\")";
            case BASIC_COMPLEX:
                double x = ((BasicComplex)o).getReal();
                double y = ((BasicComplex)o).getImage();
                return "complex(" + x + "," + y + ")";
            case BASIC_POINT:
                double a = ((BasicPoint)o).getX();
                double b = ((BasicPoint)o).getY();
                return "point(" + a + "," + b + ")";
            default:
                return null;
        }
    }

    public static Entity java2db(Object srcValue, String targetEntityClassName) throws IOException {
        String srcValueClassName = srcValue.getClass().getName();
        Entity castEntity = null;
        if(srcValueClassName.equals(targetEntityClassName) || srcValueClassName.startsWith(targetEntityClassName)){
            return (Entity) srcValue;
        }

        try {
            castEntity = dateTimeCast(srcValue,targetEntityClassName);
        }catch (IOException e){
            throw e;
        }catch (Exception e){
            throw new IOException(e);
        }

        if(castEntity != null) return castEntity;
        castEntity = basicTypeCast(srcValue,targetEntityClassName);
        if(castEntity != null) return castEntity;
        throw new IOException("only support bool byte char short int long float double Object[] List Date Time Timestamp YearMoth LocalDate LocalTime LocalDateTime Scalar Vector");
    }

    public static Entity dateTimeCast(Object srcValue, String targetEntityClassName) throws Exception{
        Entity castEntity;
        if(srcValue instanceof Scalar || srcValue instanceof Vector) {
            castEntity = dateTime_db2db((Entity) srcValue, targetEntityClassName);
            if (castEntity != null) return castEntity;
        }

        castEntity = dataTime_java2db(srcValue,targetEntityClassName);
        if(castEntity != null) return castEntity;

        if(srcValue instanceof List){
            castEntity = dateTimeArr2Vector(((List) srcValue).toArray(),targetEntityClassName);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof Object[]){
            castEntity = dateTimeArr2Vector((Object[]) srcValue,targetEntityClassName);
            if (castEntity != null) return castEntity;
        }
        return null;
    }

    public static Entity basicTypeCast(Object srcValue, String targetEntityClassName) throws IOException{
        Entity castEntity;

        if(srcValue instanceof Scalar || srcValue instanceof Vector) {
            castEntity = basicType_db2db((Entity) srcValue, targetEntityClassName);
            if (castEntity != null) return castEntity;
        }

        castEntity = basicType_java2db(srcValue,targetEntityClassName);
        if(castEntity != null) return castEntity;

        if(srcValue instanceof List){
            castEntity = basicTypeArr2Vector(((List) srcValue).toArray(),targetEntityClassName);
            if (castEntity != null) return castEntity;
        }


        if(srcValue instanceof Object[]){
            castEntity = basicTypeArr2Vector((Object[])srcValue,targetEntityClassName);
            if (castEntity != null) return castEntity;
        }
        return null;
    }

    public static Entity dataTime_java2db(Object srcValue, String targetEntityClassName) throws IOException{
        if(srcValue instanceof Entity) return null;

        String srcEntityClassName = srcValue.getClass().getName();

        if(!CheckedDateTime(srcEntityClassName,targetEntityClassName)) {
            return null;
        }
        Temporal temporal = null;
        switch (srcEntityClassName){
            case DATE:
                temporal = ((Date) srcValue).toLocalDate();
                break;
            case TIME:
                temporal = ((Time) srcValue).toLocalTime();
                break;
            case TIMESTAMP:
                temporal = ((Timestamp) srcValue).toLocalDateTime();
                break;
            case LOCAL_DATE:
                temporal = (LocalDate)srcValue;
                break;
            case LOCAL_TIME:
                temporal = (LocalTime)srcValue;
                break;
            case LOCAL_DATETIME:
                temporal = (LocalDateTime)srcValue;
                break;
            case YEAR_MONTH:
                temporal = (YearMonth)srcValue;
                break;
            default:
                return null;
        }

        return Temporal2dateTime(temporal,srcEntityClassName,targetEntityClassName);
    }

    public static boolean CheckedDateTime(String srcEntityClassName, String targetEntityClassName) throws IOException{
        switch (srcEntityClassName){
            case BASIC_MONTH:
            case BASIC_DATE:
            case BASIC_TIME:
            case BASIC_MINUTE:
            case BASIC_SECOND:
            case BASIC_NANOTIME:
            case BASIC_TIMESTAMP:
            case BASIC_DATETIME:
            case BASIC_NANOTIMESTAMP:
            case BASIC_MONTH_VECTOR:
            case BASIC_DATE_VECTOR:
            case BASIC_TIME_VECTOR:
            case BASIC_MINUTE_VECTOR:
            case BASIC_SECOND_VECTOR:
            case BASIC_NANOTIME_VECTOR:
            case BASIC_TIMESTAMP_VECTOR:
            case BASIC_DATETIME_VECTOR:
            case BASIC_NANOTIMESTAMP_VECTOR:
            case BASIC_ANY_VECTOR:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case YEAR_MONTH:
            case LOCAL_TIME:
            case LOCAL_DATE:
            case LOCAL_DATETIME:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return true;
                    default:
                        throw new IOException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            default:
                return false;
        }
    }

    public static Entity Tempos2dateTime(Object srcTempos[], String srcEntityClassName, String targetEntityClassName) throws IOException{
        int size = srcTempos.length;
        switch (targetEntityClassName) {
            case BASIC_MONTH: {
                BasicMonthVector targetVector = new BasicMonthVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setMonth(index, (YearMonth) castTemporal(srcTemporal, YEAR_MONTH));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_DATE:{
                BasicDateVector targetVector = new BasicDateVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setDate(index, (LocalDate) castTemporal(srcTemporal, LOCAL_DATE));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_TIME:{
                BasicTimeVector targetVector = new BasicTimeVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setTime(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_MINUTE:{
                BasicMinuteVector targetVector = new BasicMinuteVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setMinute(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_SECOND:{
                BasicSecondVector targetVector = new BasicSecondVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setSecond(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_NANOTIME:{
                BasicNanoTimeVector targetVector = new BasicNanoTimeVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setNanoTime(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_TIMESTAMP: {
                BasicTimestampVector targetVector = new BasicTimestampVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setTimestamp(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_DATETIME:{
                BasicDateTimeVector targetVector = new BasicDateTimeVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setDateTime(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_NANOTIMESTAMP:{
                BasicNanoTimestampVector targetVector = new BasicNanoTimestampVector(size);
                int index = 0;
                for (Object srcTemporal : srcTempos) {
                    targetVector.setNanoTimestamp(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            default:
                throw new IOException(srcEntityClassName + " can not cast " + targetEntityClassName);
        }
    }

    public static Entity Temporal2dateTime(Temporal srcTemporal, String srcEntityClassName, String targetEntityClassName) throws IOException{
        switch (targetEntityClassName) {
            case BASIC_MONTH:
                return new BasicMonth((YearMonth) castTemporal(srcTemporal,YEAR_MONTH));
            case BASIC_DATE:
                return new BasicDate((LocalDate) castTemporal(srcTemporal,LOCAL_DATE));
            case BASIC_TIME:
                return new BasicTime((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_MINUTE:
                return new BasicMinute((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_SECOND:
                return new BasicSecond((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_NANOTIME:
                return new BasicNanoTime((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_TIMESTAMP:
                return new BasicTimestamp((LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
            case BASIC_DATETIME:
                return new BasicDateTime((LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
            case BASIC_NANOTIMESTAMP:
                return new BasicNanoTimestamp((LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
            default:
                throw new IOException(srcEntityClassName + " can not cast " + targetEntityClassName);
        }
    }

    public static Entity dateTime_db2db(Entity srcEntity, String targetEntityClassName) throws Exception{
        String srcEntityClassName = null;
        if(srcEntity.isScalar()){
            Temporal srcTemporal = null;
            srcEntityClassName = srcEntity.getClass().getName();
            if(!CheckedDateTime(srcEntityClassName,targetEntityClassName))
                return null;
            switch (srcEntityClassName) {
                case BASIC_MONTH:
                case BASIC_DATE:
                case BASIC_TIME:
                case BASIC_MINUTE:
                case BASIC_SECOND:
                case BASIC_NANOTIME:
                case BASIC_TIMESTAMP:
                case BASIC_DATETIME:
                case BASIC_NANOTIMESTAMP:
                    srcTemporal = ((Scalar)srcEntity).getTemporal();
                    return Temporal2dateTime(srcTemporal, srcEntityClassName, targetEntityClassName);

                default:
                    throw new IOException(srcEntityClassName + " can not cast " + targetEntityClassName);

            }
        }else if (srcEntity.isVector()){
            if(srcEntity.rows() == 0) throw new IOException("Vector rows can not 0");
            srcEntityClassName = srcEntity.getClass().getName();
            String srcScalarFromVectorClassName = ((Vector) srcEntity).get(0).getClass().getName();
            if(!CheckedDateTime(srcScalarFromVectorClassName,targetEntityClassName))
                return null;
            Temporal[] srcTempos = null;
            switch (srcEntityClassName){
                case BASIC_MONTH_VECTOR:
                case BASIC_DATE_VECTOR:
                case BASIC_TIME_VECTOR:
                case BASIC_MINUTE_VECTOR:
                case BASIC_SECOND_VECTOR:
                case BASIC_NANOTIME_VECTOR:
                case BASIC_TIMESTAMP_VECTOR:
                case BASIC_DATETIME_VECTOR:
                case BASIC_NANOTIMESTAMP_VECTOR: {
                    int size = srcEntity.rows();
                    srcTempos = new Temporal[size];
                    for (int i = 0; i < size; ++i) {
                        srcTempos[i] = ((Scalar)((Vector) srcEntity).get(i)).getTemporal();

                    }
                    return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
                }
                case BASIC_ANY_VECTOR: {
                    if (!CheckedDateTime(srcScalarFromVectorClassName, targetEntityClassName)) {
                        return null;
                    }
                    switch (srcScalarFromVectorClassName) {
                        case BASIC_MONTH:
                        case BASIC_DATE:
                        case BASIC_TIME:
                        case BASIC_MINUTE:
                        case BASIC_SECOND:
                        case BASIC_NANOTIME:
                        case BASIC_TIMESTAMP:
                        case BASIC_DATETIME:
                        case BASIC_NANOTIMESTAMP:
                            int size = srcEntity.rows();
                            srcTempos = new Temporal[size];
                            for (int i = 0; i < size; ++i) {
                                srcTempos[i] = ((Scalar)((Vector) srcEntity).get(i)).getTemporal();
                            }
                            return Tempos2dateTime(srcTempos, srcScalarFromVectorClassName, targetEntityClassName);
                        default:
                            throw new IOException(srcScalarFromVectorClassName + " can not cast " + targetEntityClassName);
                    }
                }
                default:
                    return null;
            }
        }else {
            throw new IOException(srcEntity.getClass().getName() + " can not cast " +srcEntityClassName);
        }

    }

    public static Entity dateTimeArr2Vector(Object srcValue[],String targetEntityClassName) throws IOException{
        int size = srcValue.length;
        if(size == 0){
            throw new IOException(srcValue + "size can not 0 ");
        }
        Object srcValueFromArr = srcValue[0];
        String srcValueFromListClassName = srcValueFromArr.getClass().getName();
        if(srcValueFromArr instanceof Scalar){
            throw new IOException("you need use com.xxdb.data.Vector load com.xxdb.data.Scalar");
        }
        if(!CheckedDateTime(srcValueFromListClassName,targetEntityClassName)) return null;
        return Tempos2dateTime(srcValue,srcValueFromListClassName,targetEntityClassName);
    }

    public static boolean CheckedBasicType(String srcEntityClassName, String targetEntityClassName) throws IOException{
        switch (srcEntityClassName){
            case BASIC_BOOLEAN:
            case BASIC_BYTE:
            case BASIC_INT:
            case BASIC_SHORT:
            case BASIC_LONG:
            case BASIC_FLOAT:
            case BASIC_DOUBLE:
            case BASIC_STRING:
            case BASIC_BOOLEAN_VECTOR:
            case BASIC_BYTE_VECTOR:
            case BASIC_INT_VECTOR:
            case BASIC_SHORT_VECTOR:
            case BASIC_LONG_VECTOR:
            case BASIC_FLOAT_VECTOR:
            case BASIC_DOUBLE_VECTOR:
            case BASIC_STRING_VECTOR:
            case BASIC_ANY_VECTOR:
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BOOLEANARR:
            case BYTEARR:
            case CHARARR:
            case SHORTARR:
            case INTARR:
            case LONGARR:
            case FLOATARR:
            case DOUBLEARR:

                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return true;
                    default:
                        throw new IOException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            default:
                return false;
        }
    }

    public static Entity basicTypeArr2Vector(Object srcValue[],String targetEntityClassName) throws IOException{
        int size = srcValue.length;
        if(size == 0){
            throw new IOException(srcValue + "size can not 0 ");
        }
        Object srcValueFromArr = srcValue[0];
        String srcValueFromListClassName = srcValueFromArr.getClass().getName();
        if(srcValueFromArr instanceof Scalar){
            throw new IOException("you need use com.xxdb.data.Vector load com.xxdb.data.Scalar");
        }
        if(!CheckedBasicType(srcValueFromListClassName,targetEntityClassName)) return null;


        switch (srcValueFromListClassName){
            case BOOLEAN:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicBooleanVector targetVector = new BasicBooleanVector(size);
                        int index = 0;
                        if(srcValue instanceof Boolean[]){
                            Boolean[] booleans = (Boolean[]) srcValue;
                            for(boolean item : booleans){
                                targetVector.setBoolean(index,item);
                                ++index;
                            }
                        }else{
                            for(Object item : srcValue){
                                targetVector.setBoolean(index,(boolean)item);
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);

                }
            case BYTE:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicByteVector targetVector = new BasicByteVector(size);
                        int index = 0;
                        if(srcValue instanceof Byte[]){
                            Byte[] bytes = (Byte[]) srcValue;
                            for(byte item : bytes){
                                targetVector.setByte(index,item);
                                ++index;
                            }
                        }else{
                            for (Object item : srcValue) {
                                targetVector.setByte(index, (byte) item);
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case CHAR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicByteVector targetVector = new BasicByteVector(size);
                        int index = 0;
                        if(srcValue instanceof Character[]){
                            Character[] characters = (Character[]) srcValue;
                            for(char item : characters){
                                targetVector.setByte(index,(byte) (item & 0XFF));
                                ++index;
                            }
                        }else{
                            for (Object item : srcValue) {
                                targetVector.setByte(index, (byte) ((char) item & 0XFF));
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case INT:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicIntVector targetVector = new BasicIntVector(size);
                        int index = 0;
                        if(srcValue instanceof Integer[]){
                            Integer[] integers = (Integer[]) srcValue;
                            for(int item : integers){
                                targetVector.setInt(index,item);
                                ++index;
                            }
                        }else{
                            for (Object item : srcValue) {
                                targetVector.setInt(index, (int) item);
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case SHORT:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicShortVector targetVector = new BasicShortVector(size);
                        int index = 0;
                        if(srcValue instanceof Short[]){
                            Short[] shorts = (Short[]) srcValue;
                            for(short item : shorts){
                                targetVector.setShort(index,item);
                                ++index;
                            }
                        }else{
                            for (Object item : srcValue) {
                                targetVector.setShort(index, (short) item);
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case LONG:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicLongVector targetVector = new BasicLongVector(size);
                        int index = 0;
                        if(srcValue instanceof Long[]){
                            Long[] longs = (Long[]) srcValue;
                            for(long item : longs){
                                targetVector.setLong(index,item);
                                ++index;
                            }
                        }else{
                            for (Object item : srcValue) {
                                targetVector.setLong(index, (Long) item);
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case FLOAT:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicFloatVector targetVector = new BasicFloatVector(size);
                        int index = 0;
                        if(srcValue instanceof Float[]){
                            Float[] floats = (Float[]) srcValue;
                            for(float item : floats){
                                targetVector.setFloat(index,item);
                                ++index;
                            }
                        }else{
                            for (Object item : srcValue) {
                                targetVector.setFloat(index, (float) item);
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case DOUBLE:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicDoubleVector targetVector = new BasicDoubleVector(size);
                        int index = 0;
                        if(srcValue instanceof Double[]){
                            Double[] doubles = (Double[]) srcValue;
                            for(double item : doubles){
                                targetVector.setDouble(index,item);
                                ++index;
                            }
                        }else{
                            for (Object item : srcValue) {
                                targetVector.setDouble(index, (double) item);
                                ++index;
                            }
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case STRING:
                switch (targetEntityClassName){
                    case BASIC_STRING:
                        if(srcValue instanceof String[]){
                            String[] strings = (String[]) srcValue;
                            return new BasicStringVector(strings);
                        }
                        BasicStringVector targetVector = new BasicStringVector(size);
                        int index = 0;
                        for (Object item : srcValue) {
                            targetVector.setString(index, (String) item);
                            ++index;
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            default:
                return null;
        }
    }


    public static Entity basicType_db2db(Entity srcEntity, String targetEntityClassName) throws IOException{
        String srcEntityClassName = null;
        if(srcEntity.isScalar()){
            srcEntityClassName = srcEntity.getClass().getName();
            if(!CheckedBasicType(srcEntityClassName,targetEntityClassName))
                return null;
            switch (srcEntityClassName) {
                case BASIC_BOOLEAN:
                case BASIC_BYTE:
                case BASIC_INT:
                case BASIC_SHORT:
                case BASIC_LONG:
                case BASIC_FLOAT:
                case BASIC_DOUBLE:
                    switch (targetEntityClassName){
                        case BASIC_BOOLEAN:
                        case BASIC_BYTE:
                        case BASIC_INT:
                        case BASIC_SHORT:
                        case BASIC_LONG:
                        case BASIC_FLOAT:
                        case BASIC_DOUBLE:
                        case BASIC_STRING:
                            return srcEntity;
                        default:
                            throw new IOException(srcEntityClassName + " can not cast " + targetEntityClassName);
                    }
                case BASIC_STRING:
                    switch (targetEntityClassName){
                        case BASIC_STRING:
                            return srcEntity;
                        default:
                            throw new IOException(srcEntityClassName + " can not cast to " + targetEntityClassName);
                    }
                default:
                    return null;
            }
        }else if (srcEntity.isVector()) {
            if (srcEntity.rows() == 0) throw new IOException("Vector rows can not 0");
            srcEntityClassName = srcEntity.getClass().getName();
            String srcScalarFromVectorClassName = ((Vector) srcEntity).get(0).getClass().getName();
            if (!CheckedBasicType(srcEntityClassName, targetEntityClassName))
                return null;
            switch (srcEntityClassName){
                case BASIC_BOOLEAN_VECTOR:
                case BASIC_BYTE_VECTOR:
                case BASIC_INT_VECTOR:
                case BASIC_SHORT_VECTOR:
                case BASIC_LONG_VECTOR:
                case BASIC_FLOAT_VECTOR:
                case BASIC_DOUBLE_VECTOR:
                    switch (targetEntityClassName) {
                        case BASIC_BOOLEAN:
                        case BASIC_BYTE:
                        case BASIC_INT:
                        case BASIC_SHORT:
                        case BASIC_LONG:
                        case BASIC_FLOAT:
                        case BASIC_DOUBLE:
                        case BASIC_STRING:
                            return srcEntity;
                        default:
                            throw new IOException(srcEntityClassName + " can not cast " + targetEntityClassName);
                    }
                case BASIC_STRING_VECTOR:
                    switch (targetEntityClassName){
                        case BASIC_STRING:
                            return srcEntity;
                        default:
                            throw new IOException(srcEntityClassName + " can not cast to " + targetEntityClassName);
                    }
                case BASIC_ANY_VECTOR:
                    switch (srcScalarFromVectorClassName){
                        case BASIC_BOOLEAN:
                        case BASIC_BYTE:
                        case BASIC_INT:
                        case BASIC_SHORT:
                        case BASIC_LONG:
                        case BASIC_FLOAT:
                        case BASIC_DOUBLE:
                            switch (targetEntityClassName){
                                case BASIC_BOOLEAN:
                                case BASIC_BYTE:
                                case BASIC_INT:
                                case BASIC_SHORT:
                                case BASIC_LONG:
                                case BASIC_FLOAT:
                                case BASIC_DOUBLE:
                                case BASIC_STRING:
                                    return srcEntity;
                                default:
                                    throw new IOException(srcScalarFromVectorClassName + " can not cast " + targetEntityClassName);
                            }
                        case BASIC_STRING:
                            switch (targetEntityClassName){
                                case BASIC_STRING:
                                    return srcEntity;
                                default:
                                    throw new IOException(srcScalarFromVectorClassName + " can not cast to " + targetEntityClassName);
                            }
                        default:
                            return null;
                    }
            }
        }else{
            throw new IOException(srcEntity.getClass().getName() + " can not cast " +srcEntityClassName);
        }
        return null;
    }

    public static Entity basicType_java2db(Object srcValue, String targetEntityClassName) throws IOException{
        if(srcValue instanceof Entity) return null;

        String srcValueClassName = srcValue.getClass().getName();

        if(!CheckedBasicType(srcValueClassName,targetEntityClassName)) return null;

        switch (srcValueClassName) {
            case BOOLEAN:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicBoolean((boolean) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case BYTE:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicByte((byte) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case CHAR:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicByte((byte) ((char) srcValue & 0xFF));
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case INT:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicInt((int) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case SHORT:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicShort((short) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case LONG:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicLong((long) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case FLOAT:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicFloat((float) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case DOUBLE:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicDouble((double) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case STRING:
                switch (targetEntityClassName) {
                    case BASIC_STRING:
                        return new BasicString((String) srcValue);
                    default:
                        throw new IOException(srcValueClassName + " can not cast to " + targetEntityClassName);
                }
            case BOOLEANARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        boolean[] booleans = (boolean[]) srcValue;
                        BasicBooleanVector targetVector = new BasicBooleanVector(booleans.length);
                        int index = 0;
                        for(boolean item : booleans){
                            targetVector.setBoolean(index,item);
                            ++index;
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            case BYTEARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicByteVector targetVector = new BasicByteVector((byte[])srcValue);
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            case CHARARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        char[] chars = (char[]) srcValue;
                        BasicByteVector targetVector = new BasicByteVector(chars.length);
                        int index = 0;
                        for(char item : chars){
                            targetVector.setByte(index,(byte)(item & 0xFF));
                            ++index;
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            case SHORTARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicShortVector targetVector = new BasicShortVector((short[])srcValue);
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            case INTARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicIntVector targetVector = new BasicIntVector((int[])srcValue);
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            case LONGARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicLongVector targetVector = new BasicLongVector((long[])srcValue);
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            case FLOATARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicFloatVector targetVector = new BasicFloatVector((float[])srcValue);
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            case DOUBLEARR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        BasicDoubleVector targetVector = new BasicDoubleVector((double[])srcValue);
                        return targetVector;
                    default:
                        throw new IOException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }

            default:
                return null;
        }
    }

    public static Temporal getTemporal(Object value) throws Exception{
        switch (value.getClass().getName()){
            case BASIC_MONTH:
            case BASIC_DATE:
            case BASIC_TIME:
            case BASIC_MINUTE:
            case BASIC_SECOND:
            case BASIC_NANOTIME:
            case BASIC_TIMESTAMP:
            case BASIC_DATETIME:
            case BASIC_NANOTIMESTAMP:
                return ((Scalar)value).getTemporal();
            case DATE:
                return new BasicDate(((Date) value).toLocalDate()).getTemporal();
            case TIME:
                return new BasicNanoTime(((Time) value).toLocalTime()).getTemporal();
            case TIMESTAMP:
                return new BasicNanoTimestamp(((Timestamp) value).toLocalDateTime()).getTemporal();
            case LOCAL_DATE:
                return new BasicDate(((LocalDate) value)).getTemporal();
            case LOCAL_TIME:
                return new BasicNanoTime((LocalTime) value).getTemporal();
            case LOCAL_DATETIME:
                return new BasicNanoTimestamp((LocalDateTime) value).getTemporal();
            case YEAR_MONTH:
                return new BasicMonth((YearMonth) value).getTemporal();
            default:
                return null;
        }
    }

    public static Temporal castTemporal(Object srcValue,String targetTemporalClassName){
        String srcTemporalClassName = srcValue.getClass().getName();
        Temporal srcTemporal = null;
        switch (srcTemporalClassName){
            case DATE:
                srcTemporal = ((Date)srcValue).toLocalDate();
                srcTemporalClassName = srcTemporal.getClass().getName();
                break;
            case TIME:
                srcTemporal = ((Time)srcValue).toLocalTime();
                srcTemporalClassName = srcTemporal.getClass().getName();
                break;
            case TIMESTAMP:
                srcTemporal = ((Timestamp)srcValue).toLocalDateTime();
                srcTemporalClassName = srcTemporal.getClass().getName();
                break;
            default:
                srcTemporal = (Temporal)srcValue;
        }
        switch (targetTemporalClassName){
            case YEAR_MONTH:
                switch (srcTemporalClassName){
                    case LOCAL_TIME:
                        return YEARMONTH;
                    case LOCAL_DATE:
                        LocalDate localDate = (LocalDate)srcTemporal;
                        return YearMonth.of(localDate.getYear(),localDate.getMonthValue());
                    case LOCAL_DATETIME:
                        LocalDateTime localDateTime = (LocalDateTime)srcTemporal;
                        return YearMonth.of(localDateTime.getYear(),localDateTime.getMonthValue());
                    default:
                        return srcTemporal;

                }
            case LOCAL_DATE:
                switch (srcTemporalClassName){
                    case YEAR_MONTH:
                        return  ((YearMonth) srcTemporal).atEndOfMonth();
                    case LOCAL_TIME:
                        return LOCALDATE;
                    case LOCAL_DATETIME:
                        return  ((LocalDateTime)srcTemporal).toLocalDate();
                    default:
                        return srcTemporal;

                }
            case LOCAL_TIME:
                switch (srcTemporalClassName){
                    case YEAR_MONTH:
                        return LOCALTIME;
                    case LOCAL_DATE:
                        return LOCALTIME;
                    case LOCAL_DATETIME:
                        return ((LocalDateTime)srcTemporal).toLocalTime();
                    default:
                        return srcTemporal;
                }
            case LOCAL_DATETIME:
                switch (srcTemporalClassName){
                    case YEAR_MONTH:
                        return ((YearMonth)srcTemporal).atEndOfMonth().atStartOfDay();
                    case LOCAL_DATE:
                        return  ((LocalDate)srcTemporal).atStartOfDay();
                    case LOCAL_TIME:
                        LocalTime localTime = (LocalTime)srcTemporal;
                        return LocalDateTime.of(YEAR,MONTH,DAY,localTime.getHour(),localTime.getMinute(),localTime.getSecond(),localTime.getNano());
                    default:
                        return srcTemporal;
                }
            default:
                return srcTemporal;
        }
    }
}
