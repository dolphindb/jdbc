package com.dolphindb.jdbc;

import com.xxdb.data.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

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


    static {
        String[] arr = new String[]{BASIC_VOID,
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
                BASIC_STRING,
                BASIC_STRING
        };
        for(int i = 0, len = arr.length; i < len; ++i) {
            TYPEINT2STRING.put(i, arr[i]);
        }
    }


    public static String castDbString(Object o){
        String srcClassName = o.getClass().getName();
        switch (srcClassName){
            case STRING:
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
                return o.toString();
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
        }catch (Exception e){
            throw new IOException("only support bool byte char short int long float double Object[] List Date Time Timestamp YearMoth LocalDate LocalTime LocalDateTime Scalar Vector");
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
            castEntity = dateTimeArr2Vector(srcValue,targetEntityClassName);
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
            castEntity = basicTypeArr2Vector(srcValue,targetEntityClassName);
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

    public static Entity Tempos2dateTime(Object srcTempos, String srcEntityClassName, String targetEntityClassName) throws IOException{
        Object[] objects = (Object[]) srcTempos;
        int size = objects.length;
        switch (targetEntityClassName) {
            case BASIC_MONTH: {
                BasicMonthVector targetVector = new BasicMonthVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setMonth(index, (YearMonth) castTemporal(srcTemporal, YEAR_MONTH));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_DATE:{
                BasicDateVector targetVector = new BasicDateVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setDate(index, (LocalDate) castTemporal(srcTemporal, LOCAL_DATE));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_TIME:{
                BasicTimeVector targetVector = new BasicTimeVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setTime(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_MINUTE:{
                BasicMinuteVector targetVector = new BasicMinuteVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setMinute(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_SECOND:{
                BasicSecondVector targetVector = new BasicSecondVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setSecond(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_NANOTIME:{
                BasicNanoTimeVector targetVector = new BasicNanoTimeVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setNanoTime(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_TIMESTAMP: {
                BasicTimestampVector targetVector = new BasicTimestampVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setTimestamp(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_DATETIME:{
                BasicDateTimeVector targetVector = new BasicDateTimeVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setDateTime(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_NANOTIMESTAMP:{
                BasicNanoTimestampVector targetVector = new BasicNanoTimestampVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
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
                        srcTempos[i] = ((Vector) srcEntity).get(i).getTemporal();

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
                                srcTempos[i] = ((Vector) srcEntity).get(i).getTemporal();
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

    public static Entity dateTimeArr2Vector(Object srcValue,String targetEntityClassName) throws IOException{
        Object[] srcArr = (Object[])srcValue;
        int size = srcArr.length;
        if(size == 0){
            throw new IOException(srcArr + "size can not 0 ");
        }
        Object srcValueFromArr = srcArr[0];
        String srcValueFromListClassName = srcValueFromArr.getClass().getName();
        if(srcValueFromArr instanceof Scalar){
            throw new IOException("you need use com.xxdb.data.Vector load com.xxdb.data.Scalar");
        }
        if(!CheckedDateTime(srcValueFromListClassName,targetEntityClassName)) return null;
        return Tempos2dateTime(srcArr,srcValueFromListClassName,targetEntityClassName);
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

    public static Entity basicTypeArr2Vector(Object srcValue,String targetEntityClassName) throws IOException{
        Object[] srcArr = (Object[])srcValue;
        int size = srcArr.length;
        if(size == 0){
            throw new IOException(srcArr + "size can not 0 ");
        }
        Object srcValueFromArr = srcArr[0];
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
                        for(boolean item : (Boolean[]) srcValue){
                            targetVector.setBoolean(index,item);
                            ++index;
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
                        for(byte item : (Byte[]) srcValue){
                            targetVector.setByte(index,item);
                            ++index;
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
                        for(char item : (Character[]) srcValue){
                            targetVector.setByte(index,(byte) (item & 0XFF));
                            ++index;
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
                        for(int item : (Integer[]) srcValue){
                            targetVector.setInt(index,item);
                            ++index;
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
                        for(short item : (Short[]) srcValue){
                            targetVector.setShort(index,item);
                            ++index;
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
                        for(long item : (Long[]) srcValue){
                            targetVector.setLong(index,item);
                            ++index;
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
                        for(float item : (Float[]) srcValue){
                            targetVector.setFloat(index,item);
                            ++index;
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
                        for(double item : (Double[]) srcValue){
                            targetVector.setDouble(index,item);
                            ++index;
                        }
                        return targetVector;
                    default:
                        throw new IOException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case STRING:
                switch (targetEntityClassName){
                    case BASIC_STRING:
                        Vector targetVector = new BasicStringVector((String[]) srcValue);
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
                return ((BasicMonth) value).getTemporal();
            case BASIC_DATE:
                return ((BasicDate) value).getTemporal();
            case BASIC_TIME:
                return ((BasicTime) value).getTemporal();
            case BASIC_MINUTE:
                return ((BasicMinute) value).getTemporal();
            case BASIC_SECOND:
                return ((BasicSecond) value).getTemporal();
            case BASIC_NANOTIME:
                return ((BasicNanoTime) value).getTemporal();
            case BASIC_TIMESTAMP:
                return ((BasicTimestamp) value).getTemporal();
            case BASIC_DATETIME:
                return ((BasicDateTime) value).getTemporal();
            case BASIC_NANOTIMESTAMP:
                return ((BasicNanoTimestamp) value).getTemporal();
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
