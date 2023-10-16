package com.dolphindb.jdbc;

import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.Entity;
import com.xxdb.data.Vector;

import java.util.ArrayList;
import java.util.List;

public class ColumnBindValue implements Comparable<ColumnBindValue> {
	/* current column index */
	private int index;

	/* current column name */
	private String colName;

	/* current column type */
	private Entity.DATA_TYPE type;

	/* values would be inserted */
	private final Vector bindValues;

//	/** has this parameter been set? */
//	private boolean isSet;  // check by bindValues size

	/** scale of Decimal */
	private int scale = 0;

	public ColumnBindValue(int index, String colName, Entity.DATA_TYPE type, int scale) {
		this.index = index;
		this.colName = colName;
		this.type = type;
		this.scale = scale;
		this.bindValues = BasicEntityFactory.instance().createVectorWithDefaultValue(type, 0, scale);
	}

	int getIndex() {
		return index;
	}

	Vector getBindValues() {
		return bindValues;
	}

	String getColName() {
		return colName;
	}

	Entity.DATA_TYPE getType() {
		return type;
	}

//	boolean isSet() {
//		return isSet;
//	}

	void setIndex(int index) {
		this.index = index;
	}

	void setColName(String colName) {
		this.colName = colName;
	}

	void setType(Entity.DATA_TYPE type) {
		this.type = type;
	}

//	void setSet(boolean set) {
//		isSet = set;
//	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	@Override
	public int compareTo(ColumnBindValue o) {
		return this.index - o.index;
	}
}


class BindValue {
	/** The value to store */
	private Object value;

	private boolean isNull;

	BindValue(Object value,boolean isNull) {
		this.value = value;
		this.isNull = isNull;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}


	public boolean isNull() {
		return isNull;
	}
}
