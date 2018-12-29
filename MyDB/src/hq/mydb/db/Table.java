package hq.mydb.db;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 数据库表的映射
 * @author wanghq
 *
 */
public class Table {
	private String name;// 数据库表名
	private boolean distinct = false;// 用于查询时,是否使用distinct
	private ArrayList<Column> alColumn = new ArrayList<Column>();// 表中的栏位
	private HashMap<String, Column> hmNameToColumn = new HashMap<String, Column>();
	private String desc;// 数据库中表的描述

	public Table(String name) {
		super();
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isDistinct() {
		return distinct;
	}

	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}

	public ArrayList<Column> getColumnArray() {
		return alColumn;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	/**
	 * 添加一个Column
	 * @param columnName
	 */
	public void addColumn(Column column) {
		this.alColumn.add(column);
		this.hmNameToColumn.put(column.getName(), column);
	}

	/**
	 * 添加一个Column
	 * @param columnName
	 */
	public void addColumn(String columnName, String columnType) {
		Column column = new Column(columnName, columnType);
		this.addColumn(column);
	}

	/**
	 * 添加一个Column
	 * @param columnName
	 */
	public void addColumn(String columnName, String columnType,String columnDesc) {
		Column column = new Column(columnName, columnType,columnDesc);
		this.addColumn(column);
	}

	/**
	 * 返回当前表的栏位数
	 * @return
	 */
	public int size() {
		return this.alColumn.size();
	}

	/**
	 * 返回第Index个Column
	 * @param index
	 * @return
	 */
	public Column get(int index) {
		return this.alColumn.get(index);
	}
}
