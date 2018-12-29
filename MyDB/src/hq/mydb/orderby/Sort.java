package hq.mydb.orderby;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

public class Sort implements Serializable {
	private static final long serialVersionUID = 6991293093298687181L;
	private String sortIndex = "";//序号，用于sort排序
	private String columnName = "";
	private String order = ASC;
	public static final String ASC = "ASC";
	public static final String DESC = "DESC";

	public Sort(String columnName) {
		super();
		setColumnName(columnName);
	}

	public Sort(String columnName, String order) {
		super();
		setColumnName(columnName);
		setOrder(order);
	}

	public Sort(String columnName, String order, String sortIndex) {
		super();
		this.columnName = columnName;
		this.order = order;
		this.sortIndex = sortIndex;
	}

	public String getSortIndex() {
		return sortIndex;
	}

	public void setSortIndex(String sortIndex) {
		this.sortIndex = sortIndex;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getOrder() {
		return order;
	}

	public void setOrder(String order) {
		this.order = order;
	}

	public boolean isEmpty() {
		return StringUtils.isEmpty(this.columnName);
	}

	public boolean isNotEmpty() {
		return StringUtils.isNotEmpty(this.columnName);
	}

	public String toSQLString() {
		String retString = "";
		if (isNotEmpty()) {
			retString = this.columnName + " " + order;
		}
		return retString;
	}

}
