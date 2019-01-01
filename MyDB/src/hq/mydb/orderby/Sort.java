package hq.mydb.orderby;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

/**
 * 排序设定,用于数据库查询和TableVO排序
 * @author wanghq
 *
 */
public class Sort implements Serializable {
	private static final long serialVersionUID = 6991293093298687181L;
	private String sortIndex = "";//序号，用于sort排序
	private String columnName = "";
	private String sort = ASC;
	public static final String ASC = "ASC";
	public static final String DESC = "DESC";
	private boolean isNumber = false;// 标识当前排序栏位是否为数值.

	public Sort(String columnName) {
		super();
		setColumnName(columnName);
	}

	public Sort(String columnName, String sort) {
		super();
		setColumnName(columnName);
		setSort(sort);
	}

	public Sort(String columnName, String sort, String sortIndex) {
		super();
		this.columnName = columnName;
		this.sort = sort;
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

	public String getSort() {
		return sort;
	}

	public void setSort(String sort) {
		this.sort = sort;
	}

	public boolean isEmpty() {
		return StringUtils.isEmpty(this.columnName);
	}

	public boolean isNotEmpty() {
		return StringUtils.isNotEmpty(this.columnName);
	}

	public boolean isNumber() {
		return isNumber;
	}

	public void setNumber(boolean isNumber) {
		this.isNumber = isNumber;
	}

	public String toSQLString() {
		String retString = "";
		if (isNotEmpty()) {
			retString = this.columnName + " " + sort;
		}
		return retString;
	}

}
