package hq.mydb.data;

import org.apache.commons.lang.StringUtils;

import hq.mydb.db.Column;

/**
 * 
 * @author wanghq
 * 基础数据VO,只有Key,value,ColumType三个属性
 * ColumType: 这个属性代表是的当前CellVO对应到数据库中是什么类型,在生成SQL语句是会用到.
 * 特殊说明:
 * 1. 新建的CellVO,则默认columnType为Varchar
 */
public class CellVO extends DataObject {
	private static final long serialVersionUID = -1817230706574221809L;
	private String value = "";
	private String columnType = Column.TYPE_VARCHAR;; // 数据库栏位类型

	/**
	 * 生成一个CellVO实例
	 */
	public CellVO() {
		super();
	}

	/**
	 * 生成一个带有key和Value的CellVO实例
	 * @param key
	 * @param value
	 */
	public CellVO(String key, String value) {
		super(key);
		this.setValue(value);
	}

	/**
	 * 用于Clone包含所有属性
	 * @param key
	 * @param value
	 * @param columnType
	 * @param isPrimaryId
	 */
	public CellVO(String key, String value, String columnType) {
		super(key);
		this.value = value;
		this.columnType = columnType;
	}

	/**
	 * 得到当前CellVO的Value
	 * @return
	 */

	public String getValue() {
		return value;
	}

	/**
	 * 设定当前CellVO的Value
	 * @param value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * 得到当前CellVO的ColumnType
	 * @return
	 */

	public String getColumnType() {
		return columnType;
	}

	/**
	 * 设定当前CellVO的ColumnType
	 * @param key
	 */
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	/**
	 * 根据当前CellVO的数据库类型来返回其在SQL语句中对应的Value的String
	 * @return
	 */
	public String getValueForSQLString() {
		String sql = "";
		switch (columnType) {
		case Column.TYPE_LONG:
		case Column.TYPE_DECIMAL:
		case Column.TYPE_DATE:
		case Column.TYPE_DATETIME:
			if (StringUtils.isEmpty(value)) {
				sql = "NULL"; // 数值型的操作数据库数据,如果value为空则赋值为NULL
			} else {
				sql = value;
			}
			break;
		case Column.TYPE_TEXT:
		case Column.TYPE_VARCHAR:
			if (value == null) {
				sql = "NULL"; // 字符型的操作数据库数据,如果value为null则赋值为NULL
			} else {
				// 处理用户保存的单引号这种特殊字符.
				String sqlValue = value;
				if (value.contains("'")) {
					sqlValue = value.replace("'", "''");
				}
				// 将值加入SQL
				sql = "'" + sqlValue + "'";
			}
			break;
		}
		return sql;
	}

	public CellVO clone() {
		CellVO CellVO = new CellVO(super.getKey(), value, columnType);
		return CellVO;
	}

}
