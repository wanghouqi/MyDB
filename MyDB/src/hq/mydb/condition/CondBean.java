package hq.mydb.condition;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;

/**
 * 条件Bean用于生成查询数据库的Where后面的条件SQL
 * @author Administrator
 *
 */
public class CondBean {
	public static final String JOIN_KEY_AND = " AND "; // SQL连接符号AND
	public static final String JOIN_KEY_OR = " OR "; // SQL连接符号OR
	public static final String OPERATOR_EQUAL = "="; // 等于
	public static final String OPERATOR_GREATER = ">"; // 大于
	public static final String OPERATOR_GREATEROREQUAL = ">="; // 大于等于
	public static final String OPERATOR_IN = " IN "; // in
	public static final String OPERATOR_ISNULL = " IS NULL "; // 为空
	public static final String OPERATOR_LESS = "<"; // 小于
	public static final String OPERATOR_LESSOREQUAL = "<="; // 小于等于
	public static final String OPERATOR_LIKE = " LIKE "; // like
	public static final String OPERATOR_BETWEEN = " BETWEEN "; // between
	public static final String OPERATOR_NOTBETWEEN = " NOT BETWEEN "; // not between
	public static final String OPERATOR_NOTEQUAL = "<>"; // 不等于
	public static final String OPERATOR_NOTIN = " NOT IN "; // not in
	public static final String OPERATOR_NOTNULL = " NOT NULL "; // 不为空

	private String joinKey = "";// 跟前一个条件的连接符号 AND | OR
	private String key;// 查询的栏位: CN_NO
	private String operator;// 运算符,大于,等于,between等..
	private Object value;// 值,可以是String,Long,JSONArray等..

	/**
	 * @param key
	 * @param operator
	 */
	public CondBean(String key, String operator) {
		super();
		this.key = key;
		this.operator = operator;
	}

	/**
	 * @param key
	 * @param operator
	 * @param value
	 */
	public CondBean(String key, String operator, Object value) {
		super();
		this.key = key;
		this.operator = operator;
		this.value = value;
	}

	/**
	 * @param joinKey
	 * @param key
	 * @param operator
	 * @param value
	 */
	public CondBean(String joinKey, String key, String operator, Object value) {
		super();
		this.joinKey = joinKey;
		this.key = key;
		this.operator = operator;
		this.value = value;
	}

	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * @return the operator
	 */
	public String getOperator() {
		return operator;
	}

	/**
	 * @param operator the operator to set
	 */
	public void setOperator(String operator) {
		this.operator = operator;
	}

	/**
	 * @return the value
	 */
	public Object getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(Object value) {
		this.value = value;
	}

	/**
	 * 生成查询条件SQL语句
	 * @return
	 */
	public String toSQLString() {
		String sbSQL = "";
		switch (this.operator) {
		case CondBean.OPERATOR_EQUAL:
		case CondBean.OPERATOR_NOTEQUAL:
		case CondBean.OPERATOR_GREATER:
		case CondBean.OPERATOR_GREATEROREQUAL:
		case CondBean.OPERATOR_LESS:
		case CondBean.OPERATOR_LESSOREQUAL:
		case CondBean.OPERATOR_LIKE:
			sbSQL = key + this.operator + this.getSQLValue(this.value);
			break;
		case CondBean.OPERATOR_IN:
			String value_in = this.getSQLValue(this.value);
			if (StringUtils.isNotEmpty(value_in)) {
				sbSQL = key + this.operator + value_in;
			} else {
				sbSQL = "1=2";
			}
			break;
		case CondBean.OPERATOR_NOTIN:
			String value_notIn = this.getSQLValue(this.value);
			if (StringUtils.isNotEmpty(value_notIn)) {
				sbSQL = key + this.operator + value_notIn;
			} else {
				sbSQL = "1=1";
			}
			break;
		case CondBean.OPERATOR_BETWEEN:
		case CondBean.OPERATOR_NOTBETWEEN:
			@SuppressWarnings("unchecked")
			ArrayList<Object> betVal = (ArrayList<Object>) this.value;
			sbSQL = key + this.operator + this.getSQLValue(betVal.get(0)) + CondBean.JOIN_KEY_AND + this.getSQLValue(betVal.get(1));
			break;
		case CondBean.OPERATOR_ISNULL:
		case CondBean.OPERATOR_NOTNULL:
			sbSQL = key + this.operator;
			break;
		}
		return this.joinKey + " " + sbSQL;
	}

	/**
	 * 根据传入值的类型,生成想用用于SQL的值的字符串.
	 * @param v
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private String getSQLValue(Object v) {
		String sqlValue = "";
		if (v instanceof String) {
			sqlValue = "'" + v + "'";
		} else if (v instanceof Integer || v instanceof Long || v instanceof Float || v instanceof Double) {
			sqlValue = String.valueOf(v);
		} else if (v instanceof ArrayList || v instanceof HashSet) {
			sqlValue += "(";
			boolean isNotStart = false;
			for (Object o : (v instanceof ArrayList ? (ArrayList<Object>) v : (HashSet<Object>) v)) {
				if (StringUtils.isEmpty(getSQLValue(o))) {
					continue;// 过滤掉空的数据
				}
				if (isNotStart) {
					sqlValue += ",";
				} else {
					isNotStart = true;
				}
				sqlValue += getSQLValue(o);
			}
			sqlValue += ")";
			if (!isNotStart) {
				sqlValue = "";
			}
		}
		return sqlValue;
	}
}
