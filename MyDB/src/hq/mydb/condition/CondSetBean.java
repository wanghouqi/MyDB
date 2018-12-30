package hq.mydb.condition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * 条件集合Bean用于生成查询数据库的Where后面的条件SQL
 * @author Administrator
 *
 */
public class CondSetBean {
	private String joinKey = "";// 跟前一个条件的连接符号 AND | OR
	/*
	 * 查询条件的数组,可以是CondBean或CondSetBean
	 */
	private ArrayList<Object> alConditor = new ArrayList<Object>();

	HashMap<String, CondBean> hmKeyToCondBean = new HashMap<String, CondBean>();

	/**
	 * 
	 */
	public CondSetBean() {
		super();
	}

	/**
	 * @param joinKey : 前置连接符号
	 */
	public CondSetBean(String joinKey) {
		super();
		this.joinKey = joinKey;
	}

	/**
	 * 添加一个CondSetBean
	 * @param condSetBean
	 */
	public void addCondSetBean(CondSetBean condSetBean) {
		this.alConditor.add(condSetBean);
	}

	/**
	 * 添加一个CondBean
	 * @param condBean
	 */
	private void addCondBean(CondBean condBean) {
		this.alConditor.add(condBean);
		this.hmKeyToCondBean.put(condBean.getKey(), condBean);
	}

	// 无连接符
	/**
	 * 添加一个[=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_equal(String key, Object value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_EQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[>]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_greater(String key, Object value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_GREATER, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[>=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_greaterEqual(String key, Object value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_GREATEROREQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_less(String key, Object value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_LESS, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_lessEqual(String key, Object value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_LESSOREQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[IN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,Set<Object>类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_in(String key, Collection<?> value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_IN, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT IN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,Set<Object>类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_notIn(String key, Collection<?> value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_NOTIN, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[IS NULL]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @return
	 */
	public CondSetBean addCondBean_isNull(String key) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_ISNULL);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT NULL]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @return
	 */
	public CondSetBean addCondBean_notNull(String key) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_NOTNULL);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[LIKE]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值, "%like%"
	 * @return
	 */
	public CondSetBean addCondBean_like(String key, String value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_LIKE, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[BETWEEN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value1 : 值,类型可以是Integer,Long,Float,Double
	 * @param value2 : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_between(String key, Object value1, Object value2) {
		ArrayList<Object> al = new ArrayList<Object>();
		al.add(value1);
		al.add(value2);
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_BETWEEN, al);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT BETWEEN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value1 : 值,类型可以是Integer,Long,Float,Double
	 * @param value2 : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_notBetween(String key, Object value1, Object value2) {
		ArrayList<Object> al = new ArrayList<Object>();
		al.add(value1);
		al.add(value2);
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_NOTBETWEEN, al);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<>]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_notEqual(String key, Object value) {
		CondBean condBean = new CondBean(key, CondBean.OPERATOR_NOTEQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	// AND 连接符号
	/**
	 * 添加一个[=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_equal(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_EQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[>]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_greater(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_GREATER, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[>=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_greaterEqual(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_GREATEROREQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_less(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_LESS, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_lessEqual(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_LESSOREQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[IN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,Set<Object>类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_in(String key, Collection<?> value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_IN, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT IN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,Set<Object>类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_notIn(String key, Collection<?> value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_NOTIN, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[IS NULL]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @return
	 */
	public CondSetBean addCondBean_and_isNull(String key) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_ISNULL);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT NULL]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @return
	 */
	public CondSetBean addCondBean_and_notNull(String key) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_NOTNULL);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[LIKE]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值, "%like%"
	 * @return
	 */
	public CondSetBean addCondBean_and_like(String key, String value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_LIKE, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[BETWEEN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value1 : 值,类型可以是Integer,Long,Float,Double
	 * @param value2 : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_between(String key, Object value1, Object value2) {
		ArrayList<Object> al = new ArrayList<Object>();
		al.add(value1);
		al.add(value2);
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_BETWEEN, al);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT BETWEEN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value1 : 值,类型可以是Integer,Long,Float,Double
	 * @param value2 : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_notBetween(String key, Object value1, Object value2) {
		ArrayList<Object> al = new ArrayList<Object>();
		al.add(value1);
		al.add(value2);
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_NOTBETWEEN, al);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<>]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_and_notEqual(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_AND, key, CondBean.OPERATOR_NOTEQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	// or 连接符

	/**
	 * 添加一个[=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_equal(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_EQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[>]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_greater(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_GREATER, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[>=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_greaterEqual(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_GREATEROREQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_less(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_LESS, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<=]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_lessEqual(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_LESSOREQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[IN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,Set<Object>类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_in(String key, Collection<?> value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_IN, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT IN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,Set<Object>类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_notIn(String key, Collection<?> value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_NOTIN, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[IS NULL]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @return
	 */
	public CondSetBean addCondBean_or_isNull(String key) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_ISNULL);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT NULL]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @return
	 */
	public CondSetBean addCondBean_or_notNull(String key) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_NOTNULL);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[LIKE]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值, "%like%"
	 * @return
	 */
	public CondSetBean addCondBean_or_like(String key, String value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_LIKE, value);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[BETWEEN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value1 : 值,类型可以是Integer,Long,Float,Double
	 * @param value2 : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_between(String key, Object value1, Object value2) {
		ArrayList<Object> al = new ArrayList<Object>();
		al.add(value1);
		al.add(value2);
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_BETWEEN, al);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[NOT BETWEEN]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value1 : 值,类型可以是Integer,Long,Float,Double
	 * @param value2 : 值,类型可以是Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_notBetween(String key, Object value1, Object value2) {
		ArrayList<Object> al = new ArrayList<Object>();
		al.add(value1);
		al.add(value2);
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_NOTBETWEEN, al);
		this.addCondBean(condBean);
		return this;
	}

	/**
	 * 添加一个[<>]的条件
	 * @param key : 查询的栏位,如 CN_NO
	 * @param value : 值,类型可以是String,Integer,Long,Float,Double
	 * @return
	 */
	public CondSetBean addCondBean_or_notEqual(String key, Object value) {
		CondBean condBean = new CondBean(CondBean.JOIN_KEY_OR, key, CondBean.OPERATOR_NOTEQUAL, value);
		this.addCondBean(condBean);
		return this;
	}

	public String toSQLString() {
		return toSQLString(false);
	}

	/**
	 * 生成查询条件SQL语句
	 * @param isSubCSB : 是否是用于构建子语句,是则在返回的SQL加上()
	 * @return
	 */
	private String toSQLString(boolean isSubCSB) {
		String sbSQL = "";
		for (Object object : alConditor) {
			if (object instanceof CondSetBean) {
				sbSQL += " " + ((CondSetBean) object).toSQLString(true);
			} else if (object instanceof CondBean) {
				sbSQL += " " + ((CondBean) object).toSQLString();
			}
		}
		if (isSubCSB) {
			sbSQL = "(" + sbSQL + ")";
		}
		return this.joinKey + " " + sbSQL;
	}

	public int size() {
		return this.alConditor.size();
	}
}
