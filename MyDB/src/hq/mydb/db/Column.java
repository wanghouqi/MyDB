package hq.mydb.db;

import java.util.ArrayList;

import org.apache.commons.codec.binary.StringUtils;

/**
 * 数据库中栏位的映射
 * @author wanghq
 *
 */
public class Column {
	public static final String TYPE_LONG = "long"; // 长整数
	public static final String TYPE_DATE = "date"; // 日期
	public static final String TYPE_DATETIME = "datetime"; // 日期加时间
	public static final String TYPE_DECIMAL = "decimal"; // 小数
	public static final String TYPE_TEXT = "text"; // 文本
	public static final String TYPE_VARCHAR = "varchar"; // 字符

	private String name;// 栏位名称
	private String type;// 栏位类型
	private String desc;// 数据库中栏位的描述

	public Column(String name, String type) {
		super();
		this.name = name;
		this.type = type;
	}

	public Column(String name, String type, String desc) {
		super();
		this.name = name;
		this.type = type;
		this.desc = desc;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	/**
	 * 验证当前Column的类型是否为传入的类型
	 * @param type
	 * @return
	 */
	public boolean isType(String type) {
		return StringUtils.equals(this.getType(), type);
	}

}
