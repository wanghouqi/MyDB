package hq.mydb.data;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;

import hq.mydb.condition.CondSetBean;
import hq.mydb.db.Column;
import hq.mydb.utils.MyDBHelper;

/**
 * 用于记录数据库中的一条记录
 * Key是数据库的表名
 * @author wanghq
 *
 */
public class FormVO extends DataObject {
	private static final long serialVersionUID = -1072732898273007877L;
	private char operation = DataObject.OPERATION_UNDEFINED; // 当前FormVO用于生成SQL时对应的数据库操作类型.
	private CondSetBean condSetBean; // 用户生成Update语句的条件

	/**
	 * 生成一个FormVO实例
	 */
	public FormVO() {
	}

	/**
	 * 生成一个带有key的FormVO实例
	 * 
	 * @param key
	 */
	public FormVO(String key) {
		super(key);
	}

	/**
	 * 生成一个带有dbTableName和数据库操作类型的FormVO实例
	 * 
	 * @param key
	 * @param operation
	 */
	public FormVO(String key, char operation) {
		super(key);
		this.operation = operation;
	}

	/**
	 * 得到当前FormVO的CondSetBean
	 * 
	 * @return String
	 */
	public CondSetBean getCondSetBean() {
		return condSetBean;
	}

	/**
	 * 设定当前FormVO的CondSetBean
	 * 
	 * @param dbTableName
	 */
	public void setCondSetBean(CondSetBean condSetBean) {
		this.condSetBean = condSetBean;
	}

	public char getOperation() {
		return operation;
	}

	public void setOperation(char operation) {
		this.operation = operation;
	}

	public void setCellVOValue(String key, String value) {
		if (super.containsChildVOKey(key)) {
			CellVO cellVO = (CellVO) super.getChildVO(key);
			cellVO.setValue(value);
		}
	}

	/**
	 * 通过CellVO的key取到CellVO
	 * 
	 * @param cellVOKey
	 * @return CellVO
	 */
	public CellVO get(String cellVOKey) {
		return (CellVO) super.getChildVO(cellVOKey);
	}

	/**
	 * 根据CellVO在RowVO中的位置取出CellVO,index是从0开始的.
	 * 
	 * @param index
	 * @return CellVO
	 */
	public CellVO get(int index) {
		return (CellVO) super.getChildVO(index);
	}

	/**
	 * 通过CellVO的key取到CellVO的Value
	 * 
	 * @param cellVOKey
	 * @return String
	 */
	public String getCellVOValue(String cellVOKey) {
		if (super.containsChildVOKey(cellVOKey)) {
			CellVO cellVO = (CellVO) super.getChildVO(cellVOKey);
			return cellVO.getValue();
		} else {
			return "";
		}
	}

	/**
	 * 向当前FormVO中添加一个CellVO
	 * 
	 * @param cellVO
	 */
	public void addCellVO(CellVO cellVO) {
		super.addChildVO(cellVO);
	}

	/**
	 * 向当前FormVO中添加一组CellVO
	 * 
	 * @param cellVOArray
	 */
	public void addCellVOArray(ArrayList<CellVO> cellVOArray) {
		for (CellVO cellVO : cellVOArray) {
			this.addCellVO(cellVO);
		}
	}

	/**
	 * 向当前FormVO中添加一组CellVO
	 * 
	 * @param cellVOs
	 */
	public void addCellVOs(CellVO[] cellVOs) {
		for (CellVO cellVO : cellVOs) {
			this.addCellVO(cellVO);
		}
	}

	/**
	 * 替换当前FormVO中CellVO数组
	 * 
	 * @param cellVOArray
	 */
	public void setCellVOArray(ArrayList<CellVO> cellVOArray) {
		super.setChildVOArray(cellVOArray);
	}

	/**
	 * 得到当前FormVO中的CellVO数量
	 * 
	 * @return int
	 */
	public int size() {
		return super.sizeOfChildVOArray();
	}

	/**
	 * 根据当前FormVO生成SQL语句.operation必须有值才会得到相应的UPDATE SQL或INSERT
	 * SQL语句.
	 * 
	 * @return String UPDATE 表名称 SET 列名称1 = 新值1,列名称2 = 新值2 where ....... INSERT
	 *         INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 */
	public String toSQLString() {
		String sql = null;
		if (this.operation == DataObject.OPERATION_INSERT) {
			sql = toInsertSQLString();
		} else if (this.operation == DataObject.OPERATION_UPDATE) {
			sql = toUpdateSQLString();
		}
		return sql;
	}

	/**
	 * 根据FormVO和CondSetBean生成Update SQL语句
	 * 
	 * @return String UPDATE 表名称 SET 列名称1 = 新值1,列名称2 = 新值2 where .......
	 */
	private String toUpdateSQLString() {
		StringBuffer sqlSB = new StringBuffer();
		sqlSB.append("UPDATE ");
		sqlSB.append(super.getKey());
		sqlSB.append(" SET ");
		CellVO cvoPrimaryId = null;
		boolean isStart = false;
		for (int i = 0; i < this.size(); i++) {
			CellVO cellVO = this.get(i);
			if (StringUtils.isNotBlank(cellVO.getKey())) {
				if (StringUtils.equals(cellVO.getKey(), MyDBHelper.getPrimaryKeyColumnName())) {
					cvoPrimaryId = cellVO;
					continue;
				}
				if (isStart) {
					sqlSB.append(" ,");
				}
				sqlSB.append(cellVO.getKey());
				sqlSB.append("=");
				sqlSB.append(cellVO.getValueForSQLString());
				isStart = true;
			}
		}
		if (!isStart) {
			return null;
		}
		sqlSB.append(" WHERE ");
		if (cvoPrimaryId != null) {
			sqlSB.append(cvoPrimaryId.getKey());
			sqlSB.append("='");
			sqlSB.append(cvoPrimaryId.getValue());
			sqlSB.append("'");
		} else if (condSetBean != null) {
			String whereParameterSQL = condSetBean.toSQLString();
			if (whereParameterSQL != null && whereParameterSQL.length() > 0) {
				sqlSB.append(whereParameterSQL);
			} else {
				return null;
			}
		} else {
			return null;
		}
		return sqlSB.toString();
	}

	/**
	 * 根据FormVO生成Insert SQL语句
	 * 
	 * @return String INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 */
	private String toInsertSQLString() {
		StringBuffer sqlSB = new StringBuffer();
		sqlSB.append("INSERT INTO ");
		sqlSB.append(super.getKey());
		sqlSB.append(" ");

		StringBuffer keySQLSB = new StringBuffer();
		StringBuffer valueSQLSB = new StringBuffer();
		keySQLSB.append("(");
		valueSQLSB.append("(");
		boolean isStart = false;
		for (int i = 0; i < this.size(); i++) {
			CellVO cellVO = this.get(i);
			if (cellVO.getValue() == null) {
				continue;// 在插入的时候如果值是null则不需要处理.
			}
			if (StringUtils.isNotBlank(cellVO.getKey())) {
				if (isStart) {
					keySQLSB.append(" ,");
					valueSQLSB.append(" ,");
				}
				keySQLSB.append(cellVO.getKey());
				valueSQLSB.append(cellVO.getValueForSQLString());
				isStart = true;
			}
		}

		if (!isStart) {
			return null;
		}
		keySQLSB.append(")");
		valueSQLSB.append(")");

		sqlSB.append(keySQLSB);
		sqlSB.append(" VALUES ");
		sqlSB.append(valueSQLSB);
		return sqlSB.toString();
	}

	public boolean containsKey(String key) {
		return super.containsChildVOKey(key);
	}

	public void removeCellVO(int sortIndex) {
		super.removeChildVO(sortIndex);
	}

	public void removeCellVO(CellVO cellVO) {
		super.removeChildVO(cellVO);
	}

	public void removeCellVO(String key) {
		super.removeChildVO(key);
	}

	public CellVO[] toCellVOs() {
		CellVO[] cellVOs = new CellVO[this.size()];
		super.toArrayFromChildVOs(cellVOs);
		return cellVOs;
	}

	public FormVO clone() {
		FormVO cloneFormVO = new FormVO(this.getKey(), this.operation);
		for (int i = 0; i < this.size(); i++) {
			cloneFormVO.addCellVO(this.get(i).clone());
		}
		return cloneFormVO;
	}

	/**
	 * 清空FormVO中的CellVO数组
	 * @return
	 */
	public int clearCellVOArray() {
		return super.clearChildVO();
	}

	/**
	 * 将FormVO转换为RowVO
	 * @return RowVO
	 */
	public RowVO transformToRowVO() {
		RowVO rowVO = new RowVO(this.getKey(), this.getOperation());
		for (int i = 0; i < this.size(); i++) {
			rowVO.addCellVO(new CellVO(this.get(i).getKey(), this.get(i).getValue()));
		}
		return rowVO;
	}

	/**
	 * 输出数据,可以复制到Excel中查看
	 */
	@Override
	public String toString() {
		// Form信息
		StringBuffer sb = new StringBuffer();
		sb.append("Key" + "\t" + (StringUtils.isEmpty(this.getKey()) ? "" : this.getKey()) + "\n");
		sb.append("Operation" + "\t" + this.getOperation() + "\n");
		// 数据
		for (CellVO cb : this.toCellVOs()) {
			sb.append(cb.getKey() + "\t" + cb.getValue() + "\n");
		}
		return sb.toString();
	}

	/**
	 * 返回FormVO中的所有的CellVO对应的Key和Value对应生成的JSONObject
	 * @return
	 */
	public JSONObject toDataJSONObject() {
		JSONObject obj = new JSONObject();
		for (CellVO cvo : this.toCellVOs()) {
			if (StringUtils.isEmpty(cvo.getValue())) {
				obj.put(cvo.getKey(), cvo.getValue());
				continue;
			}
			switch (cvo.getColumnType()) {
			case Column.TYPE_LONG: // 长整数
			case Column.TYPE_DATE: // 日期
			case Column.TYPE_DATETIME: // 日期加时间
				obj.put(cvo.getKey(), Long.parseLong(cvo.getValue()));
				break;
			case Column.TYPE_DECIMAL: // 小数
				obj.put(cvo.getKey(), Double.parseDouble(cvo.getValue()));
				break;
			case Column.TYPE_TEXT: // 文本
			case Column.TYPE_VARCHAR: // 字符
				obj.put(cvo.getKey(), cvo.getValue());
				break;
			default:
				obj.put(cvo.getKey(), cvo.getValue());
				break;
			}
		}
		return obj;
	}
}
