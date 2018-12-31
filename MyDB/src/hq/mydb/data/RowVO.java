package hq.mydb.data;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

import hq.mydb.utils.MyDBHelper;

/**
 * 用于记录数据库里的一行记录
 * Key是当前记录的主键
 * @author wanghq
 *
 */
public class RowVO extends DataObject {
	private static final long serialVersionUID = 4916603218514512459L;
	private String operation = DataObject.OPERATION_UNDEFINED; // 当前RowVO用于生成SQL时对应的数据库操作类型.

	/**
	 * 生成一个RowVO实例
	 */
	public RowVO() {
		super();
	}

	public RowVO(String operation) {
		super();
		this.operation = operation;
	}

	public RowVO(String key, String operation) {
		super(key);
		this.operation = operation;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
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
	 * 向当前RowVO中添加一个CellVO
	 * 
	 * @param cellVO
	 */
	public RowVO addCellVO(CellVO cellVO) {
		super.addChildVO(cellVO);
		return this;
	}

	/**
	 * 向当前RowVO中添加一组CellVO
	 * 
	 * @param cellVOArray
	 */
	public void addCellVOArray(ArrayList<CellVO> cellVOArray) {
		for (CellVO cellVO : cellVOArray) {
			this.addCellVO(cellVO);
		}
	}

	/**
	 * 向当前RowVO中添加一组CellVO
	 * 
	 * @param cellVOs
	 */
	public void addCellVOs(CellVO[] cellVOs) {
		for (CellVO cellVO : cellVOs) {
			this.addCellVO(cellVO);
		}
	}

	/**
	 * 替换当前RowVO中CellVO数组
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
	 * 根据当前RowVO生成SQL语句.operation必须有值才会得到相应的UPDATE SQL或INSERT
	 * SQL语句.
	 * 
	 * @return String UPDATE 表名称 SET 列名称1 = 新值1,列名称2 = 新值2 where ....... INSERT
	 *         INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 */
	public String toSQLString() {
		String sql = null;
		if (StringUtils.equals(this.operation, DataObject.OPERATION_INSERT)) {
			sql = toInsertSQLString();
		} else if (StringUtils.equals(this.operation, DataObject.OPERATION_UPDATE)) {
			sql = toUpdateSQLString();
		}
		return sql;
	}

	/**
	 * 将RowVO中的CellVO的数据转换为可以用于Update SQL的字符窜语句
	 * @return String 列名称1 = 新值1,列名称2 = 新值2 where CN_ID=数值3
	 */
	public String toUpdateSQLString() {
		StringBuffer sqlSB = new StringBuffer();
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
		} else {
			return null;
		}
		return sqlSB.toString();
	}

	/**
	 * 将RowVO中的CellVO的数据转换为可以用于Insert SQL的字符窜语句
	 * @return String (列1, 列2,...) VALUES (值1, 值2,....)
	 */
	public String toInsertSQLString() {
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
		return keySQLSB.toString() + " VALUES " + valueSQLSB.toString();
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

	public RowVO clone() {
		RowVO cloneRowVO = new RowVO(this.getKey(), this.operation);
		for (int i = 0; i < this.size(); i++) {
			cloneRowVO.addCellVO(this.get(i).clone());
		}
		return cloneRowVO;
	}

	/**
	 * 清空RowVO中的CellVO数组
	 * @return
	 */
	public int clearCellVOArray() {
		return super.clearChildVO();
	}

	/**
	 * 将RowVO转换为FormVO
	 * @return FormVO
	 */
	public FormVO transformToFormVO() {
		FormVO formVO = new FormVO(this.getKey(), this.getOperation());
		for (int i = 0; i < this.size(); i++) {
			formVO.addCellVO(new CellVO(this.get(i).getKey(), this.get(i).getValue()));
		}
		return formVO;
	}
}
