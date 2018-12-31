package hq.mydb.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import hq.mydb.db.Column;
import hq.mydb.utils.MyDBHelper;

/**
 * 用于记录数据库中的一组记录,其包含多笔RowVO.
 * Key是数据库的表名
 * @author wanghq
 *
 */
public class TableVO extends DataObject {
	private static final long serialVersionUID = 6492071082836861362L;
	private String operation = DataObject.OPERATION_UNDEFINED; // 当前TableVO用于生成SQL时对应的数据库操作类型.
	private RowVO rvoHead = null;// head RowVO用于传递给页面时使用.

	/**
	 * 生成一个TableVO实例
	 */
	public TableVO() {
		super();
	}

	/**
	 * 生成一个带有key的TableVO实例
	 * @param key
	 */
	public TableVO(String key) {
		super(key);
	}

	/**
	 * 生成一个带有dbTableName和数据库操作类型的FormVO实例
	 * 
	 * @param key
	 * @param operation
	 */
	public TableVO(String key, String operation) {
		super(key);
		this.operation = operation;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	/**
	 * 得到当前Table的HeadRowVO,如果用户没有主动设定,并且是第一次读取时,使用第一条记录的key生成一个新的RowVO
	 * @return
	 */
	public RowVO getHeadRowVO() {
		if (rvoHead == null) {
			rvoHead = new RowVO();
			if (this.size() > 0) {
				for (CellVO cellVO : this.get(0).toCellVOs()) {
					rvoHead.addCellVO(new CellVO(cellVO.getKey(), cellVO.getKey()));
				}
			}
		}
		return rvoHead;
	}

	/**
	 * 设定HeadRowVO
	 * @param rvoHead
	 */
	public void setHeadRowVO(RowVO rvoHead) {
		this.rvoHead = rvoHead;
	}

	/**
	 * 通过RowVO的key取到RowVO
	 * @param rowVOKey
	 * @return RowVO
	 */
	public RowVO get(String rowVOKey) {
		return (RowVO) super.getChildVO(rowVOKey);
	}

	/**
	 * 根据RowVO在TableVO中的位置取出RowVO,index是从0开始的.
	 * @param index
	 * @return RowVO
	 */
	public RowVO get(int index) {
		return (RowVO) super.getChildVO(index);
	}

	public void removeRowVO(int sortIndex) {
		super.removeChildVO(sortIndex);
	}

	public void removeRowVO(RowVO rowVO) {
		super.removeChildVO(rowVO);
	}

	public void removeRowVO(String key) {
		super.removeChildVO(key);
	}

	/**
	 * 向当前TableVO中添加一个RowVO
	 * @param rowVO
	 */
	public void addRowVO(RowVO rowVO) {
		super.addChildVO(rowVO);
	}

	/**
	 * 向当前TableVO中添加一组RowVO
	 * 
	 * @param rowVOArray
	 */
	public void addRowVOArray(ArrayList<RowVO> rowVOArray) {
		for (RowVO rowVO : rowVOArray) {
			this.addRowVO(rowVO);
		}
	}

	/**
	 * 向当前TableVO中添加一组RowVO
	 * 
	 * @param rowVOs
	 */
	public void addRowVOs(RowVO[] rowVOs) {
		for (RowVO rowVO : rowVOs) {
			this.addRowVO(rowVO);
		}
	}

	/**
	 * 替换当前TableVO中RowVO数组
	 * 
	 * @param rowVOArray
	 */
	public void setCellVOArray(ArrayList<RowVO> rowVOArray) {
		super.setChildVOArray(rowVOArray);
	}

	/**
	 * 替换当前TableVO中RowVO数组
	 * 
	 * @param tabItemArray
	 */
	public void setRowVOArray(ArrayList<RowVO> rowVOArray) {
		super.setChildVOArray(rowVOArray);
	}

	/**
	 * 得到当前TableVO中的RowVO数量
	 * @return int
	 */
	public int size() {
		return super.sizeOfChildVOArray();
	}

	/**
	 * 根据当前TableVO生成SQL语句.operation必须有值才会得到相应的UPDATE SQL或INSERT SQL语句.
	 * @return ArrayList<String> 
	 * UPDATE 表名称 SET 列名称1 = 新值1,列名称2 = 新值2 where .......
	 * INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 */
	public ArrayList<String> toSQLStrings() {
		ArrayList<String> sqls = null;
		if (StringUtils.equals(this.operation, DataObject.OPERATION_INSERT)) {
			sqls = toInsertSQLStrings();
		} else if (StringUtils.equals(this.operation, DataObject.OPERATION_UPDATE)) {
			sqls = toUpdateSQLStrings();
		}
		return sqls;
	}

	/**
	 * 根据TableVO生成Update SQL语句数据集
	 * @return ArrayList<String>
	 *  UPDATE 表名称 SET 列名称1 = 新值1,列名称2 = 新值2 where CN_ID=数值3
	 */
	private ArrayList<String> toUpdateSQLStrings() {
		ArrayList<String> sqls = new ArrayList<String>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			StringBuffer sqlSB = new StringBuffer();
			sqlSB.append("UPDATE ");
			sqlSB.append(super.getKey());
			sqlSB.append(" SET ");
			sqlSB.append(rowVO.toUpdateSQLString());
			sqls.add(sqlSB.toString());
		}
		return sqls;
	}

	/**
	 * 根据TableVO生成Insert SQL语句数据集
	 * @return ArrayList<String>
	 *  INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 */
	private ArrayList<String> toInsertSQLStrings() {
		ArrayList<String> sqls = new ArrayList<String>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			StringBuffer sqlSB = new StringBuffer();
			sqlSB.append("INSERT INTO ");
			sqlSB.append(super.getKey());
			sqlSB.append(" ");
			sqlSB.append(rowVO.toInsertSQLString());
			sqls.add(sqlSB.toString());
		}
		return sqls;
	}

	/**
	 * 将数据的Table放入到View的Table中
	 */
	public void addDataTable(TableVO dataTabelVO) {
		for (int i = 0; i < dataTabelVO.size(); i++) {
			this.addRowVO(dataTabelVO.get(i));
		}
	}

	public TableVO clone() {
		TableVO cloneTableVO = new TableVO(this.getKey(), this.getOperation());
		for (int i = 0; i < this.size(); i++) {
			cloneTableVO.addRowVO(this.get(i).clone());
		}
		return cloneTableVO;
	}

	public RowVO[] toRowVOs() {
		RowVO[] rowVOs = new RowVO[this.size()];
		super.toArrayFromChildVOs(rowVOs);
		return rowVOs;
	}

	/**
	 * 清空TableVO中的RowVO数组
	 * @return
	 */
	public int clearRowVOArray() {
		return super.clearChildVO();
	}

	public boolean containsKey(String key) {
		return super.containsChildVOKey(key);
	}

	/**
	 * 将TableVO转换为HashMap<String,String>,一对一.
	 * @param keyCellKey : cellVO的key
	 * @param valueCellKey : cellVO的Key
	 * @return HashMap<String, String>
	 */
	public HashMap<String, String> toHashMapOfToCellVOValue(String keyCellKey, String valueCellKey) {
		return toHashMapOfToCellVOValue(keyCellKey, valueCellKey, false);
	}

	/**
	 * 将TableVO转换为HashMap<String,String>,一对一.
	 * @param keyCellKey : cellVO的key
	 * @param valueCellKey : cellVO的Key
	 * @param passEmpty : 是否过滤空值
	 * @return HashMap<String, String>
	 */
	public HashMap<String, String> toHashMapOfToCellVOValue(String keyCellKey, String valueCellKey, boolean passEmpty) {
		HashMap<String, String> hm = new HashMap<String, String>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			String value = rowVO.getCellVOValue(valueCellKey);
			if (passEmpty) {
				if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
					continue;
				}
			}
			hm.put(key, value);
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String,RowVO>,一对一.
	 * @param keyCellKey
	 * @return HashMap<String,RowVO>
	 */
	public HashMap<String, RowVO> toHashMapOfToRowVO(String keyCellKey) {
		HashMap<String, RowVO> hm = new HashMap<String, RowVO>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			hm.put(key, rowVO);
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String,FormVO>,一对一.
	 * @param keyCellKey
	 * @return HashMap<String,FormVO>
	 */
	public HashMap<String, FormVO> toHashMapOfToFormVO(String keyCellKey) {
		HashMap<String, FormVO> hm = new HashMap<String, FormVO>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			FormVO fb = rowVO.transformToFormVO();
			fb.setKey(this.getKey());
			hm.put(key, fb);
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String, ArrayList<String>>,一对多.
	 * @param keyCellKey
	 * @param valueCellKey
	 * @return HashMap<String, ArrayList<String>>
	 */
	public HashMap<String, ArrayList<String>> toHashMapOfToCellVOValueArray(String keyCellKey, String valueCellKey) {
		return toHashMapOfToCellVOValueArray(keyCellKey, valueCellKey, false);
	}

	/**
	 * 将TableVO转换为HashMap<String, ArrayList<String>>,一对多.
	 * @param keyCellKey
	 * @param valueCellKey
	 * @param passEmpty : 是否过滤空值
	 * @return HashMap<String, ArrayList<String>>
	 */
	public HashMap<String, ArrayList<String>> toHashMapOfToCellVOValueArray(String keyCellKey, String valueCellKey, boolean passEmpty) {
		HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			String value = rowVO.getCellVOValue(valueCellKey);
			if (passEmpty) {
				if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
					continue;
				}
			}
			if (hm.containsKey(key)) {
				hm.get(key).add(value);
			} else {
				ArrayList<String> al = new ArrayList<String>();
				al.add(value);
				hm.put(key, al);
			}
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String, ArrayList<String>>,一对多.
	 * @param keyCellKey
	 * @param valueCellKey
	 * @return HashMap<String, ArrayList<String>>
	 */
	public HashMap<String, HashSet<String>> toHashMapOfToCellVOValueSet(String keyCellKey, String valueCellKey) {
		return toHashMapOfToCellVOValueSet(keyCellKey, valueCellKey, false);
	}

	/**
	 * 将TableVO转换为HashMap<String, ArrayList<String>>,一对多.
	 * @param keyCellKey
	 * @param valueCellKey
	 * @param passEmpty : 是否过滤空值
	 * @return HashMap<String, ArrayList<String>>
	 */
	public HashMap<String, HashSet<String>> toHashMapOfToCellVOValueSet(String keyCellKey, String valueCellKey, boolean passEmpty) {
		HashMap<String, HashSet<String>> hm = new HashMap<String, HashSet<String>>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			String value = rowVO.getCellVOValue(valueCellKey);
			if (passEmpty) {
				if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
					continue;
				}
			}
			if (hm.containsKey(key)) {
				hm.get(key).add(value);
			} else {
				HashSet<String> al = new HashSet<String>();
				al.add(value);
				hm.put(key, al);
			}
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String, Long> ,根据keyCellKey对valueCellKey加总.
	 * @param keyCellKey : GROUP BY key
	 * @param valueCellKey : SUM key
	 * @return HashMap<String, String>
	 */
	public HashMap<String, String> toHashMapOfSumCellVOValueLong(String keyCellKey, String valueCellKey) {
		HashMap<String, String> hm = new HashMap<String, String>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			String value = rowVO.getCellVOValue(valueCellKey);
			if (hm.containsKey(key)) {
				hm.put(key, String.valueOf(Long.parseLong(value) + Long.parseLong(hm.get(key))));
			} else {
				hm.put(key, value);
			}
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String, String> ,根据keyCellKey对valueCellKey加总.
	 * @param keyCellKey : GROUP BY key
	 * @param valueCellKey : SUM key
	 * @return HashMap<String, String>
	 */
	public HashMap<String, String> toHashMapOfSumCellVOValueDouble(String keyCellKey, String valueCellKey) {
		HashMap<String, String> hm = new HashMap<String, String>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			String value = rowVO.getCellVOValue(valueCellKey);
			if (hm.containsKey(key)) {
				hm.put(key, MyDBHelper.doubleAdd(value, hm.get(key)));
			} else {
				hm.put(key, value);
			}
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String, ArrayList<RowVO>>,一对多.
	 * @param keyCellKey
	 * @return HashMap<String, ArrayList<RowVO>>
	 */
	public HashMap<String, ArrayList<RowVO>> toHashMapOfToArrayRowVO(String keyCellKey) {
		HashMap<String, ArrayList<RowVO>> hm = new HashMap<String, ArrayList<RowVO>>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			if (hm.containsKey(key)) {
				hm.get(key).add(rowVO);
			} else {
				ArrayList<RowVO> al = new ArrayList<RowVO>();
				al.add(rowVO);
				hm.put(key, al);
			}
		}
		return hm;
	}

	/**
	 * 将TableVO转换为HashMap<String, ArrayList<RowVO>>,一对多.
	 * @param keyCellKey
	 * @return HashMap<String, ArrayList<RowVO>>
	 */
	public HashMap<String, TableVO> toHashMapOfToTableVO(String keyCellKey) {
		HashMap<String, TableVO> hm = new HashMap<String, TableVO>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String key = rowVO.getCellVOValue(keyCellKey);
			if (hm.containsKey(key)) {
				hm.get(key).addRowVO(rowVO);
			} else {
				TableVO tb = new TableVO();
				tb.setKey(this.getKey());
				tb.addRowVO(rowVO);
				hm.put(key, tb);
			}
		}
		return hm;
	}

	/**
	 * 将TableVO中的某一个Cell转换乘ArrayList
	 * @param cellKey
	 * @return ArrayList<String>
	 */
	public ArrayList<String> toArrayListOfCellVOValue(String cellKey) {
		return toArrayListOfCellVOValue(cellKey, false);
	}

	/**
	 * 将TableVO中的某一个Cell转换乘ArrayList
	 * @param cellKey
	 * @param passEmpty : 是否过滤空值
	 * @return ArrayList<String>
	 */
	public ArrayList<String> toArrayListOfCellVOValue(String cellKey, boolean passEmpty) {
		ArrayList<String> al = new ArrayList<String>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String value = rowVO.getCellVOValue(cellKey);
			if (passEmpty) {
				if (StringUtils.isEmpty(value)) {
					continue;
				}
			}
			al.add(value);
		}
		return al;
	}

	/**
	 * 将TableVO中的某一个Cell转换乘HashSet
	 * @param cellKey
	 * @return HashSet<String>
	 */
	public HashSet<String> toHashSetOfCellVOValue(String cellKey) {
		return toHashSetOfCellVOValue(cellKey, false);
	}

	/**
	 * 将TableVO中的某一个Cell转换乘HashSet
	 * @param cellKey
	 * @param passEmpty : 是否过滤空值
	 * @return HashSet<String>
	 */
	public HashSet<String> toHashSetOfCellVOValue(String cellKey, boolean passEmpty) {
		HashSet<String> hs = new HashSet<String>();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String value = rowVO.getCellVOValue(cellKey);
			if (passEmpty) {
				if (StringUtils.isEmpty(value)) {
					continue;
				}
			}
			hs.add(value);
		}
		return hs;
	}

	/**
	 * 将TableVO中的某一个Cell的值进行分组成为多个TableVO转换乘DataVO
	 * @param cellKeys : 用于分组的CellVO的key,可以传入多个;
	 * @return DataVO
	 * 		TableVO key : this.getKey() + "_" + CellVO.getValue()< + "_" + CellVO.getValue() + "_" + CellVO.getValue()..>;
	 */
	public DataVO toDataVOByKey(String... cellKeys) {
		DataVO dataVO = new DataVO();
		for (int i = 0; i < this.size(); i++) {
			RowVO rowVO = this.get(i);
			String value;
			String tableKey = this.getKey();
			for (int j = 0; j < cellKeys.length; j++) {
				value = rowVO.getCellVOValue(cellKeys[j]);
				tableKey += "_" + value;
			}

			TableVO tableVO = dataVO.getTableVO(tableKey);
			if (tableVO == null) {
				tableVO = new TableVO(tableKey);
				dataVO.addTableVO(tableVO);
			}
			tableVO.addRowVO(rowVO);
		}
		return dataVO;
	}

	/**
	 * 输出数据,可以复制到Excel中查看
	 */
	@Override
	public String toString() {
		// Table信息
		StringBuffer sb = new StringBuffer();
		sb.append("Key" + "\t" + (StringUtils.isEmpty(this.getKey()) ? "" : this.getKey()) + "\n");
		sb.append("operation" + "\t" + this.getOperation() + "\n");
		// 表头
		RowVO rbHead = null;
		if (this.size() > 0) {
			rbHead = this.get(0);
			for (CellVO cb : rbHead.toCellVOs()) {
				sb.append(cb.getKey() + "\t");
			}
		}
		sb.append("\n");
		// 数据
		for (RowVO rb : this.toRowVOs()) {
			for (CellVO cbHead : rbHead.toCellVOs()) {
				sb.append(rb.getCellVOValue(cbHead.getKey()) + "\t");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * 返回TableVO中的RowVO的JSONObject的JSONArray
	 * @return
	 */
	public JSONArray toDataJSONArray() {
		JSONArray arr = new JSONArray();
		for (RowVO rvo : this.toRowVOs()) {
			arr.add(rvo.toDataJSONObject());
		}
		return arr;
	}

	/**
	 * 对指定列进行日期的格式化
	 * @param format : 格式,例: yyyy-MM-dd
	 * @param columnNames : 需要格式化的栏位名
	 */
	public void formatDate(String format, String... columnNames) {
		for (RowVO rvo : this.toRowVOs()) {
			for (String columnName : columnNames) {
				String value = rvo.getCellVOValue(columnName);
				if (StringUtils.isNotBlank(value)) {
					rvo.setCellVOValue(columnName, MyDBHelper.formatDate(Long.parseLong(value), format));
				}
			}
		}
	}

}
