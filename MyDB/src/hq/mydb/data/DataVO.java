package hq.mydb.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

public class DataVO implements Serializable {
	private static final long serialVersionUID = -6886453255211223417L;
	private ArrayList<String> tableVOKeys = new ArrayList<String>(); // TableVO的key的集合
	private ArrayList<String> formVOKeys = new ArrayList<String>(); // FormVO的key的集合
	private HashMap<String, TableVO> hmKeyToTableVO = new HashMap<String, TableVO>(); // TableVO的key--->CellVO
	private HashMap<String, FormVO> hmKeyToFormVO = new HashMap<String, FormVO>(); // FormVO的key--->CellVO
	private ArrayList<TableVO> alTableVO = new ArrayList<TableVO>(); // TableVO的集合
	private ArrayList<FormVO> alFormVO = new ArrayList<FormVO>(); // FormVO的集合

	/**
	 * 生成一个DataVO实例  
	 */
	public DataVO() {
		super();
	}

	/**
	 * 得到当前DataVO的TableVO数据集
	 * @return ArrayList<TableVO>
	 */
	public ArrayList<TableVO> getTableVOs() {
		return alTableVO;
	}

	/**
	* 得到当前DataVO的FormVO数据集
	* @return ArrayList<FormVO>
	*/
	public ArrayList<FormVO> getFormVOs() {
		return alFormVO;
	}

	/**
	 * 通过TableVO的key取到TableVO
	 * @param key
	 * @return TableVO
	 */
	public TableVO getTableVO(String key) {
		if (hmKeyToTableVO.containsKey(key)) {
			return hmKeyToTableVO.get(key);
		} else {
			return null;
		}
	}

	/**
	 * 通过FormVO的key取到FormVO
	 * @param key
	 * @return FormVO
	 */
	public FormVO getFormVO(String key) {
		if (hmKeyToFormVO.containsKey(key)) {
			return hmKeyToFormVO.get(key);
		} else {
			return null;
		}
	}

	/**
	 * 根据TableVO在DataVO中的位置取出TableVO,index是从0开始的.
	 * @param index
	 * @return TableVO
	 */
	public TableVO getTableVO(int index) {
		return alTableVO.get(index);
	}

	/**
	 * 根据FormVO在DataVO中的位置取出FormVO,index是从0开始的.
	 * @param index
	 * @return FormVO
	 */
	public FormVO getFormVO(int index) {
		return alFormVO.get(index);
	}

	/**
	 * 向当前DataVO中添加一个TableVO
	 * @param tableVO
	 */
	public void addTableVO(TableVO tableVO) {
		if (StringUtils.isNotEmpty(tableVO.getKey())) {
			tableVOKeys.add(tableVO.getKey());
			hmKeyToTableVO.put(tableVO.getKey(), tableVO);
		}
		alTableVO.add(tableVO);
	}

	/**
	 * 向当前DataVO中添加一个FormVO
	 * @param formVO
	 */
	public void addFormVO(FormVO formVO) {
		if (StringUtils.isNotEmpty(formVO.getKey())) {
			formVOKeys.add(formVO.getKey());
			hmKeyToFormVO.put(formVO.getKey(), formVO);
		}
		alFormVO.add(formVO);
	}

	/**
	 * 得到当前DataVO中的TableVO数量
	 * @return int
	 */
	public int sizeByTableVO() {
		return alTableVO.size();
	}

	/**
	 * 得到当前DataVO中的FormVO数量
	 * @return int
	 */
	public int sizeByFormVO() {
		return alFormVO.size();
	}

	/**
	 * 将一个DataVO的内容放入当前DataVO.
	 * @param dataVO
	 */
	public void addDataVO(DataVO dataVO) {
		for (int i = 0; i < dataVO.getTableVOs().size(); i++) {
			this.addTableVO(dataVO.getTableVOs().get(i));
		}
		for (int i = 0; i < dataVO.getFormVOs().size(); i++) {
			this.addFormVO(dataVO.getFormVOs().get(i));
		}
	}

	/**
	 * 将当前DataVO中的TableVO,FormVO根据key进行排序整理
	 */
	public void sortSubVOByKey() {
		// 处理FormVO的排序
		Object[] formVOKeyArray = this.formVOKeys.toArray();
		ArrayList<FormVO> formVOArrayTmp = new ArrayList<>();
		formVOArrayTmp.addAll(this.alFormVO);
		this.alFormVO.clear();
		Arrays.sort(formVOKeyArray);
		for (int i = 0; i < formVOKeyArray.length; i++) {
			this.addFormVO(this.getFormVO((String) formVOKeyArray[i]));
		}
		// 处理TableVO的排序
		Object[] tableVOKeyArray = this.tableVOKeys.toArray();
		ArrayList<TableVO> tableVOArrayTmp = new ArrayList<>();
		tableVOArrayTmp.addAll(this.alTableVO);
		this.alTableVO.clear();
		Arrays.sort(tableVOKeyArray);
		for (int i = 0; i < tableVOKeyArray.length; i++) {
			this.addTableVO(this.getTableVO((String) tableVOKeyArray[i]));
		}
	}
}
