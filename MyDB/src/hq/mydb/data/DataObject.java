package hq.mydb.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

public abstract class DataObject implements Serializable {
	private static final long serialVersionUID = -7730134303214442839L;

	public static final char OPERATION_UNDEFINED = 'x'; // 未知操作
	public static final char OPERATION_UPDATE = 'u'; // 更新表数据
	public static final char OPERATION_INSERT = 'i'; // 插入表数据
	public static final char OPERATION_DELETE = 'd'; // 删除表数据

	private String key = ""; // 组件的key

	private ArrayList<DataObject> alParentVO = new ArrayList<DataObject>();// 当前组件的父组件实例

	private ArrayList<String> alKeyOfChildVO = new ArrayList<String>(); // 包含的子组件的key的集合
	private HashMap<String, DataObject> hmKeyToChildVO = new HashMap<String, DataObject>(); // 子组件的key--->CellVO
	private ArrayList<DataObject> alChildVO = new ArrayList<DataObject>(); // 子组件的集合

	protected DataObject() {
	}

	protected DataObject(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	/**
	 * 更新自身的Key
	 * 	!! 如果有父亲,则需要更新父亲中自己的key
	 * @param key
	 */
	public void setKey(String key) {
		// 更新所属父类中的keySet
		String key_temp = this.key;// 由于父Table会有多个.导致第一个Table处理之后,this.key变了.之后其它的父Table中的key就不会变更了.
		if (this.alParentVO.size() > 0) {
			for (DataObject parentVO : this.alParentVO) {
				parentVO.replaceChildVOKey(key_temp, key);
			}
		}
		this.key = key;
	}

	/**
	 * 给Parent对象使用的设定Key
	 */
	protected void parentVOUseSetKey(String key) {
		this.key = key;
	}

	/**
	 * 将当前RowVO中key为OldKey的CellVO更新为newKey,并更新相关操作类变量
	 * @param oldKey
	 * @param newKey
	 */
	protected void replaceChildVOKey(String oldKey, String newKey) {
		/*
		 * 替换key集合中的值
		 */
		if (this.alKeyOfChildVO.contains(oldKey)) {
			this.alKeyOfChildVO.set(this.alKeyOfChildVO.indexOf(oldKey), newKey);
		}
		/*
		 * 替换HashMap中的key
		 */
		if (this.hmKeyToChildVO.containsKey(oldKey)) {
			DataObject childVOBean = this.hmKeyToChildVO.get(oldKey);
			childVOBean.parentVOUseSetKey(newKey);
			this.hmKeyToChildVO.remove(oldKey);
			this.hmKeyToChildVO.put(newKey, childVOBean);
		}
	}

	/**
	 * 得到当前VO的子VO的key集合
	 * @return
	 */
	protected ArrayList<String> getChildVOKeyArray() {
		return alKeyOfChildVO;
	}

	/**
	 * 得到当前VO的子VO集合
	 * @return
	 */
	protected ArrayList<DataObject> getChildVOArray() {
		return alChildVO;
	}

	/**
	 * 添加一组子VO
	 */
	protected void addChildVOArray(ArrayList<DataObject> childVOArray) {
		for (DataObject childVO : childVOArray) {
			this.addChildVO(childVO);
		}
	}

	/**
	 * 用传入的childVOArray替换当前组件的childVOArray
	 * !! 传入参数无法使用泛型.因为传入的都是继承了DataObject的子类.
	 * @param childVOArray
	 */
	protected void setChildVOArray(ArrayList childVOArray) {
		// 1. 构建一个临时的Array用于存储传入的Array中的数据.
		ArrayList<DataObject> tmpArray = new ArrayList<DataObject>();
		tmpArray.addAll(childVOArray);
		// 2. 清空传入的数组
		childVOArray.clear();
		// 3. 清空当前组件中的所有子组件
		this.clearChildVO();
		// 4. 将传入的Array的地址赋予this.childVOArray
		this.alChildVO = childVOArray;
		// 5. 将临时的Array中的子组件放入当前组件中.
		for (DataObject childVO : tmpArray) {
			this.addChildVO(childVO);
		}
	}

	/**
	 * 添加一个子VO
	 * @param childVO
	 */
	protected void addChildVO(DataObject childVO) {
		this.alKeyOfChildVO.add(childVO.getKey());
		this.hmKeyToChildVO.put(childVO.getKey(), childVO);
		this.alChildVO.add(childVO);
		childVO.addParentVO(this);
	}

	/**
	 * 得到当前VO的父VO集合
	 * @return
	 */
	protected ArrayList<DataObject> getParentVOArray() {
		return this.alParentVO;
	}

	/**
	 * 得到一个父VO
	 * @return
	 */
	protected DataObject getParentVO() {
		if (this.alParentVO.size() > 0) {
			return this.alParentVO.get(0);
		} else {
			return null;
		}
	}

	/**
	 * 添加一个父VO
	 * @param parentVO
	 */
	protected void addParentVO(DataObject parentVO) {
		this.alParentVO.add(parentVO);
	}

	/**
	 * 根据CellVO在RowVO中的位置取出CellVO,index是从0开始的.
	 * 
	 * @param sortIndex
	 * @return ComponentBean
	 */
	protected DataObject getChildVO(int sortIndex) {
		return this.alChildVO.get(sortIndex);
	}

	/**
	 * 通过CellVO的key取到CellVO
	 * 
	 * @param childVOKey
	 * @return ComponentBean
	 */
	protected DataObject getChildVO(String childVOKey) {
		if (this.hmKeyToChildVO.containsKey(childVOKey)) {
			return this.hmKeyToChildVO.get(childVOKey);
		} else {
			return null;
		}
	}

	/**
	 * 根据Array序号移出一个子VO
	 * @param sortIndex
	 */
	protected void removeChildVO(int sortIndex) {
		this.removeChildVO(this.alChildVO.get(sortIndex));
	}

	/**
	 * 根据子VO的key移出子VO
	 * @param childVOKey
	 */
	protected void removeChildVO(String childVOKey) {
		this.removeChildVO(this.hmKeyToChildVO.get(childVOKey));
	}

	/**
	 * 移出子VO
	 * @param childVO
	 */
	protected void removeChildVO(DataObject childVO) {
		if (childVO != null) {
			// 从子VO数组中移出
			this.alChildVO.remove(childVO);
			if (StringUtils.isNotBlank(childVO.getKey())) {
				// 移出keyToChild HashMap
				this.hmKeyToChildVO.remove(childVO.getKey());
				// 移出ChildKey Array
				this.alKeyOfChildVO.remove(childVO.getKey());
			}
			// 移出子VO中的当前父VO
			childVO.removeParentVO(this);
		}
	}

	/**
	 * 移出自身的某一个parentVO
	 * @param parentVO
	 */
	protected void removeParentVO(DataObject parentVO) {
		this.alParentVO.remove(parentVO);
	}

	/**
	 * 验证是否存在传入key的子VO.
	 * @param key
	 * @return
	 */
	protected boolean containsChildVOKey(String key) {
		return this.alKeyOfChildVO.contains(key);
	}

	/**
	 * 验证传入的VO是否为当前VO的子VO
	 * @param childVO
	 * @return
	 */
	protected boolean containsChildVO(DataObject childVO) {
		return this.alChildVO.contains(childVO);
	}

	/**
	 * 得到当前组件实例的子组件数量
	 * 
	 * @return int
	 */
	protected int sizeOfChildVOArray() {
		return this.alChildVO.size();
	}

	/**
	 * 将子VO的ArrayList转换为DataObject[]返回
	 * @param childVOs
	 * @return
	 */
	protected DataObject[] toArrayFromChildVOs(DataObject[] childVOs) {
		return this.alChildVO.toArray(childVOs);
	}

	/**
	 * 清空子VO
	 * @return
	 */
	protected int clearChildVO() {
		int size = alChildVO.size();
		DataObject[] componentBeans = this.toArrayFromChildVOs(new DataObject[size]);
		for (DataObject componentBean : componentBeans) {
			this.removeChildVO(componentBean);
		}
		return size;
	}

}
