package hq.mydb.orderby;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import hq.mydb.exception.DAOException;

public class OrderBy implements Serializable {
	private static final long serialVersionUID = -1410719477566917971L;
	private ArrayList<Sort> alData = new ArrayList<Sort>();
	private HashMap<String, Sort> hmColumnNameToSort = new HashMap<String, Sort>();

	public OrderBy() {

	}

	public OrderBy(Sort sort) {
		super();
		addSort(sort);
	}

	public boolean addSort(Sort sort) {
		boolean isOk = true;
		if (sort.isNotEmpty()) {
			String columnName = sort.getColumnName();
			if (!hmColumnNameToSort.containsKey(columnName)) {
				hmColumnNameToSort.put(columnName, sort);
				// 如果没有序号则用当前alData数组的长度作为SortIndex
				if (StringUtils.isEmpty(sort.getSortIndex())) {
					sort.setSortIndex(String.valueOf(alData.size()));
				}
				alData.add(sort);
				// 重新排序
				this.sortDataList();
			} else {
				isOk = false;
			}
		} else {
			isOk = false;
		}
		return isOk;
	}

	/**
	 * 根据编号对当前alData进行排序
	 */
	private void sortDataList() {
		Collections.sort(alData, new Comparator<Sort>() {
			public int compare(Sort sortPrev, Sort sortNext) {
				String sortIndexPrev = sortPrev.getSortIndex();
				String sortIndexNext = sortNext.getSortIndex();
				return Integer.parseInt(sortIndexPrev) - Integer.parseInt(sortIndexNext);
			}
		});
	}

	public boolean containsColumnName(String columnName) {
		return hmColumnNameToSort.containsKey(columnName);
	}

	public Sort get(String columnName) {
		return hmColumnNameToSort.get(columnName);
	}

	public int size() {
		return this.alData.size();
	}

	public boolean isEmpty() {
		return StringUtils.isEmpty(this.toSQLString());
	}

	public boolean isNotEmpty() {
		return StringUtils.isNotEmpty(this.toSQLString());
	}

	public Sort get(int i) {
		return alData.get(i);
	}

	public String toSQLString() {
		StringBuffer sql = new StringBuffer();
		try {
			for (int i = 0; i < alData.size(); i++) {
				Sort sort = alData.get(i);
				String subSQL = sort.toSQLString();
				if (subSQL == null || subSQL.length() == 0) {
					continue;
				}
				if (i != 0) {
					sql.append(" , ");
				}
				sql.append(subSQL);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DAOException("********** Order By SQL 生成错误！**********");
		}
		return sql.toString();
	}

	public static OrderBy parseOrderBy(JSON json) {
		OrderBy ob = new OrderBy();
		if (json instanceof JSONObject) {
			JSONObject t = (JSONObject) json;
			ob.addSort(new Sort(t.getString("columnName"), t.getString("sort")));
		} else if (json instanceof JSONArray) {
			JSONArray ta = (JSONArray) json;
			for (Object object : ta) {
				JSONObject t = (JSONObject) object;
				ob.addSort(new Sort(t.getString("columnName"), t.getString("sort")));
			}
		}
		return ob;
	}
}
