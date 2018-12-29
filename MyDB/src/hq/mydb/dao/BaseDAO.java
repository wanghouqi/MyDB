package hq.mydb.dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;

import hq.mydb.condition.CondSetBean;
import hq.mydb.data.CellVO;
import hq.mydb.data.DataObject;
import hq.mydb.data.DataVO;
import hq.mydb.data.FormVO;
import hq.mydb.data.RowVO;
import hq.mydb.data.TableVO;
import hq.mydb.db.Column;
import hq.mydb.db.Table;
import hq.mydb.exception.DAOException;
import hq.mydb.orderby.OrderBy;
import hq.mydb.orderby.Sort;
import hq.mydb.utils.MyDBDefinition;
import hq.mydb.utils.MyDBHelper;

/**
 * 数据库相关操作
 * @author wanghq
 *
 */
public class BaseDAO {
	private JdbcTemplate jdbcTemplate;
	private Logger log = Logger.getLogger(this.getClass().getName());
	private String databaseType = "";// 数据库类型
	private String primaryKeyColumnName = "";// 主键栏位名
	private boolean showSQL = false;// 是否显示SQL
	private static HashMap<String, Table> hmNameToTable = new HashMap<String, Table>();// 数据库表名与Table对象关系,系统启动时初始化.

	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public String getPrimaryKeyColumnName() {
		return primaryKeyColumnName;
	}

	public void setPrimaryKeyColumnName(String primaryKeyColumnName) {
		this.primaryKeyColumnName = primaryKeyColumnName;
		MyDBHelper.setPrimaryKeyColumnName(primaryKeyColumnName);
	}

	public boolean isShowSQL() {
		return showSQL;
	}

	public void setShowSQL(boolean showSQL) {
		this.showSQL = showSQL;
	}

	/**
	 * 系统启动初始化,将当前连接的数据库的信息加载到内存,生成hmNameToTable
	 */
	public void initDataBaseCache() {
		System.out.println("拟美，终于可以junit测试了");
		if (BaseDAO.hmNameToTable.size() == 0) {
			switch (this.databaseType) {
			case MyDBDefinition.DATABASE_TYPE_MYSQL:
				initDataBaseCache_mySQL();
				break;
			case MyDBDefinition.DATABASE_TYPE_SQLSERVER:
				initDataBaseCache_sqlServer();
				break;
			case MyDBDefinition.DATABASE_TYPE_ORACLE:
				initDataBaseCache_oracle();
				break;
			case MyDBDefinition.DATABASE_TYPE_MARIADB:
				initDataBaseCache_mariaDB();
				break;

			default:
				if (BaseDAO.hmNameToTable.size() == 0) {
					throw new DAOException("System initialization fail! NameToTable is empty.");
				}
			}
		}
	}

	private void initDataBaseCache_mySQL() {

	}

	private void initDataBaseCache_sqlServer() {

	}

	private void initDataBaseCache_oracle() {

	}

	private void initDataBaseCache_mariaDB() {

	}

	/**
	 * 生成一个新的数据库主键ID
	 * @return String
	 */
	public String createDataBaseId() {
		String primaryId = UUID.randomUUID().toString();
		primaryId = primaryId.replace("-", "");
		return primaryId;
	}

	/**
	 * 得到当前连接的数据库的名称,例: ERP_STD
	 * @return
	 */
	public String getDataBaseName() {
		String dataBaseName = "";
		try {
			DatabaseMetaData databaseMetaData = this.jdbcTemplate.getDataSource().getConnection().getMetaData();
			String url = databaseMetaData.getURL();
			if (url.contains("databaseName=")) {
				String tmp = url.split("databaseName=")[1];
				dataBaseName = tmp.substring(0, tmp.indexOf(";"));
			}
		} catch (Exception e) {
			throw new DAOException("getDataBaseName()", e);
		}
		return dataBaseName;
	}

	/**
	 * 得到当前连接的数据库的IP,例: 192.168.1.1
	 * @return
	 */
	public String getDataBaseIP() {
		String dataBaseIP = "";
		try {
			DatabaseMetaData databaseMetaData = this.jdbcTemplate.getDataSource().getConnection().getMetaData();
			String url = databaseMetaData.getURL();
			String[] strTmp = url.split(":");
			for (int i = 0; i < strTmp.length; i++) {
				if (strTmp[i].replace(".", "@@").split("@@").length == 4) {
					dataBaseIP = strTmp[i];
					break;
				}
			}

		} catch (Exception e) {
			throw new DAOException("getDataBaseIP()", e);
		}
		return dataBaseIP;
	}

	/**
	/**
	 *  根据querySQL查询数据,这个查询SQL中没有 ? 通配符
	 *  SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param querySQL
	 * @return List
	 */
	public List queryForList(String querySQL) {
		return queryForList(querySQL, null);
	}

	/**
	/**
	 *  根据countSQL查询数据,这个查询SQL中没有 ? 通配符
	 *  SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param querySQL
	 * @return int
	 */
	public int queryForCountSql(String countSQL) {
		if (this.isShowSQL()) {
			log.info(countSQL);
		}
		int totalDataNumber = 0;
		try {
			totalDataNumber = this.jdbcTemplate.queryForInt(countSQL);
		} catch (Exception e) {
			throw new DAOException("BaseDAO queryForCountSql Exception \r\n      querySQL : " + countSQL, e);
		}
		return totalDataNumber;
	}

	/**
	 *  根据querySQL查询数据,这个查询SQL中有 ? 通配符
	 *  SELECT 栏位1,栏位2... FROM 表名 WHERE name=? and age>?
	 * @param querySQL
	 * @param parameters
	 * @return List
	 */
	public List queryForList(String querySQL, Object[] parameters) {
		if (this.isShowSQL()) {
			log.info(querySQL);
		}
		List al = new ArrayList();
		try {
			if (parameters != null && parameters.length > 0) {
				al = this.jdbcTemplate.queryForList(querySQL, parameters);
			} else {
				al = this.jdbcTemplate.queryForList(querySQL);
			}
		} catch (Exception e) {
			throw new DAOException("BaseDAO queryForList Exception \r\n      querySQL : " + querySQL, e);
		}
		return al;
	}

	//============by column
	/**
	 * 根据传入的tableName,读取columnName等于value的[指定栏位]的TableVO数据.
	 * SELECT * FROM tableName WHERE columnName = value
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnEqual(String tableName, String columnName, String value) {
		return this.queryForTableVOByColumnEqual(new Table(tableName), columnName, value);
	}

	/**
	 * 根据传入的tableName,读取columnName等于value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT * FROM tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param sort : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnEqual(String tableName, String columnName, String value, Sort sort) {
		return queryForTableVOByColumnEqual(tableName, columnName, value, new OrderBy(sort));
	}

	/**
	 * 根据传入的tableName,读取columnName等于value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT * FROM tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param orderBy : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnEqual(String tableName, String columnName, String value, OrderBy orderBy) {
		return this.queryForTableVOByColumnEqual(new Table(tableName), columnName, value, orderBy);
	}

	/**
	 * 根据传入的table,读取columnName等于value的[指定栏位]的TableVO数据.
	 * SELECT table.attr,table.attr,table.attr... FROM table.tableName WHERE columnName = value
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnEqual(Table table, String columnName, String value) {
		return this.queryForTableVOByColumnEqual(table, columnName, value, new OrderBy());
	}

	/**
	 * 根据传入的table,读取columnName等于value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT table.attr,table.attr,table.attr... FROM table.tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param sort : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnEqual(Table table, String columnName, String value, Sort sort) {
		return queryForTableVOByColumnEqual(table, columnName, value, new OrderBy(sort));
	}

	/**
	 * 根据传入的table,读取columnName等于value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT table.attr,table.attr,table.attr... FROM table.tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param orderBy : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnEqual(Table table, String columnName, String value, OrderBy orderBy) {
		CondSetBean csb = new CondSetBean();
		csb.addCondBean_equal(columnName, value);
		return this.queryForTableVO(table, csb, orderBy);
	}

	//============by column

	//============In column
	/**
	 * 根据传入的tableName,读取columnName In value的[指定栏位]的TableVO数据.
	 * SELECT * FROM tableName WHERE columnName = value
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnIn(String tableName, String columnName, HashSet<String> inValues) {
		return this.queryForTableVOByColumnIn(new Table(tableName), columnName, inValues);
	}

	/**
	 * 根据传入的tableName,读取columnName In value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT * FROM tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param sort : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnIn(String tableName, String columnName, HashSet<String> inValues, Sort sort) {
		return queryForTableVOByColumnIn(tableName, columnName, inValues, new OrderBy(sort));
	}

	/**
	 * 根据传入的tableName,读取columnName In value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT * FROM tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param orderBy : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnIn(String tableName, String columnName, HashSet<String> inValues, OrderBy orderBy) {
		return this.queryForTableVOByColumnIn(new Table(tableName), columnName, inValues, orderBy);
	}

	/**
	 * 根据传入的table,读取columnName In value的[指定栏位]的TableVO数据.
	 * SELECT table.attr,table.attr,table.attr... FROM table.tableName WHERE columnName = value
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnIn(Table table, String columnName, HashSet<String> inValues) {
		return this.queryForTableVOByColumnIn(table, columnName, inValues, new OrderBy());
	}

	/**
	 * 根据传入的table,读取columnName In value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT table.attr,table.attr,table.attr... FROM table.tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param sort : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnIn(Table table, String columnName, HashSet<String> inValues, Sort sort) {
		return queryForTableVOByColumnIn(table, columnName, inValues, new OrderBy(sort));
	}

	/**
	 * 根据传入的table,读取columnName In value的[指定栏位]的TableVO数据,并依据orderBy排序
	 * SELECT table.attr,table.attr,table.attr... FROM table.tableName WHERE columnName = value ORDER BY orderBy
	 * @param tableName : 数据库表名
	 * @param columnName : 栏位名
	 * @param value : 值
	 * @param orderBy : 排序规则
	 * @return TableVO
	 */
	public TableVO queryForTableVOByColumnIn(Table table, String columnName, HashSet<String> inValues, OrderBy orderBy) {
		CondSetBean csb = new CondSetBean();
		csb.addCondBean_in(columnName, inValues);
		return this.queryForTableVO(table, csb, orderBy);
	}

	//============In column

	//============by DB Name
	/**
	 * 根据dbName查询数据库,没有where条件,
	 * SELECT * FROM 表名
	 * @param dbName
	 * @return TableVO
	 */
	public TableVO queryForTableVOByDBName(String dbName) {
		return queryForTableVOByDBName(dbName, new OrderBy());
	}

	/**
	 * 根据dbName查询数据库,没有where条件,
	 * SELECT *. FROM 表名 order by
	 * @param dbName
	 * @param sort
	 * @return TableVO
	 */
	public TableVO queryForTableVOByDBName(String dbName, Sort sort) {
		return queryForTableVOByDBName(dbName, new OrderBy(sort));
	}

	/**
	 * 根据dbName查询数据库,没有where条件,
	 * SELECT *. FROM 表名 order by
	 * @param dbName
	 * @param orderBy
	 * @return TableVO
	 */
	public TableVO queryForTableVOByDBName(String dbName, OrderBy orderBy) {
		return queryForTableVOByDBName(dbName, null, orderBy);
	}

	/**
	 * 根据dbName查询数据库,condSetBean里边放的是Where的条件.
	 * SELECT * FROM 表名 WHERE 条件
	 * @param dbName
	 * @param condSetBean
	 * @return TableVO
	 */
	public TableVO queryForTableVOByDBName(String dbName, CondSetBean condSetBean) {
		return queryForTableVOByDBName(dbName, condSetBean, new OrderBy());
	}

	/**
	 * 根据dbName查询数据库,condSetBean里边放的是Where的条件.
	 * SELECT * FROM 表名 WHERE 条件   order by 
	 * @param dbName
	 * @param condSetBean
	 * @param sort
	 * @return TableVO
	 */
	public TableVO queryForTableVOByDBName(String dbName, CondSetBean condSetBean, Sort sort) {
		return queryForTableVOByDBName(dbName, condSetBean, new OrderBy(sort));
	}

	/**
	 * 根据dbName查询数据库,condSetBean里边放的是Where的条件.
	 * SELECT * FROM 表名 WHERE 条件   order by 
	 * @param dbName
	 * @param condSetBean
	 * @param orderBy
	 * @return TableVO
	 */
	public TableVO queryForTableVOByDBName(String dbName, CondSetBean condSetBean, OrderBy orderBy) {
		return queryForTableVO(new Table(dbName), condSetBean, orderBy);
	}

	//============by DB Name

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,没有where条件,
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名
	 * @param table
	 * @return TableVO
	 */
	public TableVO queryForTableVO(Table table) {
		return queryForTableVO(table, new OrderBy());
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,没有where条件,
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名
	 * @param table
	 * @param sort
	 * @return TableVO
	 */
	public TableVO queryForTableVO(Table table, Sort sort) {
		return queryForTableVO(table, new OrderBy(sort));
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,没有where条件,
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名
	 * @param table
	 * @param orderBy
	 * @return TableVO
	 */
	public TableVO queryForTableVO(Table table, OrderBy orderBy) {
		return queryForTableVO(table, null, orderBy);
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,condSetBean里边放的是Where的条件.
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @return TableVO
	 */
	public TableVO queryForTableVO(Table table, CondSetBean condSetBean) {
		return queryForTableVO(table, condSetBean, new OrderBy());
	}

	public TableVO queryForTableVOOnSQL(String querySQL) {
		TableVO tableVO = new TableVO();
		try {
			List list = this.queryForList(querySQL);
			tableVO = this.buildTableVOByQueryList(list);
		} catch (Exception e) {
			throw new DAOException("BaseDAO queryForTableVO Exception \r\n      querySQL : " + querySQL, e);
		}
		return tableVO;
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,condSetBean里边放的是Where的条件.
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @param srot
	 * @return TableVO
	 */
	public TableVO queryForTableVO(Table table, CondSetBean condSetBean, Sort srot) {
		return this.queryForTableVO(table, condSetBean, new OrderBy(srot));
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,condSetBean里边放的是Where的条件.
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @param orderBy
	 * @return TableVO
	 */
	public TableVO queryForTableVO(Table table, CondSetBean condSetBean, OrderBy orderBy) {
		TableVO tableVO = new TableVO();
		try {
			String querySQL = this.buildQuerySQL(table, condSetBean, orderBy);
			tableVO = this.queryForTableVOOnSQL(querySQL);
			tableVO.setKey(table.getName());
		} catch (Exception e) {
			throw new DAOException("The queryForTableVO(Table table, CondSetBean condSetBean, OrderBy orderBy) throw Exception", e);
		}
		return tableVO;
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,没有where条件,
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名
	 * @param table
	 * @param currentPaqeIndex
	 * @param onePageDataNumber
	 * @param sort
	 * @return TableVO
	 */
	public TableVO queryForTableVOOnSplitPage(Table table, int currentPaqeIndex, int onePageDataNumber, Sort sort) {
		return queryForTableVOOnSplitPage(table, currentPaqeIndex, onePageDataNumber, new OrderBy(sort));
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,没有where条件,
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名
	 * @param table
	 * @param currentPaqeIndex
	 * @param onePageDataNumber
	 * @param orderBy
	 * @return TableVO
	 */
	public TableVO queryForTableVOOnSplitPage(Table table, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) {
		return queryForTableVOOnSplitPage(table, null, currentPaqeIndex, onePageDataNumber, orderBy);
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,condSetBean里边放的是Where的条件.
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @param currentPaqeIndex
	 * @param onePageDataNumber
	 * @param sort
	 * @return TableVO
	 */
	public TableVO queryForTableVOOnSplitPage(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, Sort sort) {
		return queryForTableVOOnSplitPage(table, condSetBean, currentPaqeIndex, onePageDataNumber, new OrderBy(sort));
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,condSetBean里边放的是Where的条件.
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @param currentPaqeIndex
	 * @param onePageDataNumber
	 * @param orderBy
	 * @return TableVO
	 */
	public TableVO queryForTableVOOnSplitPage(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) {
		TableVO tableVO = new TableVO();
		try {
			String querySQL = this.buildSQLForSplitPage(table, condSetBean, currentPaqeIndex, onePageDataNumber, orderBy);
			List list = this.queryForList(querySQL);
			tableVO = this.buildTableVOByQueryList(list);
			tableVO.setKey(table.getName());
		} catch (Exception e) {
			throw new DAOException(
					"The queryForTableVOOnSplitPage(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) throw Exception", e);
		}
		return tableVO;
	}

	/**
	 * 将jdbcTemplate.queryForList这次查询SQL返回的List构建为一个TableVO
	 * @param list
	 * @return TableVO
	 */
	private TableVO buildTableVOByQueryList(List list) {
		return this.buildTableVOByQueryList(list, null);
	}

	/**
	 * 将jdbcTemplate.queryForList这次查询SQL返回的List构建为一个TableVO
	 * @param list : 数据库查询的结果集
	 * @param table : 表信息
	 * @return TableVO
	 */
	private TableVO buildTableVOByQueryList(List list, Table table) {
		TableVO tableVO = new TableVO();
		try {
			for (int i = 0; i < list.size(); i++) {
				RowVO rowVO = new RowVO();
				HashMap hm = (HashMap) list.get(i);
				Set keySet = hm.keySet();
				Iterator it = keySet.iterator();
				while (it.hasNext()) {
					String key = (String) it.next();
					Object value_obj = hm.get(key);
					String value = "";
					if (value_obj != null) {
						value = String.valueOf(value_obj);// 这里的返回类型是根数据库栏位类型相同,需要转为字符串
					}
					if (StringUtils.equals(this.getPrimaryKeyColumnName(), key)) {
						rowVO.setKey(value);// 如果有主键,则使用主键作为RowVO的key
					}
					rowVO.addCellVO(new CellVO(key, value));
				}
				tableVO.addRowVO(rowVO);
			}
		} catch (Exception e) {
			throw new DAOException("The buildTableVOByQueryList(List list) throw Exception", e);
		}
		return tableVO;
	}

	/**
	 *  根据querySQL查询数据,这个查询SQL中没有 ? 通配符
	 *  SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param querySQL
	 * @return Map
	 */
	public Map queryForMap(String querySQL) {
		return queryForMap(querySQL, null);
	}

	/**
	 *  根据querySQL查询数据,这个查询SQL中有 ? 通配符
	 *  SELECT 栏位1,栏位2... FROM 表名 WHERE name=? and age>?
	 * @param querySQL
	 * @param parameters
	 * @return Map
	 */
	public Map queryForMap(String querySQL, Object[] parameters) {
		if (this.isShowSQL()) {
			log.info(querySQL);
		}
		Map map = new HashMap();
		try {
			try {
				if (parameters != null && parameters.length > 0) {
					map = this.jdbcTemplate.queryForMap(querySQL, parameters);
				} else {
					map = this.jdbcTemplate.queryForMap(querySQL);
				}
			} catch (EmptyResultDataAccessException ex) {
				map = new HashMap();
			}
		} catch (Exception e) {
			throw new DAOException("BaseDAO queryForMap Exception \r\n      querySQL : " + querySQL, e);
		}
		return map;
	}

	/**
	 * 查询CN_ID=primaryId的dbName表记录
	 * SELECT * FROM dbName WHERE CN_ID=primaryId
	 * @param dbName
	 * @param primaryId
	 * @return FormVO
	 */
	public FormVO queryForFormVOById(String dbName, String primaryId) {
		return queryForFormVOById(new Table(dbName), primaryId);
	}

	/**
	 * 查询CN_ID=primaryId的table对应表记录,返回指定的字段.
	 * SELECT table.attr,table.attr,table.attr... FROM table.dbName WHERE CN_ID=primaryId
	 * @param table
	 * @param primaryId
	 * @return FormVO
	 */
	public FormVO queryForFormVOById(Table table, String primaryId) {
		CondSetBean csb = new CondSetBean();
		csb.addCondBean_equal(this.getPrimaryKeyColumnName(), primaryId);
		return queryForFormVO(table, csb);
	}

	/**
	 * 根据dbName查询数据库,condSetBean里边放的是Where的条件.
	 * SELECT * FROM 表名 WHERE 条件
	 * @param tableName
	 * @param condSetBean
	 * @return FormVO
	 */
	public FormVO queryForFormVOByDBName(String tableName, CondSetBean condSetBean) {
		Table table = new Table(tableName);
		return queryForFormVO(table, condSetBean);
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,没有where条件,
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名
	 * @param table
	 * @return FormVO
	 */
	public FormVO queryForFormVO(Table table) {
		return queryForFormVO(table, null);
	}

	/**
	 * 根据Table查询数据库,Table的tableName是数据库的表名,condSetBean里边放的是Where的条件.
	 * Table的attribute是SQL的栏位名,如果没有则是返回所有 * .
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @return FormVO
	 */
	public FormVO queryForFormVO(Table table, CondSetBean condSetBean) {
		FormVO formVO = new FormVO();
		try {
			String querySQL = this.buildQuerySQL(table, condSetBean);
			Map map = this.queryForMap(querySQL);
			formVO = this.buildFormVOByQueryMap(map);
			formVO.setKey(table.getName());
		} catch (Exception e) {
			throw new DAOException("The queryForFormVO(Table table, CondSetBean condSetBean) throw Exception", e);
		}
		return formVO;
	}

	/**
	 * 将jdbcTemplate.queryForList这次查询SQL返回的List构建为一个TableVO
	 * @param map
	 * @return FormVO
	 */
	private FormVO buildFormVOByQueryMap(Map map) {
		FormVO formVO = new FormVO();
		try {
			Iterator it = map.keySet().iterator();
			while (it.hasNext()) {
				String key = (String) it.next();
				Object value_obj = map.get(key);
				String value = "";
				if (value_obj != null) {
					value = String.valueOf(value_obj);// 这里的返回类型是根数据库栏位类型相同,需要转为字符串
				}
				formVO.addCellVO(new CellVO(key, value));
			}
		} catch (Exception e) {
			throw new DAOException("The buildFormVOByQueryMap(Map map) throw Exception", e);
		}
		return formVO;
	}

	/**
	 * 根据Table生成查询语句,这种查询语句是没有where条件的
	 * SELECT 栏位1,栏位2... FROM 表名
	 * @param table
	 * @return String
	 */
	private String buildQuerySQL(Table table) {
		return buildQuerySQL(table, null, null);
	}

	/**
	 * 根据Table和condSetBean生成查询语句,condSetBean里边放的是Where的条件.
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @return String
	 */
	private String buildQuerySQL(Table table, CondSetBean condSetBean) {
		return buildQuerySQL(table, condSetBean, null);
	}

	/**
	 * 根据Table和condSetBean生成查询语句,condSetBean里边放的是Where的条件.
	 * SELECT 栏位1,栏位2... FROM 表名 WHERE 条件
	 * @param condSetBean
	 * @param orderBy
	 * @return String
	 */
	private String buildQuerySQL(Table table, CondSetBean condSetBean, OrderBy orderBy) {
		StringBuffer returnSQL = new StringBuffer();
		try {
			returnSQL.append("SELECT ");
			if (table.size() == 0) {
				returnSQL.append("* FROM ");
			} else {
				if (table.isDistinct()) {
					returnSQL.append(" DISTINCT ");
				}
				for (int i = 0; i < table.size(); i++) {
					Column column = table.get(i);
					// 放入查询字段名到SQL中
					returnSQL.append(column.getName());
					// 根据情况放入,和form连接
					if (i == table.size() - 1) {
						returnSQL.append(" FROM ");
					} else {
						returnSQL.append(", ");
					}
				}
			}
			returnSQL.append(table.getName());
			if (StringUtils.equals(databaseType, MyDBDefinition.DATABASE_TYPE_SQLSERVER)) {
				returnSQL.append(" (NOLOCK)");// SQL Server数据库查询时加入NOLOCK,不锁表.
			}
			// where 条件
			String whereSQL = null;
			if (condSetBean != null) {
				whereSQL = condSetBean.toSQLString();
			}

			if (StringUtils.isNotBlank(whereSQL)) {
				returnSQL.append(" WHERE ");
				returnSQL.append(whereSQL);
			}
			// order by
			if (orderBy != null && orderBy.isNotEmpty()) {
				returnSQL.append(" ORDER BY ");
				returnSQL.append(orderBy.toSQLString());
			} else {
				// 如果用户没有设定orderby则使用主键排序.
				returnSQL.append(" ORDER BY " + this.getPrimaryKeyColumnName());
			}
		} catch (Exception e) {
			throw new DAOException("The buildQuerySQL(Table table, CondSetBean condSetBean, OrderBy orderBy) throw Exception", e);
		}
		return returnSQL.toString();
	}

	/**
	 * 根据Table生成查询语句,这种查询语句是没有where条件的
	 * SELECT COUNT(*) FROM 表名
	 * @param table
	 * @return String
	 */
	public String buildCountSQL(Table table) {
		return buildCountSQL(table, null);
	}

	/**
	 * 根据Table和condSetBean生成查询语句,condSetBean里边放的是Where的条件.
	 * SELECT COUNT(*) FROM 表名 WHERE 条件
	 * @param table
	 * @param condSetBean
	 * @return String
	 */
	public String buildCountSQL(Table table, CondSetBean condSetBean) {
		StringBuffer returnSQL = new StringBuffer();
		try {

			String columnSQL = "";
			if (table.size() == 0 || !table.isDistinct()) {
				columnSQL = " * ";
			} else {
				StringBuffer columnSQLT = new StringBuffer();
				boolean isStart = true;
				for (int i = 0; i < table.size(); i++) {
					Column column = table.get(i);
					if (!isStart) {
						columnSQLT.append("+");
					} else {
						isStart = false;
					}
					switch (column.getType()) {
					case Column.TYPE_LONG:
					case Column.TYPE_DATE:
					case Column.TYPE_DATETIME:
					case Column.TYPE_DECIMAL:
						columnSQLT.append("CONVERT(NVARCHAR(50),ISNULL(" + column.getName() + ",0))");
						break;
					case Column.TYPE_TEXT:
					case Column.TYPE_VARCHAR:
						columnSQLT.append("ISNULL(" + column.getName() + ",'')");
						break;
					}
				}
				columnSQL = columnSQLT.toString();
			}

			returnSQL.append("SELECT");
			if (table.isDistinct()) {
				returnSQL.append(" DISTINCT ");
			}
			returnSQL.append(" COUNT(");
			returnSQL.append(columnSQL);
			returnSQL.append(") FROM ");

			returnSQL.append(table.getName());

			// where 条件
			String whereSQL = null;
			String csbWhereSQL = "";
			if (condSetBean != null) {
				csbWhereSQL = condSetBean.toSQLString();
			}
			if (StringUtils.isNotBlank(whereSQL)) {
				returnSQL.append(" WHERE ");
				returnSQL.append(whereSQL);
			}

		} catch (Exception e) {
			throw new DAOException("The buildCountSQL(Table table, CondSetBean condSetBean) throw Exception", e);
		}
		return returnSQL.toString();
	}

	private String buildSQLForSplitPage(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) {
		String returnSQL = "";
		if (StringUtils.equals(this.databaseType, MyDBDefinition.DATABASE_TYPE_SQLSERVER)) {
			returnSQL = this.buildSQLForSplitPageOnSQLServer(table, condSetBean, currentPaqeIndex, onePageDataNumber, orderBy);
		} else if (StringUtils.equals(this.databaseType, MyDBDefinition.DATABASE_TYPE_ORACLE)) {
			returnSQL = this.buildSQLForSplitPageOnOracle(table, condSetBean, currentPaqeIndex, onePageDataNumber);
		} else if (StringUtils.equals(this.databaseType, MyDBDefinition.DATABASE_TYPE_MYSQL)) {
			returnSQL = this.buildSQLForSplitPageOnMySQL(table, condSetBean, currentPaqeIndex, onePageDataNumber);
		} else if (StringUtils.equals(this.databaseType, MyDBDefinition.DATABASE_TYPE_MARIADB)) {
			returnSQL = this.buildSQLForSplitPageOnMariaDB(table, condSetBean, currentPaqeIndex, onePageDataNumber);
		}
		return returnSQL;
	}

	private String buildSQLForSplitPageOnMySQL(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber) {
		return null;
	}

	private String buildSQLForSplitPageOnMariaDB(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber) {
		return null;
	}

	private String buildSQLForSplitPageOnOracle(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber) {
		return null;
	}

	private String buildSQLForSplitPageOnSQLServer(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) {
		String returnSQL = new String();
		try {
			String columnSQL = "";
			if (table.size() == 0) {
				columnSQL = " * ";
			} else {
				StringBuffer columnSQLT = new StringBuffer();
				for (int i = 0; i < table.size(); i++) {
					Column column = table.get(i);
					columnSQLT.append(column.getName());
					if (i != table.size() - 1) {
						columnSQLT.append(", ");
					}
				}
				columnSQL = columnSQLT.toString();
			}
			// where 条件
			String whereSQL = null;
			String csbWhereSQL = "";
			if (condSetBean != null) {
				csbWhereSQL = condSetBean.toSQLString();
			}
			int maxRowNumber = onePageDataNumber * (currentPaqeIndex - 1);

			// sub SQL
			StringBuffer subSQL = new StringBuffer();
			subSQL.append("SELECT ");
			subSQL.append("TOP " + maxRowNumber + " ");
			subSQL.append(this.getPrimaryKeyColumnName() + " ");
			subSQL.append("FROM ");
			subSQL.append(table.getName());
			if (StringUtils.isNotBlank(whereSQL)) {
				subSQL.append(" WHERE ");
				subSQL.append(whereSQL);
			}
			if (orderBy != null && orderBy.isNotEmpty()) {
				subSQL.append(" ORDER BY ");
				subSQL.append(orderBy.toSQLString());
			} else {
				subSQL.append(" ORDER BY " + this.getPrimaryKeyColumnName());
			}

			// split Page SQL
			StringBuffer splitPageSQL = new StringBuffer();
			splitPageSQL.append("SELECT ");
			if (table.isDistinct()) {
				splitPageSQL.append(" DISTINCT ");
			}
			splitPageSQL.append("TOP " + onePageDataNumber + " ");
			splitPageSQL.append(columnSQL + " ");
			splitPageSQL.append("FROM ");
			splitPageSQL.append(table.getName() + " ");
			splitPageSQL.append("WHERE ");
			splitPageSQL.append(this.getPrimaryKeyColumnName() + " NOT IN (");
			splitPageSQL.append(subSQL.toString());
			splitPageSQL.append(")");
			if (StringUtils.isNotBlank(whereSQL)) {
				splitPageSQL.append(" AND (");
				splitPageSQL.append(whereSQL);
				splitPageSQL.append(" ) ");
			}
			if (orderBy != null && orderBy.isNotEmpty()) {
				splitPageSQL.append(" ORDER BY ");
				splitPageSQL.append(orderBy.toSQLString());
			} else {
				splitPageSQL.append(" ORDER BY " + this.getPrimaryKeyColumnName());
			}
			returnSQL = splitPageSQL.toString();
		} catch (Exception e) {
			throw new DAOException(
					"The buildSQLForSplitPageOnSQLServer(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) throw Exception", e);
		}
		return returnSQL;
	}

	/**
	 * 执行updateSQL
	 * @param updateSQL
	 * @return HashMap
	 */
	public void update(String updateSQL) {
		if (this.isShowSQL()) {
			String sql;
			if (updateSQL.length() > 1000) {
				sql = updateSQL.substring(0, 999);
			} else {
				sql = updateSQL;
			}
			log.info(sql);
		}
		try {
			this.jdbcTemplate.update(updateSQL);
		} catch (Exception e) {
			throw new DAOException("BaseDAO Update Exception \r\n      updateSQL : " + updateSQL, e);
		}
	}

	/**
	 * 用FormVO更新数据库,FormVO会生成UpdateSQL.
	 * UPDATE 表名称 SET 列名称1 = 新值1,列名称2 = 新值2 where .......
	 * @param formVO
	 * @return HashMap<String, String>
	 */
	public void update(FormVO formVO) {
		try {
			if (formVO.size() > 0) {
				if (StringUtils.equals(formVO.getOperation(), FormVO.OPERATION_UPDATE)) {
					TableVO tableVO = new TableVO(formVO.getKey(), formVO.getOperation());
					tableVO.addRowVO(formVO.transformToRowVO());
					this.update(tableVO);
				} else {
					throw new RuntimeException("********** DAO Exception！==> On update, formVO has no Operation **********");
				}
			} else {
				throw new RuntimeException("********** DAO Exception！==> On update, formVO has no any data! **********");
			}
		} catch (Exception e) {
			throw new DAOException("The update(FormVO formVO) throw Exception", e);
		}
	}

	/**
	 * 用TableVO更新数据库,TableVO会生成UpdateSQL.
	 * 用于更新的TableVO中必须包含主键
	 * UPDATE 表名称 SET 列名称1 = 新值1,列名称2 = 新值2 where CN_ID=值1
	 * @param tableVO
	 * @return HashMap<String, String>
	 */
	public void update(TableVO tableVO) {
		try {
			if (tableVO.size() > 0) {
				if (StringUtils.equals(tableVO.getOperation(), TableVO.OPERATION_UPDATE)) {
					ArrayList<String> sqls = tableVO.toSQLStrings();
					for (int i = 0; i < sqls.size(); i++) {
						this.update(sqls.get(i));
					}
				} else {
					throw new RuntimeException("********** DAO Exception！==> On update, tableVO has no Operation **********");
				}
			} else {
				throw new RuntimeException("********** DAO Exception！==> On update, tableVO has no any data! **********");
			}
		} catch (Exception e) {
			throw new DAOException("The update(TableVO tableVO) throw Exception", e);
		}
	}

	/**
	 * 执行Insert　SQL　返回新的ID值
	 * @param insertSQL
	 * @return HashMap<String, String> 
	 */
	public void insert(final String insertSQL) {
		if (this.isShowSQL()) {
			String sql;
			if (insertSQL.length() > 1000) {
				sql = insertSQL.substring(0, 999);
			} else {
				sql = insertSQL;
			}
			log.info(sql);
		}
		try {
			this.jdbcTemplate.update(new PreparedStatementCreator() {
				public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
					PreparedStatement ps = con.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
					return ps;
				}
			});
		} catch (Exception e) {
			throw new DAOException("BaseDAO Insert Exception \r\n      insertSQL : " + insertSQL, e);
		}
	}

	/**
	 * 用FormVO插入数据库记录,FormVO会生成Insert SQL.
	 * 传入的FromVO中不需要传入主键,在执行SQL语句之后系统将新的ID放入以CN_ID为Key的CellVO中
	 * INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 * @param formVO
	 */
	public void insert(FormVO formVO) {
		try {
			if (formVO.size() > 0) {
				if (StringUtils.equals(formVO.getOperation(), FormVO.OPERATION_INSERT)) {
					TableVO tableVO = new TableVO(formVO.getKey(), formVO.getOperation());
					RowVO rowVO = new RowVO();
					tableVO.addRowVO(rowVO);
					for (int i = 0; i < formVO.size(); i++) {
						rowVO.addCellVO(formVO.get(i));
					}
					// 执行SQL
					this.insert(tableVO);
					// 将执行完新增后所增加的栏位仿佛FormVO中
					RowVO rbInsert = tableVO.get(0);
					for (int i = 0; i < rbInsert.size(); i++) {
						CellVO cb = rbInsert.get(i);
						if (formVO.containsKey(cb.getKey())) {
							formVO.setCellVOValue(cb.getKey(), cb.getValue());
						} else {
							formVO.addCellVO(cb);
						}
					}
				} else {
					throw new RuntimeException("********** DAO Exception！==> On update, formVO has no Operation **********");
				}
			} else {
				throw new RuntimeException("********** DAO Exception！==> On update, formVO has no any data! **********");
			}
		} catch (Exception e) {
			throw new DAOException("The insert(FormVO formVO) throw Exception", e);
		}
	}

	/**
	 * 用TableVO插入数据库记录,TableVO会生成Insert SQL.
	 * 传入的TableVO中不需要传入主键,在执行SQL语句之后系统将新的ID放入以CN_ID为Key的CellVO中
	 * INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 * INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 * INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
	 * @param tableVO
	 * @param HashMap
	 */
	public void insert(TableVO tableVO) {
		try {
			if (StringUtils.equals(tableVO.getOperation(), TableVO.OPERATION_INSERT)) {
				// 放入主键
				for (int i = 0; i < tableVO.size(); i++) {
					RowVO rowVO = tableVO.get(i);
					// 放入主键
					if (rowVO.containsKey(this.getPrimaryKeyColumnName())) {
						if (StringUtils.isEmpty(rowVO.getCellVOValue(this.getPrimaryKeyColumnName()))) {
							rowVO.setCellVOValue(this.getPrimaryKeyColumnName(), this.createDataBaseId());
						}
					} else {
						rowVO.addCellVO(new CellVO(this.getPrimaryKeyColumnName(), this.createDataBaseId()));
					}
				}
				// 执行SQL
				ArrayList<String> sqls = tableVO.toSQLStrings();
				for (int i = 0; i < sqls.size(); i++) {
					this.insert(sqls.get(i));
				}
			} else {
				throw new RuntimeException("********** DAO Exception！==> On update, tableVO has no Operation **********");
			}
		} catch (Exception e) {
			throw new DAOException("The insert(TableVO tableVO) throw Exception", e);
		}
	}

	/**
	 * 执行deleteSQL
	 * @param deleteSQL
	 * @return String
	 */
	public String delete(String deleteSQL) {
		if (this.isShowSQL()) {
			log.info(deleteSQL);
		}
		String errorMessage = new String();
		try {
			this.jdbcTemplate.update(deleteSQL);
		} catch (Exception e) {
			errorMessage = "com.oletech.mdms.alert.deleteErrorOnFK";
			throw new DAOException("BaseDAO Delete Exception \r\n      deleteSQL : " + deleteSQL, e);
		}
		return errorMessage;
	}

	/**
	 * 删除tableName表记录,根据primaryId
	 * DELETE FROM 表名称  WHERE CN_ID = 'primaryId'
	 * @param tableName
	 * @param primaryId
	 */
	public String deleteById(String tableName, String primaryId) {
		String errorMessage = null;
		if (StringUtils.isNotEmpty(primaryId)) {
			CondSetBean csb = new CondSetBean();
			csb.addCondBean_equal(this.getPrimaryKeyColumnName(), primaryId);
			errorMessage = this.delete(tableName, csb);
		} else {
			throw new RuntimeException("********** DAO Exception！==> On deleteById(), primaryId is empty! **********");
		}
		return errorMessage;
	}

	/**
	 * 删除tableName表记录,根据primaryId
	 * DELETE FROM 表名称  WHERE CN_ID IN 'primaryId'
	 * @param tableName
	 * @param inPrimaryIds
	 */
	public String deleteInId(String tableName, HashSet<String> inPrimaryIds) {
		String errorMessage = null;
		if (inPrimaryIds != null && inPrimaryIds.size() > 0 && StringUtils.isNotEmpty(inPrimaryIds.iterator().next())) {
			if (inPrimaryIds.size() > 1000) {
				// 超过1000笔,每次处理1000笔
				HashSet<String> hmPrimaryId_t = new HashSet<String>();
				for (String primaryId : inPrimaryIds) {
					hmPrimaryId_t.add(primaryId);
					if (hmPrimaryId_t.size() == 1000) {
						errorMessage = this.delete(tableName, new CondSetBean().addCondBean_in(this.getPrimaryKeyColumnName(), hmPrimaryId_t));
						hmPrimaryId_t = new HashSet<String>();// 清空
					}
				}
				if (hmPrimaryId_t.size() > 0) {
					errorMessage = this.delete(tableName, new CondSetBean().addCondBean_in(this.getPrimaryKeyColumnName(), hmPrimaryId_t));
				}
			} else {
				errorMessage = this.delete(tableName, new CondSetBean().addCondBean_in(this.getPrimaryKeyColumnName(), inPrimaryIds));
			}
		} else {
			throw new RuntimeException("********** DAO Exception！==> On deleteInId(), inPrimaryIds is null or empty! **********");
		}
		return errorMessage;
	}

	/**
	 * 删除tableName表记录,根据condSetBean条件
	 * DELETE FROM 表名称  WHERE .......
	 * @param tableName
	 * @param condSetBean
	 */
	public String delete(String tableName, CondSetBean condSetBean) {
		String errorMessage = null;
		try {
			if (StringUtils.isEmpty(tableName)) {
				throw new RuntimeException("********** DAO Exception！==> On delete, tableName is null! **********");
			} else if (condSetBean == null || StringUtils.isEmpty(condSetBean.toSQLString())) {
				log.error("********** DAO Exception！==> On delete, condSetBean is null! **********");
				log.error("DELETE FROM " + tableName + " WHERE ");
				throw new RuntimeException("********** DAO Exception！==> On delete, condSetBean is null! **********");
			} else {
				errorMessage = this.delete("DELETE FROM " + tableName + " WHERE " + condSetBean.toSQLString());
			}
		} catch (Exception e) {
			throw new DAOException("The delete(String tableName, CondSetBean condSetBean) throw Exception", e);
		}
		return errorMessage;
	}

	/**
	 * 清空tableName表记录
	 * DELETE FROM 表名称 
	 * @param tableName
	 * @param String
	 */
	public String clearTable(String tableName) {
		String errorMessage = null;
		try {
			if (StringUtils.isEmpty(tableName)) {
				throw new RuntimeException("********** DAO Exception！==> On delete, tableName is null! **********");
			} else {
				errorMessage = this.delete("DELETE FROM" + tableName);
			}
		} catch (Exception e) {
			throw new DAOException("The clearTable(String tableName) throw Exception", e);
		}
		return errorMessage;
	}

	/**
	 * saveOrUpdateFormVO()
	 * 主键不存在或值为"",则执行插入,否则更新.
	 * @param formVO
	 * @return String
	 */
	public void saveOrUpdateFormVO(FormVO formVO) {
		try {
			if (formVO.size() == 0) {
				// 没有记录就无需处理保存
				return;
			}
			TableVO tableVO = new TableVO(formVO.getKey(), formVO.getOperation());
			RowVO rowVO = new RowVO();
			tableVO.addRowVO(rowVO);
			for (int i = 0; i < formVO.size(); i++) {
				rowVO.addCellVO(formVO.get(i));
			}
			this.saveOrUpdateTableVO(tableVO);
			if (formVO.get(this.getPrimaryKeyColumnName()) == null) {
				formVO.addCellVO(new CellVO(this.getPrimaryKeyColumnName(), rowVO.getCellVOValue(this.getPrimaryKeyColumnName())));
			} else {
				formVO.setCellVOValue(this.getPrimaryKeyColumnName(), rowVO.getCellVOValue(this.getPrimaryKeyColumnName()));
			}
		} catch (Exception e) {
			throw new DAOException("The saveOrUpdateFormVO(FormVO formVO, String userId, String companyId) throw Exception", e);
		}
	}

	/**
	 * saveOrUpdateTableVO()
	 * 主键不存在或值为"",则执行插入,否则更新.
	 * @param tableVO
	 *          
	 * @return String
	 */
	public void saveOrUpdateTableVO(TableVO tableVO) {
		try {
			if (tableVO.size() == 0) {
				// 没有记录就无需处理保存
				return;
			}
			String tableName = tableVO.getKey();
			// get dataTable
			if (StringUtils.isNotEmpty(tableName)) {
				Table table = BaseDAO.hmNameToTable.get(tableName);
				DataVO dataVO = this.buildTableVOOnSaveOrUpdate(tableVO, table);
				for (int i = 0; i < dataVO.sizeByTableVO(); i++) {
					TableVO modifyTableVO = dataVO.getTableVO(i);
					if (StringUtils.equals(modifyTableVO.getOperation(), DataObject.OPERATION_INSERT)) {
						this.insert(modifyTableVO);
					} else if (StringUtils.equals(modifyTableVO.getOperation(), DataObject.OPERATION_UPDATE)) {
						this.update(modifyTableVO);
					}
				}

			}
		} catch (Exception e) {
			throw new DAOException("The saveOrUpdateTableVO(TableVO tableVO, String userId, String companyId) throw Exception", e);
		}
	}

	/**
	 * 对传入的TableVO解析,构建两个TableVO(如果有相应的数据),分别用于插入数据库记录和更新数据库记录.</br>
	 * 1. 将表中不存在的栏位从TableVO中删除</br>
	 * 2. 将TableVO中的RowVO分组,放入两个TableVO中.</br>
	 * 3. 给每个CellVO设定DatabaseType</br>
	 * @param tableVO 
	 * @param table 
	 * 
	 */
	private DataVO buildTableVOOnSaveOrUpdate(TableVO tableVO, Table table) {
		DataVO retDataVO = new DataVO();
		TableVO insertTableVO = new TableVO(tableVO.getKey(), TableVO.OPERATION_INSERT);
		TableVO updateTableVO = new TableVO(tableVO.getKey(), TableVO.OPERATION_UPDATE);
		try {
			HashMap<String, String> hmDBNameToDBTypeId = new HashMap<String, String>();
			HashMap<String, Boolean> hmDBNameIsRelation = new HashMap<String, Boolean>();
			for (int i = 0; i < table.size(); i++) {
				Column column = table.get(i);
				hmDBNameToDBTypeId.put(column.getName(), column.getType());
			}
			RowVO[] rowVOs = tableVO.toRowVOArray();
			for (int i = 0; i < rowVOs.length; i++) {
				RowVO rowVO = rowVOs[i];
				CellVO[] cellVOs = rowVO.toCellVOArray();
				for (int j = 0; j < cellVOs.length; j++) {
					CellVO cellVO = cellVOs[j];
					String dbName = cellVO.getKey();
					if (hmDBNameToDBTypeId.containsKey(dbName)) {
						cellVO.setColumnType(hmDBNameToDBTypeId.get(dbName));
					} else {
						rowVO.removeCellVO(cellVO);
					}
				}
				if (rowVO.size() > 0) {
					if (StringUtils.equals(tableVO.getOperation(), TableVO.OPERATION_INSERT)) {
						insertTableVO.addRowVO(rowVO);
						rowVO.setOperation(RowVO.OPERATION_INSERT);
					} else if (StringUtils.equals(tableVO.getOperation(), TableVO.OPERATION_UPDATE)) {
						updateTableVO.addRowVO(rowVO);
						rowVO.setOperation(RowVO.OPERATION_UPDATE);
					} else if (StringUtils.equals(rowVO.getOperation(), RowVO.OPERATION_INSERT)) {
						insertTableVO.addRowVO(rowVO);
					} else if (StringUtils.equals(rowVO.getOperation(), RowVO.OPERATION_UPDATE)) {
						updateTableVO.addRowVO(rowVO);
					} else {
						if (rowVO.containsKey(this.getPrimaryKeyColumnName())) {
							String primaryId = rowVO.getCellVOValue(this.getPrimaryKeyColumnName());
							if (StringUtils.isBlank(primaryId)) {
								// 主键为空的是新增记录
								insertTableVO.addRowVO(rowVO);
								rowVO.setOperation(RowVO.OPERATION_INSERT);
							} else {
								// 主键不为空为更新记录
								updateTableVO.addRowVO(rowVO);
								rowVO.setOperation(RowVO.OPERATION_UPDATE);
							}
						} else {
							// 没有主键为新增记录
							insertTableVO.addRowVO(rowVO);
							rowVO.setOperation(RowVO.OPERATION_INSERT);
						}
					}
				} else {
					tableVO.removeRowVO(rowVO);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("********** Triangle Run time Exception！**********");
		}
		if (insertTableVO.size() > 0) {
			retDataVO.addTableVO(insertTableVO);
		}
		if (updateTableVO.size() > 0) {
			retDataVO.addTableVO(updateTableVO);
		}
		return retDataVO;
	}

	/**
	 * 根据tableName和CondSetBean条件取分页dataTableVO
	 * 这个方法是对无classId表进行分页查询操作的
	 * @param tableName
	 * @param CondSetBean
	 *          CondBean key :  return CellVO key
	 * @param currentPaqeIndex  当前分页页面的index
	 * @param onePageDataNumber   分页页面单页显示数据的数量,必须传
	 * @return DataVO 
	 * 		<ul>dataTableVO  => key : tableName
	 *          <ul>CellVO key : Table.attribute.dbName</ul>/ul>
	 * 		<ul>splitPageInfoFormVO  => key : splitPageInfoFormVO
	 *          <ul>CellVO key : totalDataNumber,totalPageNumber,currentPaqeIndex,onePageDataNumber</ul></ul>
	 */
	public DataVO getTableDataOnSplitPageByTableName(String tableName, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber) {
		return this.getTableDataOnSplitPageByTableName(tableName, condSetBean, currentPaqeIndex, onePageDataNumber, new OrderBy());
	}

	/**
	 * 根据tableName和CondSetBean条件取分页dataTableVO
	 * 这个方法是对无classId表进行分页查询操作的
	 * @param tableName
	 * @param CondSetBean
	 *          CondBean key :  return CellVO key
	 * @param currentPaqeIndex  当前分页页面的index
	 * @param onePageDataNumber   分页页面单页显示数据的数量,必须传
	 * @param sort
	 * @return DataVO 
	 * 		<ul>dataTableVO  => key : tableName
	 *          <ul>CellVO key : Table.attribute.dbName</ul></ul>
	 * 		<ul>splitPageInfoFormVO  => key : splitPageInfoFormVO
	 *          <ul>CellVO key : totalDataNumber,totalPageNumber,currentPaqeIndex,onePageDataNumber</ul></ul>
	 */
	public DataVO getTableDataOnSplitPageByTableName(String tableName, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, Sort sort) {
		return getTableDataOnSplitPageByTableName(tableName, condSetBean, currentPaqeIndex, onePageDataNumber, new OrderBy(sort));
	}

	/**
	 * 根据tableName和CondSetBean条件取分页dataTableVO
	 * 这个方法是对无classId表进行分页查询操作的
	 * @param tableName
	 * @param CondSetBean
	 *          CondBean key :  return CellVO key
	 * @param currentPaqeIndex  当前分页页面的index
	 * @param onePageDataNumber   分页页面单页显示数据的数量,必须传
	 * @param orderBy
	 * @return DataVO 
	 * 		<ul>dataTableVO  => key : tableName
	 *          <ul>CellVO key : Table.attribute.dbName</ul></ul>
	 * 		<ul>splitPageInfoFormVO  => key : splitPageInfoFormVO
	 *          <ul>CellVO key : totalDataNumber,totalPageNumber,currentPaqeIndex,onePageDataNumber</ul></ul>
	 */
	public DataVO getTableDataOnSplitPageByTableName(String tableName, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) {
		DataVO dataVO = new DataVO();
		try {
			if (StringUtils.isNotBlank(tableName)) {
				// get Table
				Table table = new Table(tableName);
				dataVO = this.getTableDataOnSplitPageByTableName(table, condSetBean, currentPaqeIndex, onePageDataNumber, orderBy);
			}
		} catch (Exception e) {
			throw new DAOException(
					"The getTableDataOnSplitPageByTableName(String tableName, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) throw Exception",
					e);
		}
		return dataVO;
	}

	/**
	 * 根据tableName和CondSetBean条件取分页dataTableVO
	 * 这个方法是对无classId表进行分页查询操作的
	 * @param table
	 * @param CondSetBean
	 *          CondBean key :  return CellVO key
	 * @param currentPaqeIndex  当前分页页面的index
	 * @param onePageDataNumber   分页页面单页显示数据的数量,必须传
	 * @param sort
	 * @return DataVO 
	 * 		<ul>dataTableVO  => key : tableName
	 *          <ul>CellVO key : Table.attribute.dbName</ul></ul>
	 * 		<ul>splitPageInfoFormVO  => key : splitPageInfoFormVO
	 *          <ul>CellVO key : totalDataNumber,totalPageNumber,currentPaqeIndex,onePageDataNumber</ul></ul>
	 */
	public DataVO getTableDataOnSplitPageByTableName(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, Sort sort) {
		return getTableDataOnSplitPageByTableName(table, condSetBean, currentPaqeIndex, onePageDataNumber, new OrderBy(sort));
	}

	/**
	 * 根据tableName和CondSetBean条件取分页dataTableVO
	 * 这个方法是对无classId表进行分页查询操作的
	 * @param table
	 * @param CondSetBean
	 *          CondBean key :  return CellVO key
	 * @param currentPaqeIndex  当前分页页面的index
	 * @param onePageDataNumber   分页页面单页显示数据的数量,必须传
	 * @param orderB
	 * @return DataVO 
	 * 		<ul>dataTableVO  => key : tableName
	 *          <ul>CellVO key : Table.attribute.dbName</ul></ul>
	 * 		<ul>splitPageInfoFormVO  => key : splitPageInfoFormVO
	 *          <ul>CellVO key : totalDataNumber,totalPageNumber,currentPaqeIndex,onePageDataNumber</ul></ul>
	 */
	public DataVO getTableDataOnSplitPageByTableName(Table table, CondSetBean condSetBean, int currentPaqeIndex, int onePageDataNumber, OrderBy orderBy) {
		DataVO dataVO = new DataVO();
		String countSQL = null;
		try {
			// get dataTableVO
			TableVO dataTableVO = new TableVO();
			if (onePageDataNumber > 0) {
				if (condSetBean != null && condSetBean.size() > 0) {
					dataTableVO = this.queryForTableVOOnSplitPage(table, condSetBean, currentPaqeIndex, onePageDataNumber, orderBy);
				} else {
					dataTableVO = this.queryForTableVOOnSplitPage(table, currentPaqeIndex, onePageDataNumber, orderBy);
				}

				FormVO splitPageInfoFormVO = new FormVO("splitPageInfoFormVO");
				countSQL = this.buildCountSQL(table, condSetBean);
				if (this.isShowSQL()) {
					log.info(countSQL);
				}
				int totalDataNumber = this.jdbcTemplate.queryForInt(countSQL);
				int totalPageNumber = totalDataNumber / onePageDataNumber;
				if (totalDataNumber % onePageDataNumber > 0) {
					totalPageNumber++;
				}
				splitPageInfoFormVO.addCellVO(new CellVO("totalDataNumber", String.valueOf(totalDataNumber)));
				splitPageInfoFormVO.addCellVO(new CellVO("totalPageNumber", String.valueOf(totalPageNumber)));
				splitPageInfoFormVO.addCellVO(new CellVO("currentPaqeIndex", String.valueOf(currentPaqeIndex)));
				splitPageInfoFormVO.addCellVO(new CellVO("onePageDataNumber", String.valueOf(onePageDataNumber)));

				dataVO.addFormVO(splitPageInfoFormVO);
			}
			dataVO.addTableVO(dataTableVO);
		} catch (Exception e) {
			throw new DAOException("BaseDAO getTableDataOnSplitPageByTableName Exception \r\n      countSQL : " + countSQL, e);
		}
		return dataVO;
	}

	/**
	 * 执行executeTableSQL语句
	 * @param executeSQL : 可以在对应数据库服务器执行的语句
	 * @return String
	 */
	public String executeSQL(String executeSQL) {
		if (this.isShowSQL()) {
			log.info(executeSQL);
		}
		String errorMsg = "";
		try {
			this.jdbcTemplate.execute(executeSQL);
		} catch (Exception e) {
			throw new DAOException("BaseDAO executeSQL Exception \r\n      executeString : " + executeSQL, e);
		}
		return errorMsg;
	}

	public Connection getConnection() {
		Connection conn;
		try {
			conn = this.jdbcTemplate.getDataSource().getConnection();
		} catch (Exception e) {
			throw new DAOException("The getConnection() throw Exception", e);
		}
		return conn;
	}
}
