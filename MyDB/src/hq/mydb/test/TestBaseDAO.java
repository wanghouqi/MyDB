package hq.mydb.test;

import java.util.HashSet;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import hq.mydb.condition.CondSetBean;
import hq.mydb.dao.BaseDAO;
import hq.mydb.data.CellVO;
import hq.mydb.data.FormVO;
import hq.mydb.data.RowVO;
import hq.mydb.data.TableVO;
import hq.mydb.db.Table;

@RunWith(value = SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/spring-db.xml" })
public class TestBaseDAO {
	@Resource //自动注入,默认按名称
	private BaseDAO baseDAO;

	@Test //标明是测试方法
	@Transactional //标明此方法需使用事务
	@Rollback(false) //标明使用完此方法后事务不回滚,true时为回滚
	public void testInitDataBaseCache() {
		try {
			baseDAO.initDataBaseCache();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testQuerFormVO() {
		try {
			FormVO fvo1 = this.baseDAO.queryForFormVOById("tl_boolean", "yes");
			System.out.println("/******* fvo1");
			System.out.println(fvo1);
			System.out.println("*************/");
			Table table = new Table("tl_boolean");
			table.addColumn("CN_NAME");
			FormVO fvo2 = this.baseDAO.queryForFormVOById(table, "yes");
			System.out.println("/******* fvo2");
			System.out.println(fvo2);
			System.out.println("*************/");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testQuerTableVO() {
		try {
			TableVO tvo1 = this.baseDAO.queryForTableVO("tl_boolean");
			System.out.println("/******* tvo1");
			System.out.println(tvo1);
			System.out.println("*************/");
			Table table = new Table("tl_boolean");
			table.addColumn("CN_NAME");
			TableVO tvo2 = this.baseDAO.queryForTableVOByColumnEqual(table, "CN_ID", "yes");
			System.out.println("/******* tvo2");
			System.out.println(tvo2);
			System.out.println("*************/");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSaveFormVO() {
		try {
			// 插入
			FormVO fvo_insert1 = new FormVO("tl_boolean");
			fvo_insert1.addCellVO(new CellVO("CN_NAME", "fvo_insert1"));
			this.baseDAO.saveOrUpdateFormVO(fvo_insert1);
			System.out.println(fvo_insert1);
			// 插入
			FormVO fvo_insert2 = new FormVO("tl_boolean", FormVO.OPERATION_INSERT);
			fvo_insert2.addCellVO(new CellVO("CN_ID", "fvo_insert2"));
			fvo_insert2.addCellVO(new CellVO("CN_NAME", "fvo_insert2"));
			this.baseDAO.saveOrUpdateFormVO(fvo_insert2);
			// 更新
			FormVO fvo_update1 = new FormVO("tl_boolean");
			fvo_update1.addCellVO(new CellVO("CN_ID", "insert2"));
			fvo_update1.addCellVO(new CellVO("CN_NAME", "fvo_update1"));
			this.baseDAO.saveOrUpdateFormVO(fvo_update1);
			// 更新
			FormVO fvo_update2 = new FormVO("tl_boolean", FormVO.OPERATION_UPDATE);
			fvo_update2.addCellVO(new CellVO("CN_ID", "insert2"));
			fvo_update2.addCellVO(new CellVO("CN_NAME", "fvo_update2"));
			this.baseDAO.saveOrUpdateFormVO(fvo_update2);
			System.out.println("testSaveFormVO() finish!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSaveTableVO() {
		try {
			// 插入
			TableVO tvo_insert1 = new TableVO("tl_boolean");
			RowVO rvo1 = new RowVO();
			tvo_insert1.addRowVO(rvo1);
			rvo1.addCellVO(new CellVO("CN_NAME", "tvo_i1"));
			this.baseDAO.saveOrUpdateTableVO(tvo_insert1);
			System.out.println(tvo_insert1);
			// 插入
			TableVO tvo_insert2 = new TableVO("tl_boolean", TableVO.OPERATION_INSERT);
			RowVO rvo2 = new RowVO();
			tvo_insert2.addRowVO(rvo2);
			rvo2.addCellVO(new CellVO("CN_ID", "tvo_insert2"));
			rvo2.addCellVO(new CellVO("CN_NAME", "tvo_i2"));
			this.baseDAO.saveOrUpdateTableVO(tvo_insert2);
			// 更新
			TableVO tvo_update1 = new TableVO("tl_boolean");
			RowVO rvo3 = new RowVO();
			tvo_update1.addRowVO(rvo3);
			rvo3.addCellVO(new CellVO("CN_ID", "tvo_insert2"));
			rvo3.addCellVO(new CellVO("CN_NAME", "tvo_u1"));
			this.baseDAO.saveOrUpdateTableVO(tvo_update1);
			// 更新
			TableVO tvo_update2 = new TableVO("tl_boolean", TableVO.OPERATION_UPDATE);
			RowVO rvo4 = new RowVO();
			tvo_update2.addRowVO(rvo4);
			rvo4.addCellVO(new CellVO("CN_ID", "tvo_insert2"));
			rvo4.addCellVO(new CellVO("CN_NAME", "tvo_u2"));
			this.baseDAO.saveOrUpdateTableVO(tvo_update2);
			System.out.println("testSaveTableVO() finish!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDelete() {
		try {
			TableVO tvo = new TableVO("tl_boolean", TableVO.OPERATION_INSERT);
			tvo.addRowVO(new RowVO().addCellVO(new CellVO("CN_ID", "DEL1")).addCellVO(new CellVO("CN_NAME", "DEL1")));
			tvo.addRowVO(new RowVO().addCellVO(new CellVO("CN_ID", "DEL2")).addCellVO(new CellVO("CN_NAME", "DEL2")));
			tvo.addRowVO(new RowVO().addCellVO(new CellVO("CN_ID", "DEL3")).addCellVO(new CellVO("CN_NAME", "DEL3")));
			tvo.addRowVO(new RowVO().addCellVO(new CellVO("CN_ID", "DEL4")).addCellVO(new CellVO("CN_NAME", "DEL4")));
			this.baseDAO.saveOrUpdateTableVO(tvo);

			this.baseDAO.delete("DELETE FROM tl_boolean WHERE CN_ID = 'DEL1'");
			this.baseDAO.delete("tl_boolean", new CondSetBean().addCondBean_equal("CN_ID", "DEL2"));
			this.baseDAO.deleteById("tl_boolean", "DEL3");
			HashSet<String> hsDel = new HashSet<String>();
			hsDel.add("DEL4");
			this.baseDAO.deleteInId("tl_boolean", hsDel);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}