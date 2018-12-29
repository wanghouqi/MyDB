package hq.mydb.test;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import hq.mydb.dao.BaseDAO;
import hq.mydb.data.TableVO;

@RunWith(value = SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/spring-db.xml" })
public class TestBaseDAO {
	@Resource //自动注入,默认按名称
	private BaseDAO baseDAO;

	@Test //标明是测试方法
	@Transactional //标明此方法需使用事务
	@Rollback(false) //标明使用完此方法后事务不回滚,true时为回滚
	public void testInitDataBaseCache() {
		baseDAO.initDataBaseCache();
		//		assertTrue(list.size() > 0);
	}

}