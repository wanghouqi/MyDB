package hq.mydb.test;

import org.junit.Test;

import hq.mydb.utils.MyDBHelper;

public class TestHelper {
	@Test
	public void testGetLastDayOfMonth() {
		System.out.println("yearMonth=" + MyDBHelper.formatDate(MyDBHelper.getLastDayOfMonth("201811", "yyyyMM"), "yyyy-MM-dd HH:mm:ss SSS"));
	}
}
