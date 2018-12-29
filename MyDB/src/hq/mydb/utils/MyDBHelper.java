package hq.mydb.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;

import hq.mydb.exception.DAOException;

public class MyDBHelper {
	private static String primaryKeyColumnName = "";// 主键栏位名,通过BaseDAO初始化.
	
	public static String getPrimaryKeyColumnName() {
		return primaryKeyColumnName;
	}

	public static void setPrimaryKeyColumnName(String primaryKeyColumnName) {
		MyDBHelper.primaryKeyColumnName = primaryKeyColumnName;
	}

	/**
	 * 将Exception的printStackTrace()转成字符窜.
	 * 
	 * @param t
	 * @return String
	 */
	public static String getTrace(Throwable t) {
		String traceMsg = "N/A";
		if (t != null) {
			StringWriter stringWriter = new StringWriter();
			PrintWriter writer = new PrintWriter(stringWriter);
			t.printStackTrace(writer);
			StringBuffer buffer = stringWriter.getBuffer();
			traceMsg = buffer.toString();
		}
		return traceMsg;
	}

	/**
	 * 密码加密
	 * 
	 * @throws NoSuchAlgorithmException
	 */
	public static String encryptPassword(String data) {
		String password = "";
		try {
			if (StringUtils.isEmpty(data)) {
				return data;
			}
			byte[] srcBytes = data.getBytes();
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.update(Base64.encodeBase64(srcBytes));
			password = new String(new Hex().encode(md5.digest()));
		} catch (Exception e) {
			// TODO: handle exception
		}
		return password;
	}

	/**
	 * 首字母转小写
	 * 
	 * @param s
	 * @return
	 */
	public static String toLowerCaseFirstOne(String s) {
		if (Character.isLowerCase(s.charAt(0)))
			return s;
		else
			return (new StringBuilder()).append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
	}

	/**
	 * 首字母转大写
	 * 
	 * @param s
	 * @return
	 */
	public static String toUpperCaseFirstOne(String s) {
		if (Character.isUpperCase(s.charAt(0)))
			return s;
		else
			return (new StringBuilder()).append(Character.toUpperCase(s.charAt(0))).append(s.substring(1)).toString();
	}

	/**
	 * 将传入的Number进行格式化
	 * 
	 * @param number
	 * @param format
	 * @return String
	 */
	public static String formatNumber(double number, String format) {
		String number_format = "";
		try {
			DecimalFormat decimalFormat = new DecimalFormat("0.0000");
			if (StringUtils.isNotEmpty(format)) {
				decimalFormat = new DecimalFormat(format);
			}
			number_format = decimalFormat.format(number);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 继续转换得到秒数的long型
		return number_format;
	}

	/**
	 * 将传入的Number进行格式化
	 * 
	 * @param number
	 * @param format
	 * @return String
	 */
	public static String formatNumber(long number, String format) {
		String number_format = "";
		try {
			DecimalFormat decimalFormat = new DecimalFormat("0.0000");
			if (StringUtils.isNotEmpty(format)) {
				decimalFormat = new DecimalFormat(format);
			}
			number_format = decimalFormat.format(number);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 继续转换得到秒数的long型
		return number_format;
	}

	/**
	 * 使用默认长度截取字符窜
	 * 
	 * @param showValue
	 * @return String
	 */
	public static String splitShowValue(String showValue) {
		return splitShowValue(showValue, -1);
	}

	/**
	 * 根据传入的长度截取字符窜
	 * 
	 * @param showValue
	 * @param length
	 * @return String
	 */
	public static String splitShowValue(String showValue, int length) {
		if (length < 0) {
			length = 10;
		}
		String returnString = showValue;
		if (StringUtils.isBlank(showValue)) {
			returnString = "";
		} else {
			returnString = substring(showValue, length);
			if (returnString.length() < showValue.length()) {
				returnString += "...";
			}
		}
		return returnString;
	}

	/**
	 * 截取一段字符的长度,不区分中英文,如果数字不正好，则少取一个字符位
	 *
	 * @author patriotlml
	 * @param origin
	 *            原始字符串
	 * @param len,
	 *            截取长度(一个汉字长度按2算的)
	 * @return String, 返回的字符串
	 */
	public static String substring(String origin, int len) {
		if (origin == null || origin.equals("") || len < 1) {
			return "";
		}
		try {
			if (len > length(origin)) {
				return origin;
			}
			len = getSplitLength(origin, len);
			origin = origin.substring(0, len);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return origin;
	}

	/**
	 * 得到一个用于拆分混合字符的长度
	 *
	 * @param s
	 *            ,需要得到长度的字符串
	 * @param len
	 *            英文需要的长度
	 * @return int, 得到的字符串长度
	 */
	public static int getSplitLength(String s, int len) {
		if (s == null) {
			return 0;
		}
		int ret = 0;
		char[] c = s.toCharArray();
		// System.out.println("c.length=" + c.length);
		for (int i = 0; i < c.length; i++) {
			len--;
			if (!isLetter(c[i])) {
				len--;
			}
			if (len < 0) {
				continue;
			}
			ret++;
		}
		return ret;
	}

	/**
	 * 得到一个字符串的长度,显示的长度,一个汉字或日韩文长度为2,英文字符长度为1
	 *
	 * @param s
	 *            需要得到长度的字符串
	 * @return int 得到的字符串长度
	 */
	public static int length(String s) {
		if (s == null) {
			return 0;
		}
		char[] c = s.toCharArray();
		int len = 0;
		for (int i = 0; i < c.length; i++) {
			len++;
			if (!isLetter(c[i])) {
				len++;
			}
		}
		return len;
	}

	/**
	 * 判断一个字符是Ascill字符还是其它字符（如汉，日，韩文字符）
	 *
	 * @param c
	 *            需要判断的字符
	 * @return boolean 返回true,Ascill字符
	 */
	public static boolean isLetter(char c) {
		int k = 0x80;
		return c / k == 0 ? true : false;
	}

	/*
	 * 判断是否为自然数, 123, -123, 12.3, -12.3
	 * 
	 * @param str 传入的字符串
	 * 
	 * @return 是整数返回true,否则返回false
	 */
	public static boolean isNumber(String str) {
		boolean isOk = false;
		if (str != null && !str.equals("")) {
			Pattern pattern = Pattern.compile("^([+-]?)\\d*\\.?\\d+$");
			isOk = pattern.matcher(str).matches();
		}
		return isOk;
	}

	/**
	 * 提供精确的加法运算。
	 * @param v1  被加数
	 * @param v2  加数
	 * @return String 两个参数的和
	 */
	public static String doubleAdd(double v1, double v2) {
		return doubleAdd(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供精确的加法运算。
	 * @param v1  被加数
	 * @param v2  加数
	 * @return String 两个参数的和
	 */
	public static String doubleAdd(String v1, String v2) {
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new RuntimeException("**** doubleAdd(String v1, String v2) Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			double d = b1.add(b2).doubleValue();
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * 提供精确的减法运算。
	 * @param v1  被减数
	 * @param v2  减数
	 * @return String 两个参数的差
	 */
	public static String doubleSub(double v1, double v2) {
		return doubleSub(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供精确的减法运算。
	 * @param v1  被减数
	 * @param v2  减数
	 * @return String 两个参数的差
	 */
	public static String doubleSub(String v1, String v2) {
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new RuntimeException("**** doubleSub(String v1, String v2) Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			double d = b1.subtract(b2).doubleValue();
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * 提供精确的减法运算,结果小于0时则显示返回0.
	 * @param v1  被减数
	 * @param v2  减数
	 * @return String 两个参数的差
	 */
	public static String doubleSubPositiveNumber(double v1, double v2) {
		return doubleSubPositiveNumber(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供精确的减法运算,结果小于0时则显示返回0.
	 * @param v1  被减数
	 * @param v2  减数
	 * @return String 两个参数的差
	 */
	public static String doubleSubPositiveNumber(String v1, String v2) {
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new RuntimeException("**** doubleSubPositiveNumber(String v1, String v2) Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			double d = b1.subtract(b2).doubleValue();
			if (d < 0) {
				d = 0;
			}
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * 提供精确的乘法运算。
	 * @param v1  被乘数
	 * @param v2  乘数
	 * @return String 两个参数的积
	 */
	public static String doubleMul(double v1, double v2) {
		return doubleMul(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供精确的乘法运算。
	 * @param v1  被乘数
	 * @param v2  乘数
	 * @return String 两个参数的积
	 */
	public static String doubleMul(String v1, String v2) {
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new RuntimeException("**** doubleMul(String v1, String v2) Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			double d = b1.multiply(b2).doubleValue();
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * 提供（相对）精确的除法运算，当发生除不尽的情况时，精确到 小数点以后10位，以后的数字四舍五入。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String 两个参数的商
	 */
	public static String doubleDiv(double v1, double v2) {
		return doubleDiv(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供（相对）精确的除法运算，当发生除不尽的情况时，精确到 小数点以后4位，以后的数字四舍五入。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String 两个参数的商
	 */
	public static String doubleDiv(String v1, String v2) {
		return doubleDiv(v1, v2, 4);
	}

	/**
	 * 
	 * @param v1  被除数
	 * @param v2  除数
	 * @param scale 除法运算精度
	 * @return String 两个参数的商
	 */
	public static String doubleDiv(double v1, double v2, int scale) {
		return doubleDiv(String.valueOf(v1), String.valueOf(v2), scale);
	}

	/**
	 * 提供（相对）精确的除法运算。当发生除不尽的情况时，由scale参数指
	 * 定精度，以后的数字四舍五入。
	 * @param v1
	 * @param v2
	 * @param scale
	 * @return String
	 */
	public static String doubleDiv(String v1, String v2, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException("The scale must be a positive integer or zero");
		}
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new RuntimeException("**** doubleDiv(String v1, String v2) Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			double d = b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * 提供精确的小数位四舍五入处理。
	 * @param v 需要四舍五入的数字
	 * @param scale 小数点后保留几位
	 * @return String 四舍五入后的结果
	 */
	public static String doubleRound(double v, int scale) {
		return doubleRound(String.valueOf(v), scale);
	}

	/**
	 * 提供精确的小数位四舍五入处理。
	 * @param v 需要四舍五入的数字
	 * @param scale 小数点后保留几位
	 * @return String 四舍五入后的结果
	 */
	public static String doubleRound(String v, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException("The scale must be a positive integer or zero");
		}
		if (StringUtils.isBlank(v)) {
			throw new RuntimeException("**** doubleRound(String v, int scale) Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b = new BigDecimal(v);
			BigDecimal one = new BigDecimal("1");
			double d = b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * 提供取整的除法运算，只保留商的整数位。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String 两个参数的商
	 */
	public static String doubleDivToInt(double v1, double v2) {
		return doubleDivToInt(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供取整的除法运算，只保留商的整数位。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String 两个参数的商
	 */
	public static String doubleDivToInt(String v1, String v2) {
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new DAOException("**** doubleMul(" + v1 + ", " + v2 + ") Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			double d = b1.divideToIntegralValue(b2).doubleValue();
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new DAOException("doubleDivToInt(" + v1 + ", String " + v2 + ")", ex);
		}
	}

	/**
	 * 提供取余数运算。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String 两个参数的余数
	 */
	public static String doubleRemainder(double v1, double v2) {
		return doubleRemainder(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供取余数运算。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String 两个参数的余数
	 */
	public static String doubleRemainder(String v1, String v2) {
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new RuntimeException("**** doubleRemainder(String v1, String v2) Exception, parameter cannot be blank！****");
		}
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			double d = b1.remainder(b2).doubleValue();
			return String.valueOf(d);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	/**
	 * 提供取余数运算。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String[] 两个参数的余数
	 */
	public static String[] doubleDivideAndRemainder(double v1, double v2) {
		return doubleDivideAndRemainder(String.valueOf(v1), String.valueOf(v2));
	}

	/**
	 * 提供取余数运算。
	 * @param v1  被除数
	 * @param v2  除数
	 * @return String[] 两个参数的余数
	 */
	public static String[] doubleDivideAndRemainder(String v1, String v2) {
		if (StringUtils.isBlank(v1) || StringUtils.isBlank(v2)) {
			throw new RuntimeException("**** doubleDivideAndRemainder(String v1, String v2) Exception, parameter cannot be blank！****");
		}
		String[] st = new String[2];
		try {
			BigDecimal b1 = new BigDecimal(v1);
			BigDecimal b2 = new BigDecimal(v2);
			BigDecimal[] bt = b1.divideAndRemainder(b2);
			double d1 = bt[0].doubleValue();
			double d2 = bt[1].doubleValue();
			st[0] = String.valueOf(d1);
			st[1] = String.valueOf(d2);
		} catch (Exception ex) {
			st[0] = "";
			st[1] = "";
			throw new RuntimeException(ex.getMessage());
		}
		return st;
	}
}
