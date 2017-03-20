package com.maplecloudy.maple.util;

import java.io.File;
import java.io.InputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

/**
 * API接口的常用的工具类
 * 
 * @author Jin xueliang 2011/12/26
 */
public class ApiUtil {
  
  /**
   * 保留浮点类型数据四位小数
   * 
   * @param d
   *          :要处理的浮点类型的参数
   * @return 格式化之后的浮点小数
   */
  public static double formatDouble(double d) {
    NumberFormat format = DecimalFormat.getInstance();
    format.setMaximumFractionDigits(4);
    format.setMinimumFractionDigits(4);
    return Double.valueOf(format.format(d));
  }
  
  /**
   * 验证字符串是否为null，为空，为空格
   * 
   * @param s
   *          :要验证的字符串
   * @return true/false 不为NULL|空|空格返回true,否则返货false
   */
  public static boolean verifyStr(String s) {
    return null != s && !"".equals(s.trim());
  }
  
  /**
   * 验证输入的字符是不是正整数
   * 
   * @param num
   *          :要验证的字符
   * @return true/false 是数字/不是数字
   */
  public static boolean isFigure(String num) {
    return Pattern.compile("^[1-9]{1}[0-9]*$").matcher(num).matches();
  }
  
  /**
   * 判断是不是数字，如果不是赋值给指定的值
   */
  public static int intCharsProcess(String numChars, int defaultNum) {
    int bN = defaultNum;
    try {
      if (isAllFigure(numChars)) {
        bN = Integer.parseInt(numChars);
      }
    } catch (Exception e) {
      bN = defaultNum;
      e.printStackTrace();
    }
    
    return bN;
  }
  
  /**
   * 验证输入的字符是不是整数(正负数，0)
   */
  public static boolean isAllFigure(String num) {
    return Pattern.compile("^-?\\d+$").matcher(num).matches();
  }
  
  /**
   * 验证输入的字符串是不是数字(整数、小数等)
   * 
   * @param decimal
   *          要验证的串
   * @return true/false 是数字/不是数字
   */
  public static boolean isDecimal(String decimal) {
    return Pattern.compile("[0-9]+(.[0-9]+)?").matcher(decimal).matches();
  }
  
  /**
   * 判断IP是不是合法的IP
   * 
   * @param ip
   *          要判断的ip true/false 合法/违法
   */
  public static boolean isLegalIP(String ip) {
    return Pattern
        .compile(
            "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}")
        .matcher(ip).matches();
  }
  
  /**
   * 得到随机生成的UUID
   * 
   * @return 返回UUID的字符串
   */
  public static String getUUID() {
    return UUID.randomUUID().toString().replace("-", "");
  }
  
  /**
   * 模拟生成udid
   */
  public static String simulateUdid() {
    return getUUID().substring(0, 16) + "#" + getUUID().substring(16, 32);
  }
  
  /**
   * 将字符串简单处理，如果是NULL，就返回NULL，否则返回剔去空格的字符串
   * 
   * @param s
   *          :要处理的字符串
   * @return 处理后串
   */
  public static String resolveStr(String s) {
    return null == s ? null : s.trim();
  }
  
  /**
   * 将请求参数值字符串简单处理，如果是NULL，就返回""，否则返回剔去空格的字符串
   * 
   * @param s
   *          :参数名称
   * @return 处理后串
   */
  public static String resolveStr(String s, HttpServletRequest request) {
    return null == request.getParameter(s) ? "" : request.getParameter(s)
        .trim();
  }
  
  /**
   * 得到请求的IP
   * 
   * @param request
   * @return 得到请求IP的串
   */
  public static String getIP(HttpServletRequest request) {
    String ip = request.getHeader("x-forwarded-for");
    if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
      ip = request.getHeader("Proxy-Client-IP");
    }
    if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
      ip = request.getHeader("WL-Proxy-Client-IP");
    }
    if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
      ip = request.getRemoteAddr();
    }
    return ip;
  }
  
  /**
   * 以utf-8的编码转码
   * 
   * @param s
   *          要转码的字符
   * @return
   */
  public static String encodeChars(String s) {
    String encodeChars = "";
    try {
      if (verifyStr(s)) {
        encodeChars = URLEncoder.encode(s, "utf-8");
      }
    } catch (Exception e) {
      encodeChars = "";
      e.printStackTrace();
    }
    return encodeChars;
  }
  
  /**
   * 以utf-8的编码解码
   * 
   * @param s
   *          要解码的字符
   * @return
   */
  public static String decodeChars(String s) {
    String decodeChars = "";
    try {
      if (verifyStr(s)) {
        decodeChars = URLDecoder.decode(s, "utf-8");
      }
    } catch (Exception e) {
      decodeChars = "";
      e.printStackTrace();
    }
    
    return decodeChars;
  }
  
  // /**
  // * 读取无需校验的点击的密匙的key值放在字符容器中
  // * @return
  // */
  // public static ArrayList<String> get3DKeys() {
  // ArrayList<String> keys = null;
  // try {
  // File file = new
  // File(Thread.currentThread().getContextClassLoader().getResource("/").getPath()
  // + "3dkeys.xml");
  // SAXReader reader = new SAXReader();
  // Document doc = reader.read(file);
  // Element root = doc.getRootElement();
  // Element keysElement = root.element("keys");
  //
  // List<Element> list = keysElement.elements();
  // int size = list.size();
  // keys = new ArrayList<String>();
  // for (int index = 0; index < size; index ++) {
  // keys.add(list.get(index).getText().trim());
  // }
  // } catch (Exception e) {
  // keys = null;
  // e.printStackTrace();
  // }
  // return keys;
  // }
  
  /**
   * 摩谷3D点击无需校验的广告位的配置文件的最后的更改的时间
   * 
   * @return long 更改的时间
   * @author Jin xueliang
   */
  public static long getFileLastModifiedTime(String fileName) {
    File propertiesFile = new File(Thread.currentThread()
        .getContextClassLoader().getResource("/").getPath()
        + "/" + fileName);
    long l = 0l;
    try {
      if (propertiesFile.exists()) {
        l = propertiesFile.lastModified();
      }
    } catch (Exception e) {
      l = 0l;
    }
    
    return l;
  }
  
  /**
   * 获取文件的最后更新的时间
   */
  public static long getFileLastModifiedTime(File file) {
    long l = 0l;
    try {
      l = file.lastModified();
    } catch (Exception e) {
      l = 0l;
      e.printStackTrace();
    }
    return l;
  }
  
  /**
   * 根据文件名称，获取src目录下配置文件
   * 
   * @param fileName
   * @return Properties对象
   * @author Jin xueliang
   */
  public static Properties catchProperties(String fileName) {
    
    // InputStream inputStream =
    // Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);//有类加载容器的缓存
    InputStream inputStream = null;
    Properties properties = new Properties();
    try {
      inputStream = Thread.currentThread().getContextClassLoader()
          .getResource(fileName).openStream();
      properties.load(inputStream);
      return properties;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
      try {
        if (null != inputStream) inputStream.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * 取两个正整数的差值的绝对值
   * 
   * @param a
   * @param b
   * @return 差值的绝对值
   * @author Jin xueliang
   */
  public static int getAbsoluteValue(int a, int b) {
    return a - b > 0 ? a - b : b - a;
  }
  
  /**
   * 取两个实数的差值的绝对值
   * 
   * @param a
   * @param b
   * @return 差值的绝对值
   * @author Jin xueliang
   */
  public static double getAbsoluteValue(double a, double b) {
    return a - b > 0 ? a - b : b - a;
  }
  
  /**
   * 取两个正整数的差值
   * 
   * @param a
   * @param b
   * @return 差值
   * @author Jin xueliang
   */
  public static int getSpaceValue(int a, int b) {
    return a - b;
  }
  
  /**
   * 数字字符，转换返回，若数字字符不合法，返回指定的数字
   * 
   * @param s
   * @param i
   * @return
   */
  public static int getUW(String s, int i) {
    
    if (verifyStr(s) && isAllFigure(s.trim())) {
      return Integer.parseInt(s.trim());
    }
    
    return i;
  }
  
  /**
   * 剔除字符串的所有的空格
   * 
   * @param s
   * @return 剔除所有空格之后的字符串
   */
  public static String noTrim(String s) {
    return s.replaceAll(" ", "");
  }
}
