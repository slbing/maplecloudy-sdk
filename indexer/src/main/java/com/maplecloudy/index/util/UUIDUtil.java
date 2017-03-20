package com.maplecloudy.index.util;

import java.util.UUID;

/**
 * UUID工具类
 * 
 * @author cc
 * @date 2011-10-24
 * @version 1.0
 */
public class UUIDUtil {
	
	/**
	 * 使用{@link UUID#randomUUID()}生成一个新的UUID<br>
	 * 并去除其中的-符号
	 * @return 一个UUID的字符串
	 */
	public static String uuid(){
		UUID uuid = UUID.randomUUID();
		String uuidStr = uuid.toString();
		
		return uuidStr.replace("-", "").toUpperCase();
		
	}
}
