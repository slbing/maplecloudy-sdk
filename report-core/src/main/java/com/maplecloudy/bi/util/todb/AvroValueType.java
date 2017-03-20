package com.maplecloudy.bi.util.todb;

import java.util.Map;

public interface AvroValueType {
	/**
	 * @return get a row data, Map of columnName and columnValue</br>
	 * columnName 必须采用小写，否则不能正常导入数据
	 */
	public Map<String,Object> getRow(Map<String,Object> reuse);
	
}
