package com.maplecloudy.bi.util.todb;

import java.sql.SQLException;

public interface SetSqlPara {
	void setValue(int paraIndex, Object value) throws SQLException;

}
