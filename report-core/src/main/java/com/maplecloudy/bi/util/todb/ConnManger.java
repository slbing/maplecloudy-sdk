package com.maplecloudy.bi.util.todb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.maplecloudy.bi.mapreduce.output.db.DBConfiguration;

/**
 * @author liwang4
 * manage connections for ReportToDbAlg. 
 * all connections are managed by this class, so you should not
 * new or close a connection in somewhere else.
 *
 */
public class ConnManger {
	private static final String P_FILE_NAME="report.db.properties";
	private Properties conn_properties = new Properties();
	private static final String DB_DEF = "db.";
	private static final String ALG_DEF = "alg.";
	private static final String ALG_DEF_DEFAULT = "other";
	private static final String IP = ".ip";
	private static final String PORT = ".port";
	private static final String DB = ".db";
	private static final String USER = ".user";
	private static final String PWD = ".pwd";

	private Map<String, Connection> algConnMap = new HashMap<String, Connection>();

	public ConnManger() throws IOException {
		super();
		readProperties();
	}

	private static ConnManger cm = null;

	public static ConnManger get() throws IOException{
		if (null == cm){
			cm = new ConnManger();
		}

		return cm;
	}

	protected void readProperties() throws IOException{

		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(P_FILE_NAME);    
		conn_properties.load(inputStream);

	}

	protected Connection  getConn(String alg) throws ClassNotFoundException, SQLException{
		Connection ret = null;
		ret = algConnMap.get(alg);

		if (null == ret){
			String db_conn_name = conn_properties.getProperty(ALG_DEF+alg);
			if (null != db_conn_name){
				String ip = conn_properties.getProperty(DB_DEF+db_conn_name+IP);
				String port = conn_properties.getProperty(DB_DEF+db_conn_name+PORT);
				String db = conn_properties.getProperty(DB_DEF+db_conn_name+DB);
				String user = conn_properties.getProperty(DB_DEF+db_conn_name+USER);
				String pwd = conn_properties.getProperty(DB_DEF+db_conn_name+PWD);

				System.out.println("alg="+alg);
				System.out.println("db_conn_name="+db_conn_name);
//				System.out.println("ip="+ip);
//				System.out.println("port="+port);
//				System.out.println("db="+db);
//				System.out.println("user="+user);
//				System.out.println("pwd="+pwd);

				if (null!=ip && null!=db && null!=user && null!=pwd){
					if (null == port){
						port = "3306";
					}
					Configuration conf = new Configuration();
					conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver");
					conf.set(DBConfiguration.USERNAME_PROPERTY,user);
					conf.set(DBConfiguration.PASSWORD_PROPERTY,pwd);
					conf.set(DBConfiguration.URL_PROPERTY,"jdbc:mysql://"+ip+":"+port+"/"+db
							+"?characterEncoding=utf-8&rewriteBatchedStatements=true&zeroDateTimeBehavior=convertToNull&noAccessToProcedureBodies=true&Pooling=false");

					DBConfiguration dbConf = new DBConfiguration(conf);
					ret = dbConf.getConnection();
					ret.setAutoCommit(false);
					algConnMap.put(alg, ret);
				}
			}
		}
		
		return ret;
	}

	public Connection getAlgConn(String alg) throws ClassNotFoundException, SQLException{
		System.out.println("get conn info for "+alg);
		Connection ret = getConn(alg);
		if (null == ret){
			ret = getConn(ALG_DEF_DEFAULT);
		}
		return ret;
	}
	
	public Connection getAlgConn(Class<?> alg) throws ClassNotFoundException, SQLException{
		String strAlg = alg.getName();
		return getAlgConn(strAlg);
	}
	
	public void closeConns(){
		for (Connection conn : algConnMap.values()){
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		algConnMap.clear();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException{
		ConnManger.get().getAlgConn("BusinessSource");
		ConnManger.get().getAlgConn(BusinessSource.class);
		ConnManger.get().getAlgConn("BusinessSourc");
	}
	
	public class BusinessSource{}

}
