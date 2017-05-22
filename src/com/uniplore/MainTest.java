package com.uniplore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.ZipfDistribution;

import redis.clients.jedis.Jedis;

public class MainTest {

	private static String driver="org.postgresql.Driver";
	private static String url ="jdbc:postgresql://192.168.100.125:5432/tian";
	private static String user = "lys";
	private static String pwd = "gpadmin";
	private static int thr=10;
	public MainTest() {
		// TODO Auto-generated constructor stub
	}
	
	
	private static void WriteToDisk(String key,List<String> rs,String path,Map<String,String> sqlmap) throws IOException{
		// 如果使用SSD/Disk缓存
		
		File file=new File(path+sqlmap.get(key));
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		for(String d:rs){			
				bw.write(d);
				bw.newLine();
				bw.flush();
			}
			bw.close();
			fw.close();
			
	}	
	/*
	 * 测试单线程下查询的响应时间
	 */
	public static void TestTpch(){
		String path="d:/RedisResultSet/";
		WorkloadGenerate rt=new WorkloadGenerate();
		List<String> ls=rt.readSql();
		Map<String,String> sqlmap=rt.getSqlMap();
		for(int i=0;i<ls.size();i++)
		try {
			Class.forName(driver);		
		    Connection conn = DriverManager.getConnection(url, user, pwd);
		    Statement st= conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
		    long startTimex = System.currentTimeMillis();
		    ResultSet rs = st.executeQuery(ls.get(i));
		    List<String> rsList=new ArrayList<String>();
		    ResultSetMetaData rsmd=rs.getMetaData();
			int colcount=rsmd.getColumnCount();
			while(rs.next()){
				String td="";
				for(int j=1;j<=colcount;j++){
					td=td+rs.getString(j);	
				}
				rsList.add(td);
			}			    
		    WriteToDisk(ls.get(i),rsList,path,sqlmap);
		    long endTimex = System.currentTimeMillis();
		    conn.close();
		    System.out.println(i+":Time:"+(endTimex-startTimex));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * 测试Zip
	 */
	public static void testZip(){
		int count[]=new int[15];
		for(int i=1;i<15;i++){		
			count[i]=0;
			}
		ZipfDistribution zd = new ZipfDistribution(14,1);
		for(int i=0;i<100;i++){		
		count[(int)zd.sample()]++;
		}
		for(int i=0;i<15;i++){		
			System.out.println(i+":"+count[i]);
			}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Start");
		CacheManagement rt=new CacheManagement();
		rt.initMetaData();
		try{
			List<Thread> ct=new ArrayList<Thread>();
	        for(int i=0;i<thr;i++){
	        	ct.add(new Thread(new RequestThread(i,rt)));
	    	    ct.get(i).start();
	    	    }
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		TestTpch();
		//test();
	}

}
