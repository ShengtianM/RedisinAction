package com.uniplore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class WorkloadGenerate {
	
	/*
	 * SQL语句目录，根据负载不同而变化
	 * tpchsqls tpcsqlb tpcsqlc tpcsqld
	 * SSDWorkload
	 */
	private String sqlPath="D:/tpcsqld/";
	/*
	 * 选择请求的分布方式
	 * 1 随机分布
	 * 2 Zipf分布
	 */
	private int dis=2;
	
	/* 请求序列长度 */
	public int sqlNum=100;	

	private Map<String,String> sqlmap=new HashMap<String,String>();

	public WorkloadGenerate() {
		// TODO Auto-generated constructor stub
	}
	
	public Map<String,String> getSqlMap(){
		return sqlmap;
	}
	
	// 直接赋值初始化SQL语句
	public List<String> initSql(){
		List<String> sqls=new ArrayList<String>();
		sqls.add("select count(*) from customer;");//2 short
		sqls.add("select count(*) from lineitem;");
		sqls.add("select count(*) from lineitem100m;");
		sqls.add("select count(*) from lineitem10m;");
		sqls.add("select count(*) from lineitem1m;");
		sqls.add("select count(*) from lineitem1ms;");
		sqls.add("select count(*) from lineitem50m;");
		sqls.add("select count(*) from nation;");
		sqls.add("select count(*) from orders;");
		sqls.add("select count(*) from part;");
		sqls.add("select count(*) from partsupp;");
		sqls.add("select count(*) from region;");
		sqls.add("select count(*) from supplier;");
		return sqls;
	}
	
	// 随机化SQL请求
	public List<String> genSqllist(){
		List<String> sqls=readSql();
		List<String> sqllist=new ArrayList<String>();
		if(dis==1){
			for(int i=0;i<sqlNum;i++){			
				int p=(int)(Math.random()*sqls.size());
				sqllist.add(sqls.get(p));}
		}
		else if(dis==2){
			ZipfDistribution zd = new ZipfDistribution(sqls.size(),1);
			for(int i=0;i<sqlNum;i++){			
				int p=(int)zd.sample();
				sqllist.add(sqls.get(p-1));}
		}
		return sqllist;
	}
		
	// 读取文件初始化SQL语句
	public List<String> readSql(){
		BufferedReader in;
		List<String> ls=new ArrayList<String>();
		String sql="";
		//设置文件夹路径
		File f=new File(sqlPath);
		try {
			//判断是否为文件夹
			if (f.isDirectory()){
				//得到文件夹下所有内容
				String[] flist=f.list();
				for(int i=0;i<flist.length;i++){
					//对每个文件进行处理
					in = new BufferedReader(new FileReader(sqlPath+flist[i]));
					String s; //用于暂存读取的文件内容
					s=in.readLine();
					while((s=in.readLine())!=null){
						//拼接SQL语句
						sql=sql+s+" ";						
					}
					in.close();
					ls.add(sql);
					//更新SSD中查询语句与查询
					sqlmap.put(sql, i+"file");
					//完成一个文件的读取输出OK
					System.out.println(flist[i]+"->"+i+":"+sql+"OK");
					sql="";
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return ls;
	}	


}
