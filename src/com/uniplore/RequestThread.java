package com.uniplore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
/*
 * 线程请求
 */
public class RequestThread implements Runnable {
    private int name;
    private CacheManagement rt;
    private String writePath="d:/WorkLoadData/SSD/DW/"; /*负载保存位置*/
    // d:/WorkLoadData/
    // d:/WorkLoadData/SSD/
    private int wg=2;
    // 1表示生成负载文件 2表示执行负载
    public RequestThread(int Name,CacheManagement redis) {
		name=Name;
		rt=redis;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println(name+"is Starting.");        
        List<String> rsList=new ArrayList<String>();
        List<String> ls=rt.generateWorkload();
        File file=new File(writePath+name+".wl");
        try { 
        	//将生成的工作负载写入到文件
        	if(wg==1){        		
        		FileWriter fw= new FileWriter(file);
        		BufferedWriter bw = new BufferedWriter(fw);
        		for(int i=0;i<ls.size();i++){
        			String key=ls.get(i);
        			bw.write(key);
        			bw.newLine();
        			bw.flush();
		   			}
        		bw.close();
        		fw.close();
        		}
        	else{
        		BufferedReader in= new BufferedReader(new FileReader(file));
        		String s;
        		while((s=in.readLine())!=null){
        			//执行请求
        			rsList=rt.searchData(s);
        			Thread.sleep(1000);
        		}
        		in.close();
				}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
    }

}
