package com.uniplore;
import redis.clients.jedis.Jedis;
import java.util.*;
public class RedisTest {

	public static int limit=10;	
	public Jedis jedis;
	public static void main(String[] args) {
		RedisTest rt=new RedisTest();
		// TODO Auto-generated method stub
		rt.connection();
		rt.start();
	}
	public Jedis connection(){
		jedis=new Jedis("localhost");
		return jedis;
	}
	public void start(){
		String data=null;
		String key=null;
		int current=0;
		int i=0;
		int count=0;
		HashMap<String,String> hm=new HashMap<String,String>();
		
		System.out.println("Connection to server sucessfully");
		System.out.println("Server is running:"+jedis.ping());
		//Element[] elarry=new Element[10];		
		for(i=0;i<10;i++){
			key=i+"test";
			data=i+"test";
			hm.put(key,data);
//			//elarry[i].setCount();
//			//elarry[i].setData("test");
     		hashinsert(key,hm);
		}
		//	hashinsert("key"+i,count,"data"+i);
		for(i=0;i<1;i++)
		getValue(i+"test"); 
	}
	public void hashinsert(String key,HashMap hm){
		System.out.println(jedis.hmset(key,hm).toString());
		
	}
	
	public void getValue(String key){
		HashMap<String,String> hm=new HashMap<String,String>();
		String ss=new String();
		List<String> ee=jedis.hmget(key,ss);
		System.out.println(ss);
		for(String c:ee){
			System.out.println(c);
		}
	}
	
	public void exist(String key){

		System.out.println(jedis.exists(key).toString());
	}

}
