package com.uniplore;
import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
public class CacheManagement {
    /* Redis db保存数据
     * 1 对应缓存数据存储
     * 2 对应缓存度量数据存储
     * 3 对应缓存链表 LRUList FIFOList
     * 4 CRSR 5 LIRS 6 SSD Data  7 GDS  8 CSRSR 
     */
	// 线程池的构建
	private static JedisPool pool = null;
	
	private ReentrantLock lock= new ReentrantLock();
	
	public int diskType=1;
	/* 是否使用磁盘缓存数据
	 * 0 不使用   非0 Disk/SSD
	 */
	private String writePath="d:/RedisData/";
	
	public static int CacheType=6;
	/* 缓存算法类型
	 * 1 LRU 2 FIFO 3 CRSR 4 LIRS 5 GDS 6 CSRSR	*/		
	private static int up=1;
	private static double percent=0.5;
	private static int RedisSizeLimit=(int)(12425*up*percent/9);//6213 690	33687
	private static int DiskSizeLimit=(int)(12425*up*percent/9*8);  //269498
	/* Redis 大小  Disk/SSD大小  */
	private static int writeBackToRedis=0;  //WR
	// 1 命中后写回Redis 0命中后不写回
	private static int writeBackToSSD=0;   //WS
	// 1 删除后写入到SSD 0删除后不写入到SSD
	
	private static int writeCost=1;
	// 1 直接插入代价 0使用算法代价
	private static String driver="org.postgresql.Driver";
	private static String url ="jdbc:postgresql://192.168.100.125:5432/tian_5G_tpch";
	private static String user = "lys";
	private static String pwd = "gpadmin";

//	private static String driver="com.mysql.jdbc.Driver";
//	private static String url ="jdbc:mysql://localhost:3306/foodmart?useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull";
//	private static String user = "root";
//	private static String pwd = "mysql";	
	
	private WorkloadGenerate wg;
	private Map<String,String> sqlmap=new HashMap<String,String>();
	private static long allTime=0;
	private static long requestNum=0;
	private static long startRedisTime=0;
	private static long cacheHit=0;
	private static long ssdHit=0;
	private static int lirLong=(int) (RedisSizeLimit*0.8);	/* LIRS参数 */

       	
	// 生成工作负载
	public List<String> generateWorkload(){
		wg=new WorkloadGenerate();
		List<String> sqls=wg.genSqllist();
		sqlmap=wg.getSqlMap();
		return sqls;
	}
	
	//得到当前Redis缓存剩余大小
	public int getUseRedisSize() {
		int useSize=0;
		pool = getPool();
		Jedis jedis=pool.getResource();	
		switch(CacheType){
		case 1:
			break;
		case 2:
			break;
		case 3:
			jedis.select(4);
			Set<String> srkeys=jedis.zrange("CandiateList", 0, -1);
			jedis.select(2);
			for(String gdskey:srkeys){
				useSize=(int) (useSize+jedis.zscore("Size", gdskey));
			}
			break;
		case 4:
			break;
		case 5:
			jedis.select(7);
			Set<String> gdskeys=jedis.zrange("GDSSet", 0, -1);
			jedis.select(2);
			for(String gdskey:gdskeys){
				useSize=(int) (useSize+jedis.zscore("Size", gdskey));
			}
			break;
		case 6:
			jedis.select(8);
			Set<String> cskeys=jedis.zrange("CandiateList", 0, -1);
			jedis.select(2);
			for(String gdskey:cskeys){
				useSize=(int) (useSize+jedis.zscore("Size", gdskey));
			}
			break;
		}
		returnResource(pool, jedis);
		return useSize;
	}
	
	// 得到当前SSD/DISK缓存剩余大小	
	public int getUseDiskSize() {
		int useSize=0;
		pool = getPool();
		Jedis jedis=pool.getResource();	
		jedis.select(6);
		Set<String> ls=jedis.zrange("SSDData", 0, -1);
		jedis.select(2);
		for(String gdskey:ls){
			useSize=(int) (useSize+jedis.zscore("Size", gdskey));
			}	
		return useSize;
	}

	// 初始化监测指标
	public void initMetaData(){
		startRedisTime=System.currentTimeMillis();
		pool = getPool();
		Jedis jedis=pool.getResource();	
		jedis.select(2);
		jedis.hset("Total", "time", "0");
		jedis.hset("Total", "requestNum", "0");
		jedis.hset("Total", "hit", "0");
		jedis.hset("Total", "diskHit", "0");
		//jedis.zadd("Size", 0,"useSize");
		//jedis.zadd("Size", 0,"diskUseSize");
		returnResource(pool,jedis);	
	}
	
	
		
	public Map<String, String> getSqlmap() {
		return sqlmap;
	}

	public void setSqlmap(Map<String, String> sqlmap) {
		this.sqlmap = sqlmap;
	}

	// 查询请求
	public List<String> searchData(String sql) throws Exception{
		List<String> ls=new ArrayList<String>();
		pool = getPool();
		Jedis jedis=pool.getResource();	
		jedis.select(2);		
		long startTimex = System.currentTimeMillis();
		jedis.hincrBy("Total", "requestNum", 1);
		requestNum++;
		// 缓存中已存在
		if(isExist(sql)){			
			ls=getData(sql);
			jedis.select(2);
			jedis.hincrBy("Total", "hit", 1);
			long endTimex = System.currentTimeMillis();
			long time = endTimex-startTimex;
			allTime+=time;
			cacheHit++;
			jedis.hincrBy("Total", "time", time);
	    	System.out.println("Redis命中:"+allTime+"ms"+",Sum"+requestNum+",cacheHit"+cacheHit+",SSDHit"+ssdHit);
	    	hitKey(sql);
		}
		else {
			if(diskType!=0){
				jedis.select(6);
				boolean flag=jedis.exists(sql);
				// SSD/Disk中存在
				if(flag==true){
					ssdHit++;
					
					String path=jedis.get(sql);
					jedis.select(2);
					jedis.hincrBy("Total", "diskHit", 1);					
					double rssize=jedis.zscore("Size", sql);
					double score=1;
				    if(writeCost==1){
				    	score=jedis.zscore("Time", sql);
				    }
				    else{
				    	jedis.select(6);
				    	score=jedis.zscore("SSDData", sql);
				    	jedis.select(2);
				    }
					ls=getDataFromDisk(path);					
					
					long endTimex = System.currentTimeMillis();
					long time = endTimex-startTimex;
					//命中后是否直接写回
					if(writeBackToRedis==1){
						//如果Redis缓存有足够空间，则将结果集写回Redis
						if(rssize<RedisSizeLimit){
							insertData(sql,ls,(long)score,(int)rssize);
							delDataInDiskBySql(sql);
						}
					}
					allTime+=time;
					jedis.hincrBy("Total", "time", time);
			    	System.out.println("SSD命中总时间为:"+allTime+"ms"+",hitSum"+requestNum+"cacheHit"+cacheHit+",SSDHit"+ssdHit);
			    	hitKeyInSSD(sql);
				}
				else{
					ls=getDataBySql(sql,startTimex);}
				}
			else{
				ls=getDataBySql(sql,startTimex);	
				}									
		}
		System.out.println("Total time is"+(System.currentTimeMillis()-startRedisTime));
		returnResource(pool,jedis);
		return ls;
	}
	
	private List<String> getDataBySql(String sql,long startTimex) throws Exception{
		List<String> rsList=new ArrayList<String>();
		pool = getPool();
		Jedis jedis=pool.getResource();	
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, user, pwd);
		Statement st= conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);			
		ResultSet rs = st.executeQuery(sql);
		int sizecount=0;		
		ResultSetMetaData rsmd=rs.getMetaData();
		int colcount=rsmd.getColumnCount();
		while(rs.next()){
			String td="";
			for(int i=1;i<=colcount;i++){
				td=td+rs.getString(i);
				sizecount++;
			}
			for(int j=0;j<up;j++){
				rsList.add(td);
			}	
		}	
		conn.close();
		long endTimex = System.currentTimeMillis();
		long time = endTimex-startTimex;
		allTime+=time;

		sizecount=sizecount*up;
		jedis.select(2);
		jedis.hincrBy("Total", "time", time);
		System.out.println("未命中总时间为:"+allTime+"ms"+"-hit"+requestNum+",cacheHit"+cacheHit+",SSDHit"+ssdHit);
		System.out.println("总时间为:"+(endTimex-startRedisTime));
		jedis.zadd("Size", sizecount, sql);
		jedis.zadd("Time", time, sql);
		insertData(sql,rsList,time,sizecount);
		returnResource(pool,jedis);
		return rsList;
	}
	
	// 未命中则将数据加入到Redis或SSD/Disk中
	private void insertData(String key,List<String> rs,long time,int rsSize) throws Exception{
		// key为SQL语句 ,rs为SQL结果集,time为响应时间,判断是否插入成
		boolean flg=insertResultToRedis(key,rs,rsSize);
		if(flg){
			pool = getPool();
			Jedis jedis=pool.getResource();				
		switch(CacheType){
			case 1:// 在名称为LRUList的list头添加一个值为key的元素
				jedis.select(3);
				jedis.lpush("LRUList", key);
				//System.out.println("key"+key+"被加入LRU");		    
				break;
			case 2://For FIFO
				// 在名称为FIFOList的list头添加一个值为key的元素
				jedis.select(3);
				jedis.rpush("FIFOList", key);
				//System.out.println("key"+key+"被加入FIFO");
				break;
			case 3:// For CRSR				
				//For CRSR-Size
				//jedis.select(2);
				//double elementsize=jedis.zscore("Size", key);
				jedis.select(4);
				jedis.zadd("HitList",1,key);
				jedis.zadd("TimeList",System.currentTimeMillis(),key);
				jedis.zadd("Time",time,key);
				jedis.zadd("CandiateList",time*requestNum/allTime, key);
				//For CRSR-Size
				//jedis.zadd("CandiateList",time*requestNum*RedisSizeLimit/(allTime*elementsize), key);					
				break;		
			case 4://For LIRS
				jedis.select(5);
				Long len = jedis.llen("LIRList");
				if(len<lirLong){
					jedis.lpush("LIRList", key);
				}else{
					jedis.lpush("HIRList", key);	
				}			
				break;
			case 5://For GDS
				jedis.select(7);
				jedis.zadd("GDSSet",1/rsSize,key);
				break;
			case 6:// For CSRSR				
				jedis.select(8);
				// 计算响应时间
				jedis.zadd("Time",time,key);
			// 初始代价
			//jedis.zadd("CandiateList",time*requestNum/allTime, key);
				if(writeCost==1){
					jedis.zadd("CandiateList",time*requestNum/(allTime*rsSize), key);
					}
				else{
					jedis.zadd("CandiateList",time, key);
				}
				break;
			}	
			returnResource(pool,jedis);	}
		else{
			// do nothing
		} 
	}
	
	//将数据插入到Redis
	private boolean insertResultToRedis(String key,List<String> rs,int rsSize) throws Exception{
		boolean flag=false;
		try{			
			lock.lock();
			if(isExist(key)){
				flag=false;
			}
			else if(rsSize>RedisSizeLimit){
				flag=false;
				WriteToDisk(key,rs,rsSize,0);
			}
			else{
				pool = getPool();
				Jedis jedis=pool.getResource();		
				// 判断数据插入位置
				if((getUseRedisSize()+rsSize)>RedisSizeLimit){
					if((rsSize+getUseDiskSize())>=DiskSizeLimit){
						while(getUseRedisSize()+rsSize>RedisSizeLimit){
							delData(rsSize);			
						}
						// 将key的结果集插入到缓存
						jedis.select(1);
						for(int i=0;i<rs.size();i++){
							jedis.lpush(key, rs.get(i));
						}
						flag=true;
					}
					else{
						WriteToDisk(key,rs,rsSize,0);
						flag=false;
					}					
				}
				else{
					while(getUseRedisSize()+rsSize>RedisSizeLimit){
						delData(rsSize);			
					}
					// 将key的结果集插入到缓存
					jedis.select(1);
					for(int i=0;i<rs.size();i++){
						jedis.lpush(key, rs.get(i));
					}
					flag=true;
				}
				returnResource(pool,jedis);	
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				lock.unlock();
			}
		return flag;
	}

		
	// SQL代价计算 For CRSR
	private float comCost(String key) throws Exception{
		pool = getPool();
		Jedis jedis=pool.getResource();	
		jedis.select(4);
		Double hit=jedis.zscore("HitList", key);
		double timesub=System.currentTimeMillis()-jedis.zscore("TimeList", key);
		double timeexec=jedis.zscore("Time", key);
		returnResource(pool,jedis);
		return (float) (hit*timeexec/timesub);
	}
	
	// 从SSD或Disk获取数据
	private List<String> getDataFromDisk(String path) throws Exception{
		BufferedReader in = new BufferedReader(new FileReader(path));
		List<String> ls=new ArrayList<String>();
		String s=null;
		while((s=in.readLine())!=null){
			ls.add(s);
		}
		in.close();
		return ls;
	}
	
	// 从缓存返回数据
	private List<String> getData(String key){
		pool = getPool();
		Jedis jedis=pool.getResource();
		jedis.select(1);
		List<String> ls=jedis.lrange(key, 0, jedis.llen(key));
		returnResource(pool,jedis);
		return ls;
	}
	
	// 根据key值判断Redis是否存在该key
	private boolean isExist(String key){
		pool = getPool();
		Jedis jedis=pool.getResource();
		jedis.select(1);
		boolean rs=jedis.exists(key);
		returnResource(pool,jedis);
		return rs;
	}
	
	// 从Redis中删除数据
	private void delData(int rsSize) throws IOException{
		pool = getPool();
		Jedis jedis = pool.getResource();
		String key="";
		switch(CacheType){
			case 1:	// 删除LRUList中的队尾的元素		
				jedis.select(3);
				key=jedis.rpop("LRUList");
			    //WriteToDisk(key,getData(key),rsSize);
				break;
			case 2: // 删除key For FIFO
				jedis.select(3);
				key=jedis.lpop("FIFOList");
			    //WriteToDisk(key,getData(key),rsSize);
				break;
			case 3:	// For CRSR
				jedis.select(4); 
				Set<String> keys=jedis.zrange("CandiateList", 0, 1);
				Iterator<String> iter = keys.iterator();
				key =iter.next();
				if(writeBackToSSD==1){
					//WriteToDisk(key,getData(key),rsSize);
					}
				jedis.zrem("CandiateList", key);
				break;
			case 4:// For LIRS
				jedis.select(5);
				if(jedis.llen("HIRList")>0){
					key=jedis.rpop("HIRList");
				}
				else{
					key=jedis.rpop("LIRList");
				}
				if(writeBackToSSD==1){
					//WriteToDisk(key,getData(key),rsSize);
					}
				break;
			case 5:// For GDS
				jedis.select(7);
				Set<String> ckeys=jedis.zrange("GDSSet", 0, 1);
				Iterator<String> citer = ckeys.iterator();
				if(citer.hasNext()){
					key =citer.next();
				   // WriteToDisk(key,getData(key),rsSize);
					double sub=jedis.zscore("GDSSet", key);
					jedis.zrem("GDSSet", key);
					jedis.select(7);
					Set<String> gdskeys=jedis.zrange("GDSSet", 0, -1);
					for(String gdskey:gdskeys){
						jedis.zincrby("GDSSet", sub*(-1), gdskey);
					}
					}
				else{}
				break;
			case 6:// For CSRSR
				jedis.select(8);
				double sub=0;
				Set<String> cskeys=jedis.zrange("CandiateList", 0, 1);
				Iterator<String> csiter = cskeys.iterator();
				if(csiter.hasNext()){
					key =csiter.next();
					sub=jedis.zscore("CandiateList", key);
					if(writeBackToSSD==1){						
						WriteToDisk(key,getData(key),rsSize,sub);
						}	
					//double sub=jedis.zscore("CandiateList", key);
					jedis.zrem("CandiateList", key);
					Set<String> gdskeys=jedis.zrange("CandiateList", 0,-1);
					for(String gdskey:gdskeys){
						jedis.zincrby("CandiateList", sub*(-1), gdskey);
					}
				}
				else {
					System.out.println("noKey");
				}
			  break;
			}
		if(key.length()>1){			
	       	jedis.select(1);		
			jedis.del(key);
			System.out.println(sqlmap.get(key)+"删除");
		}
		System.out.println("当前剩余Redis大小:"+(RedisSizeLimit-getUseRedisSize())+"需要:"+rsSize);
		returnResource(pool,jedis);
	}
	
	//将数据写入磁盘或SSD
	private void WriteToDisk(String key,List<String> rs,int size, double sub) throws IOException{
		// 如果使用SSD/Disk缓存
		if(diskType!=0){
			pool = getPool();
			Jedis jedis=pool.getResource();
			while((size+getUseDiskSize())>=DiskSizeLimit){ 
				delDataInDisk();
            }
			jedis.select(2);
			double rsTime=jedis.zscore("Time", key);
			jedis.select(6);
			boolean flag=jedis.exists(key);
			if(flag==false){
				File file=new File(writePath+sqlmap.get(key));
				FileWriter fw = new FileWriter(file);
				BufferedWriter bw = new BufferedWriter(fw);
				for(String d:rs){			
					bw.write(d);
					bw.newLine();
					bw.flush();
				}
				bw.close();
				fw.close();
				jedis.set(key, writePath+sqlmap.get(key));
				if((writeCost==1)||(sub==0)){
					jedis.zadd("SSDData", rsTime*requestNum/(allTime*size), key);
					}
				else{
					jedis.zadd("SSDData", sub, key);
				}
			}		
			returnResource(pool,jedis);
		}
	}	
	
	private void delDataInDiskBySql(String key)throws IOException{
		pool = getPool();
		Jedis jedis=pool.getResource();
		jedis.select(6);
		jedis.del(key);
		double sub=jedis.zscore("SSDData", key);
		jedis.zrem("SSDData", key);
		boolean success= new File(writePath+sqlmap.get(key)).delete();
		if (success) {
	           System.out.println("Successfully deleted:"+key);	
	           Set<String> gdskeys=jedis.zrange("SSDData", 0,-1);
	   		   for(String gdskey:gdskeys){
	   		   jedis.zincrby("SSDData", sub*(-1), gdskey);
	   		   }
	       } else {
	           System.out.println("Failed to delete empty directory");
	       }
		
		returnResource(pool,jedis);
	}
	
	// 从SSD或Disk上删除对应缓存文件
	private void delDataInDisk() throws IOException{
		pool = getPool();
		Jedis jedis=pool.getResource();
		jedis.select(6);
		//随机删除文件
		//String key=jedis.randomKey();	
		
		//删除代价最小的文件
		Set<String> keys=jedis.zrange("SSDData", 0, 1);
		Iterator<String> iter = keys.iterator();
		String key =iter.next();
		
		//删除Key
		delDataInDiskBySql(key);
		returnResource(pool,jedis);
	}
	
	//SSD命中后
	private void hitKeyInSSD(String key) throws Exception{
		pool = getPool();
		Jedis jedis=pool.getResource();
		jedis.select(2);
	    double sizeScore=jedis.zscore("Size", key);	
	    double rstime = jedis.zscore("Time", key);
	    jedis.select(6);
		jedis.zincrby("SSDData", rstime/sizeScore, key);
		returnResource(pool,jedis);
	}

	// 命中后增加对应的标记位或者对对应缓存单元进行处理
	private void hitKey(String key)throws Exception{
		pool = getPool();
		Jedis jedis=pool.getResource();	
		switch(CacheType){
			case 1:// For LRU
				jedis.select(3);
				// 删除LRUList中10个值为key的元素
				jedis.lrem("LRUList",1,key);
				// 在LRUList的首部添加值为key的元素
				jedis.lpush("LRUList", key);
				//System.out.println("key被命中"+key+"并加入LRU");
				break;
			case 2://For FIFO
				jedis.select(3);
				jedis.lrem("FIFOList", 1, key);
				// 在FIFOList的尾部添加值为key的元素
				jedis.rpush("FIFOList", key);
				//System.out.println("key被命中"+key+"并加入FIFO");
				break;
			case 3:// For CRSR
				jedis.select(4);
				jedis.zincrby("HitList",1,key);	
				jedis.zadd("TimeList", System.currentTimeMillis(),key);
				float cos=0;
				try {
					cos = comCost(key);
				} catch (Exception e) {
					e.printStackTrace();
				}
				jedis.zadd("CandiateList", cos, key);
				break;
			case 4://For LIRS
				jedis.select(5);
				List<String> hirs=jedis.lrange("HIRList", 0, -1);
				int flag=0;
				String lirkey=null;
				for(int i=0;i<hirs.size();i++){
					if(key.equals(hirs.get(i))){
						flag=1;
						break;
					}
				}
				if(flag==1){
					lirkey=jedis.rpop("LIRList");
					jedis.lpush("LIRList", key);
					jedis.lrem("HIRLIst", 1, key);
					jedis.lpush("HIRList", lirkey);
				}else{
					jedis.lrem("LIRList", 1, key);
					jedis.lpush("LIRList", key);
				}
				break;
			case 5://For GDS
				jedis.select(2);
			    double hScore=jedis.zscore("Size", key);
			    jedis.select(7);
				jedis.zincrby("GDSSet", hScore, key);
				break;
			case 6://For CSRSR
				jedis.select(2);
			    double sizeScore=jedis.zscore("Size", key);
			    jedis.select(8);
			    double rstime = jedis.zscore("Time", key);
				jedis.zincrby("CandiateList", rstime/sizeScore, key);
				break;
			}		
			returnResource(pool,jedis);
		}
	
	private JedisPool getPool(){
		if(pool == null){
			JedisPoolConfig config = new JedisPoolConfig();
            //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
            //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
            config.setMaxIdle(1000);
            //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            config.setMaxTotal(1000);
            //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
            //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
            config.setTestOnBorrow(true);
            pool = new JedisPool(config, "localhost", 6379);
		}
		return pool;
	}	
	
	private void returnResource(JedisPool pool,Jedis redis){
		if(redis!=null){
			pool.returnResource(redis);
		}
		
	}

}
