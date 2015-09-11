package com.ks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
import java.util.UUID;

public class GenLog {
    static int count=0;
   public static void main(String[] args) throws IOException, InterruptedException, ParseException {
    	count=0;
		int FILESIZE = 1024*1024;//1m
		int filenums=1;//begin 4:16 
		SimpleDateFormat smf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date d = smf.parse("2015-02-11 11:55:13.812");
		long begindate = d.getTime();
		for (int i = 1; i <=filenums; i++) {
			String fpath = "F:/wangliang/storm/flumelog/"+System.currentTimeMillis();
			FileOutputStream fos = getFOStream(fpath+".tmp");
			FileChannel fc = fos.getChannel();
			int fsize=0;
			int bufsize=5*1024*1024;
			ByteBuffer buf = ByteBuffer.allocate(bufsize+30);
			byte[] msg =null;
			while(fsize<FILESIZE){
				msg= genData(String.valueOf(begindate)).getBytes();
				int preposition = msg.length+buf.position();
				fsize+=msg.length;
				System.out.println("fsize:"+fsize+" < "+FILESIZE);
				System.out.println("preposition:"+preposition);
				if(preposition<bufsize){
					buf.put(msg);
				}else{
					buf.flip();
					fc.write(buf);
					buf.clear();
					buf.put(msg);
				}
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				begindate++;
			}
			buf.flip();
			fc.write(buf);
			fc.close();
			fos.close();
			File f = new File(fpath+".tmp");
			f.renameTo(new File(fpath+".txt"));
			System.out.println("ms:"+System.currentTimeMillis());
			System.out.println("date:"+smf.format(new Date()));
			printysumset();
			Thread.sleep(500);
		}
	} 
   public static void printBuf(ByteBuffer buf){
	   Charset charset = Charset.forName("UTF-8");
	   CharsetDecoder dec = charset.newDecoder();
	   CharBuffer charbuf;
	try {
		charbuf = dec.decode(buf);
		//buf.flip();
		System.out.println(charbuf.toString());
	} catch (CharacterCodingException e) {
		e.printStackTrace();
	}
   }
    public static FileOutputStream getFOStream(String filePath) throws IOException{
    	File f = new File(filePath);
    	if(!f.exists())f.createNewFile();
    	return new FileOutputStream(f); 
    }
    public static void printysumset(){
    	for(Entry<String, ConcurrentHashMap<String, Integer>> entry:ymap.entrySet()){
    		System.out.println(entry.getKey()+":"+entry.getValue());
    	}
    }
    
	private static Map<String, ConcurrentHashMap<String, Integer>> ymap=new ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>();
    public static String genData(String create){
    	System.out.println("count:"+(++count));
    	Random r = new Random();
    	int id = r.nextInt(10000000);
		int memberid = r.nextInt(100000);
		int totalprice = r.nextInt(1000)+100;
		int youhui = r.nextInt(100);
		int sendpay = r.nextInt(30);
		StringBuffer data = new StringBuffer();
		data.append(String.valueOf(id))
		.append("\t")
		.append(String.valueOf(memberid))
		.append("\t")
		.append(String.valueOf(totalprice))
		.append("\t")
		.append(String.valueOf(youhui))
		.append("\t")
		.append(String.valueOf(sendpay))
		.append("\t")
		.append(create)//System.currentTimeMillis())
		.append("\n");
		String str = data.toString();
		System.out.print(str);
		ConcurrentHashMap<String, Integer> ym = ymap.get(String.valueOf(sendpay));
		if(ym==null){
			ym =new ConcurrentHashMap<String, Integer>();
			ymap.put(String.valueOf(sendpay), ym);
		} 
		Integer ysum = ym.get("sum");
		if (ysum == null)
			ysum = 0;
		ysum+=Integer.valueOf(youhui);
		ym.put("sum", ysum);
		Integer ycount = ym.get("count");
		if (ycount == null)
			ycount = 0;
		ycount++;
		ym.put("count", ycount);
		return str;
    }
}
