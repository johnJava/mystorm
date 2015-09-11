package com.ks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class GenEquLog {
    static int count=0;
   public static void main(String[] args) throws IOException, InterruptedException {
    	count=0;
		int FILESIZE = 5*1024*1024;//1m
		int filenums=1024;//begin 4:16
		for (int i = 1; i <=filenums; i++) {
			String fpath = "/home/wangliang/flumetmp/flumelog/"+System.currentTimeMillis();
			FileOutputStream fos = getFOStream(fpath+".tmp");
			FileChannel fc = fos.getChannel();
			int fsize=0;
			int bufsize=128*1024;
			fsize+=bufsize;
			ByteBuffer buf = ByteBuffer.allocate(bufsize);
			byte[] msg =null;
			while(fsize<FILESIZE){
				msg= genData().getBytes();
				int preposition = msg.length+buf.position();
				System.out.print("fsize:"+fsize+" < "+FILESIZE);
				System.out.println("preposition:"+preposition);
				if(preposition<bufsize){
					buf.put(msg);
				}else{
					buf.flip();
					fc.write(buf);
					buf.clear();
					fsize+=bufsize;
					buf.put(msg);
				}
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			fc.write(buf);
			fc.close();
			fos.close();
			File f = new File(fpath+".tmp");
			f.renameTo(new File(fpath+".txt"));
			System.out.println("ms:"+System.currentTimeMillis());
			System.out.println("date:"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
			Thread.sleep(500);
		}
	} 
    public static FileOutputStream getFOStream(String filePath) throws IOException{
    	File f = new File(filePath);
    	if(!f.exists())f.createNewFile();
    	return new FileOutputStream(f); 
    }
    public static String genData(){
    	System.out.println("count:"+(++count));
    	Random r = new Random();
    	int id = r.nextInt(10000000);
		int memberid = r.nextInt(100000);
		int totalprice = r.nextInt(1000)+100;
		int youhui = r.nextInt(100);
		int sendpay = r.nextInt(3);
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
		.append(System.currentTimeMillis())
		.append("\n");
		String str = data.toString();
		System.out.println(str);
		return str;
    }
}
