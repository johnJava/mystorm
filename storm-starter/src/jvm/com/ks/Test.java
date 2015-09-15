package com.ks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Test {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		SimpleDateFormat smf = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date d1 = smf.parse("2015-09-12 20:00:00.000");
		Date d2 = smf.parse("2015-09-12 20:01:00.000");
		Date d3 = smf.parse("2015-09-12 20:02:00.000");
		Date d4 = smf.parse("2015-09-12 20:03:00.000");
		Date d5 = smf.parse("2015-09-12 20:04:00.000");
		Date d6 = smf.parse("2015-09-12 20:05:00.000");
		Date d7 = smf.parse("2015-09-12 20:06:00.000");
		Date d8 = smf.parse("2015-09-12 20:07:00.000");
		Date d11 = smf.parse("2015-09-12 20:00:00.000");
		Date d12 = smf.parse("2015-09-12 21:00:00.000");
		Date d13 = smf.parse("2015-09-12 22:00:00.000");
		Date d14 = smf.parse("2015-09-12 23:00:00.000");
		Date d15 = smf.parse("2015-09-13 00:00:00.000");
		Date d16 = smf.parse("2015-09-13 01:00:00.000");
		Date d17 = smf.parse("2015-09-13 02:00:00.000");
		Date d18 = smf.parse("2015-09-13 03:00:00.000");
		Date d19 = smf.parse("2015-09-13 23:59:59.999");
		long dlong = d19.getTime();
		System.out.println("d19 long ="+dlong);
		long lt = dlong/1000;
		System.out.println("d19 sec ="+lt+"("+smf.format(new Date(lt*1000l))+")");
		lt = dlong/60000;
		System.out.println("d19 m ="+lt+"("+smf.format(new Date(lt*60000l))+")");
		lt = dlong/(60000*60);
		System.out.println("d19 h ="+lt+"("+smf.format(new Date(lt*3600000l))+")");
		lt = (dlong+60000*60*8)/(60000*60*24);
		System.out.println("d19 d ="+lt+"("+smf.format(new Date(lt*60000*60*24l))+")");
		System.out.println("d19 d ="+lt+"("+d19.getDay()+")");
		System.out.println("d19 W ="+lt+"("+smf.format(new Date(lt*60000*60*24l))+")");
		System.out.println("d19 M ="+lt+"("+d19.getMonth()+")");
		System.out.println("d19 Y ="+lt+"("+d19.getYear()+")");
		
		System.out.println(d1.getTime());
		System.out.println(d2.getTime());
		System.out.println(d3.getTime());
		System.out.println(d4.getTime());
		System.out.println(d5.getTime());
		System.out.println(d6.getTime());
		System.out.println(d7.getTime());
		System.out.println(d8.getTime());
		System.out.println(d11.getTime());
		System.out.println(d12.getTime()+"-"+d11.getTime()+":"+(d12.getTime()-d11.getTime()));
		System.out.println(d13.getTime()+"-"+d12.getTime()+":"+(d13.getTime()-d12.getTime()));
		System.out.println(d14.getTime()+"-"+d13.getTime()+":"+(d14.getTime()-d13.getTime()));
		System.out.println(d15.getTime()+"-"+d14.getTime()+":"+(d15.getTime()-d14.getTime()));
		System.out.println(d16.getTime()+"-"+d15.getTime()+":"+(d16.getTime()-d15.getTime()));
		System.out.println(d17.getTime()+"-"+d16.getTime()+":"+(d17.getTime()-d16.getTime()));
		System.out.println(d18.getTime()+"-"+d17.getTime()+":"+(d18.getTime()-d17.getTime()));
		System.out.println(smf.format(new Date(1442070000000l)));
		System.out.println(smf.format(new Date(1442080000000l)));
		System.out.println(smf.format(new Date(1442090000000l)));
		System.out.println(smf.format(new Date(1442100000000l)));
		System.out.println(smf.format(new Date(1442200000000l)));
		System.out.println(smf.format(new Date(1441965484931l)));
		System.out.println(smf.format(new Date(1441965485945l)));
		System.out.println(smf.format(new Date(1441965486958l)));
		System.out.println(smf.format(new Date(1441965487970l)));
		System.out.println(smf.format(new Date(1441965488982l)));
	}

}
