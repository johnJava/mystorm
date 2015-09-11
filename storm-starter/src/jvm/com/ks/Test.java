package com.ks;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Test {

	public static void main(String[] args) {
//		List<String> ls = new ArrayList<String>(10000);
//				System.out.println(ls.size());
//	 long l = Long.MAX_VALUE;
//	 System.out.println("1441676848080:"+(l-1441676848080l));
		SimpleDateFormat smf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println(smf.format(new Date()));
		
	}

}
