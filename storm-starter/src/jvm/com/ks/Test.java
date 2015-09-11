package com.ks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Test {

	public static void main(String[] args) throws Exception {
		SimpleDateFormat smf = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println(smf.format(new Date(1441965476856l)));
		System.out.println(smf.format(new Date(1441965479715l)));
		System.out.println(smf.format(new Date(1441965481817l)));
		System.out.println(smf.format(new Date(1441965482869l)));
		System.out.println(smf.format(new Date(1441965483914l)));
		System.out.println(smf.format(new Date(1441965484931l)));
		System.out.println(smf.format(new Date(1441965485945l)));
		System.out.println(smf.format(new Date(1441965486958l)));
		System.out.println(smf.format(new Date(1441965487970l)));
		System.out.println(smf.format(new Date(1441965488982l)));
	}

}
