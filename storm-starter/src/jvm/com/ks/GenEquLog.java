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
import java.util.Random;



/**
 * @author user
 *
 */
public class GenEquLog {
	public static void main(String[] args) throws Exception {//负责控制生成的线程数目
		/*args= new String[2];
		args[0]="x";
		args[1]="30";*/
		if(args.length!=3){
			throw new Exception("参数个数不对，应该为3个");
		}
		String suborgcode=args[0];
		int equs=Integer.valueOf(args[1]);
		String logdir=args[2];
		GenEquLog gnlog = new GenEquLog(suborgcode, equs,logdir);
		gnlog.startGenerateLog();
	}
	
	List<String> vals = null;//变量数组
	String suborgcode;//风场代码
	int equs;//风机数目
	static final String DEFAULT_LOG_DIR="F:/wangliang/storm/flumelog/";
	String LOG_DIR; 
	Random random = new Random();
	SimpleDateFormat smf = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public GenEquLog(String suborgcode,int equs) {
		this(suborgcode, equs, DEFAULT_LOG_DIR);
	}
	public GenEquLog(String suborgcode,int equs,String logdir) {
		this.suborgcode=suborgcode;
		this.equs=equs;
		this.LOG_DIR=logdir;
		loadVals();//加载变量名称
	}
	public void startGenerateLog(){
		int maxequs=100000;
		for (int i = 1; i < this.equs+1; i++) {
			new Thread(new Worker(String.valueOf(maxequs+i))).start();//启动单个风电数据生成线程
		}
	}
	class Worker implements Runnable{
		private static final int DEFAULT_INTERVAL=1000;
		private static final int DEFAULT_BUFFER_SIZE=5*1024*1024;
		private String equnum;//风机编号
		private int interval;
		private ByteBuffer buf = null;//ByteBuffer.allocate(1024);
		@SuppressWarnings("unused")
		private Worker(){};
		public Worker(String equnum) {
			this(equnum, DEFAULT_INTERVAL);
		}
		public Worker(String equnum,int interval) {
			this(equnum, DEFAULT_INTERVAL, DEFAULT_BUFFER_SIZE);
		}
		public Worker(String equnum,int interval,int bufsize) {
			this.equnum=equnum;
			this.interval=interval;
			buf = ByteBuffer.allocate(bufsize);
		}
		@Override
		public void run() {//控制文件的生成频率
			while(true){
				long curtime = System.currentTimeMillis();
				String filename=suborgcode+"00001_"+equnum+"_"+curtime;
				try {
					generateLogFile(filename,curtime);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				try {
					Thread.sleep(this.interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		/**
		 * 生成日志文件
		 * @param filename 生成文件名称
		 * @param curtime 监测时间
		 * @throws IOException
		 */
		public void generateLogFile(String filename, long curtime) throws IOException {
			String filepath = LOG_DIR+"/"+filename;
			FileOutputStream fos = getFOStream(filepath+".tmp");
			writeLogContent(curtime,fos.getChannel());
			fos.close();
			File f = new File(filepath+".tmp");
			f.renameTo(new File(filepath+".txt"));
		}

		/**
		 * 随机生成日志内容
		 * @param curtime  监测时间
		 * @param fileChannel 
		 * @return
		 * @throws IOException 
		 */
		public void writeLogContent(long curtime, FileChannel fileChannel) throws IOException {
			buf.put(generateLogContent(curtime));
			buf.flip();
			fileChannel.write(buf);
			fileChannel.close();
			buf.clear();
		}
		/**
		 * @param curtime 监测时间
		 * @return
		 */
		public byte[] generateLogContent(long curtime){
			/* 	变量名		监测时间		风电型号编号	风电企业编号	风电场编号		风机编号		监测值
				valname		curtime		equmodel	org			suborg		equnum		value */
			StringBuffer content = new StringBuffer();
			for(int i=0;i<vals.size();){
				String val = vals.get(i);
				String valname=val;//变量名称
				String equmodel="000001";//风电型号编号
				String org="000001";//风电企业编号
				String suborg=suborgcode+"00001";//风电场编号
				int value = random.nextInt(100);
				System.out.println(valname+getBlank(40-valname.length())+"\t"+curtime+"("+smf.format(new Date(curtime))+")"+"\t"+equmodel+"\t"+org+"\t"+suborg+"\t"+equnum+"\t"+value);
				content.append(valname+"\t");
				content.append(curtime+"\t");
				content.append(equmodel+"\t");
				content.append(org+"\t");
				content.append(suborg+"\t");
				content.append(equnum+"\t");
				content.append(value);
				i++;
				if(i<vals.size())content.append("\n");
			}
			return content.toString().getBytes();
		}

		public String getBlank(int size){
			String blank="";
			while(size-->0){
				blank+=" ";
			}
			return blank;
		}
		
		/**
		 *  获得输出流
		 * @param filePath 文件路径
		 * @return
		 * @throws IOException
		 */
		public FileOutputStream getFOStream(String filePath) throws IOException {
			File f = new File(filePath);
			if (!f.exists())
				f.createNewFile();
			return new FileOutputStream(f);
		}
	}
	/**
	 * 加载监测变量
	 */
	public void loadVals() {
		vals = new ArrayList<String>(){
			private static final long serialVersionUID = -9017642088965571785L;

			{
				add("CI_Ncc310Temperature1");
				add("CI_Ncc310Temperature2");
				add("CI_NacelleTemperature1");
				add("CI_NacelleTemperature2");
				add("CI_OutsideAirTemperature");
				add("CI_NacellePosition");
				add("CI_HydraulicPpAccumPrechargePressure");
				add("CI_HssHydraulicBrakePressure");
				add("CI_YawHydraulicBrakePressure");
				add("CI_GearboxSumpOilTemperature");
				add("CI_GearboxOilTemperatureInlet");
				add("CI_GearboxHssBearingTemperature");
				add("CI_GearboxLssBearingTemperature");
				add("CI_GearboxOilPressure");
				add("CI_GeneratorWindingUTemperature1");
				add("CI_GeneratorWindingVTemperature1");
				add("CI_GeneratorWindingWTemperature1");
				add("CI_GeneratorWindingUTemperature2");
				add("CI_GeneratorWindingVTemperature2");
				add("CI_GeneratorWindingWTemperature2");
				add("CI_GeneratorSlipRingTemperature");
				add("CI_GeneratorBearingDrivetrainTemperature");
				add("CI_GeneratorBearingNonDrivetrainTemperature");
				add("CI_RotorSpeed");
				add("CI_WindSpeed1");
				add("CI_YawError1");
				add("CI_WindSpeed2");
				add("CI_YawError2");
				add("CI_TowerBasePcsCoolingWaterTemperature");
				add("CI_TowerBaseEnvironmentTemperature");
				add("CI_TowerBaseCabinetTemperature");
				add("CI_TowerBaseTransformerTempHV1");
				add("CI_TowerBaseTransformerTempHV2");
				add("CI_TowerBaseTransformerTempHV3");
				add("CI_TowerBaseTransformerTempHV4");
				add("CI_BoxTransformerAnalogInput1");
				add("CI_BoxTransformerAnalogInput2");
				add("CI_BoxTransformerAnalogInput3");
				add("CI_BoxTransformerAnalogInput4");
				add("CI_SubPitchPosition1");
				add("CI_SubPitchPosition2");
				add("CI_SubPitchPosition3");
				add("CI_SubPitchRate1");
				add("CI_SubPitchRate2");
				add("CI_SubPitchRate3");
				add("CI_SubPitchPrivMotorCurrentBlade1");
				add("CI_SubPitchPrivMotorCurrentBlade2");
				add("CI_SubPitchPrivMotorCurrentBlade3");
				add("CI_SubPitchPrivVoltage1");
				add("CI_SubPitchPrivVoltage2");
				add("CI_SubPitchPrivVoltage3");
				add("CO_SubPitchPositionDemand1");
				add("CO_SubPitchPositionDemand2");
				add("CO_SubPitchPositionDemand3");
				add("CO_SubPitchRateDemand1");
				add("CO_SubPitchRateDemand2");
				add("CO_SubPitchRateDemand3");
				add("CI_SubPcsDcVoltage");
				add("CI_SubPcsMainsVoltage");
				add("CI_SubPcsMainsCurrent");
				add("CI_SubPcsReactivePower");
				add("CI_SubPcsActivePower");
				add("CI_SubPcsMeasuredGeneratorSpeed");
				add("CI_SubPcsMeasuredElectricalTorque");
				add("CI_SubPcsGridFrequency");
				add("CI_SubPcsGroundCurrent");
				add("CI_SubPcsGridVoltageAB");
				add("CI_SubPcsGridVoltageBC");
				add("CI_SubPcsGridVoltageCA");
				add("CI_SubPcsGridCurrentA");
				add("CI_SubPcsGridCurrentB");
				add("CI_SubPcsGridCurrentC");
				add("CI_SubPcsLineSideCurrent");
				add("CI_SubPcsGeneratorSideCurrent");
				add("CI_SubPcsStatorCurrent");
				add("CI_SubPcsIGBTTemp1");
				add("CI_SubPcsIGBTTemp2");
				add("CI_SubPcsIGBTTemp3");
				add("CO_SubPcsTorqueDemand");
				add("CO_SubPcsReactivePowerDemand");
				add("CI_SubVibNacelleForeAftAcceleration");
				add("CI_SubVibNacelleSideSideAcceleration");
				add("CI_SubVibGearboxAcceleration");
				add("CI_SubVibPrivBand1");
				add("CI_SubVibPrivBand2");
				add("CI_SubVibPrivBand3");
				add("CI_SubVibPrivBand4");
				add("CI_SubVibPrivBand5");
				add("CI_SubVibPrivBand6");
				add("CI_SubVibPrivBand7");
				add("CI_SubVibPrivBand8");
				add("CI_SubVibPrivBand9");
				add("CI_SubVibPrivBand10");
				add("CI_SubVibPrivBand11");
				add("CI_SubVibPrivBand12");
				add("CI_SubIprVoltageL1L2");
				add("CI_SubIprVoltageL2L3");
				add("CI_SubIprVoltageL3L1");
				add("CI_SubIprCurrentL1");
				add("CI_SubIprCurrentL2");
				add("CI_SubIprCurrentL3");
				add("CI_SubIprRealPower");
				add("CI_SubIprReactivePower");
				add("CI_SubIprPrivPowerFactor");
				add("CI_SubIprFrequency");
				add("CI_SubIprEnergyReal");
				add("CI_AC400VActivePower");
				add("CI_AC400VReactivePower");
				add("CI_AC400VApparentPower");
				add("CI_AC400VConsume");
				add("CI_YawActivePower");
				add("CI_YawModuleTemperature");
				add("CI_YawGeneratorCurrent");
				add("CI_YawMotorVoltage");
				add("CI_YawDCVoltage");
				add("D_TShortAverageWindSpeed");
				add("D_TMedAverageWindSpeed");
				add("D_TLongAverageWindSpeed");
				add("D_YawRate");
				add("UC_ScadaActivePowerLimit");
				add("UC_ScadaReactivePowerLimit");
				add("P_ActivePowerLimit");
			}
		};
	}
}


