package com.sample.beam.devices;
import java.math.BigInteger;

//import javax.xml.bind.DatatypeConverter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sample.beam.df.shared.DeviceTelemetry;

public class TCPDevice extends Device{

	int currentIndex = 0;
	byte[] hexBytesArr;
	private static final Logger LOG = LoggerFactory.getLogger(TCPDevice.class);
	
	public static void main(String[] args) {

		String hexEncoded = "2353503100c80064313233343536373839303132333435363738393000e5494e2d313330343838383133313330303330303231380000000002000000000014ba001d022d0258000b022d000000000216020900000000013a00cf0000000000ab091a093900000084000000000000000000000000000000000000000008f50000000003e80e6a000000000000465e479a000000000000000000000000446c06670000000000000000439b000000000000470ffb00471718000000000044eaa0004180000042920000000000003f80000046be7e00461ba4000000000000000000470630002e";
		DeviceTelemetry dt = new DeviceTelemetry();
		TCPDevice hd = new TCPDevice();
		hd.processPacket(hexEncoded,dt);
	}

	public void processPacket(String hexEncoded, DeviceTelemetry dt)
	{
		hexBytesArr = toByteArray(hexEncoded);
		
		// Initial details
		LOG.debug("START OF PACKET STRING:" + getStringFromBytes(getBytes(2)));
		
		dt.setFaultPeriodic(getStringFromBytes(getBytes(1)));
		dt.setCommMode(getCommunicationMode(getStringFromBytes(getBytes(1))));
		dt.setSwVersion(getIntegerFromBytes(getBytes(2))/100+"");
		dt.setProtVersion(getIntegerFromBytes(getBytes(2))/100+"");
		dt.setSerialNo(getStringFromBytes(getBytes(20)));
		dt.setDataBytes(getIntegerFromBytes(getBytes(2)));
		dt.setSiteId(getStringFromBytes(getBytes(10)));

		LOG.debug("Identifier of Fault/Perodic:"+ dt.getFaultPeriodic());
		LOG.debug("Communication Mode:" + dt.getCommMode());		
		LOG.debug("Software Version:" + dt.getSwVersion());
		LOG.debug("Protocol Version :" + dt.getProtVersion());
		LOG.debug("IIPMS Serial Number:" + dt.getSerialNo());
		LOG.debug("Number of Data Bytes:" + dt.getDataBytes());
		LOG.debug("Site ID:" + dt.getSiteId());

		// Timestamp 		
		int date = Integer.valueOf(getStringFromBytes(getBytes(2)));
//		LOG.debug("Date:" + date);
		
		int hrs = Integer.valueOf(getStringFromBytes(getBytes(2)));
//		LOG.debug("Hours:" + hrs);
		
		int minutes = Integer.valueOf(getStringFromBytes(getBytes(2)));
//		LOG.debug("Minutes:" + minutes);
		
		int sec = Integer.valueOf(getStringFromBytes(getBytes(2)));
//		LOG.debug("Seconds:" + sec);
		
		int month = Integer.valueOf(getStringFromBytes(getBytes(2)));
//		LOG.debug("Months:" + month);
		
		int year = Integer.valueOf(getStringFromBytes(getBytes(2)));
//		LOG.debug("Years:" + year);

		DateTime datetime = new DateTime(2000+year, month, date, hrs, minutes, sec, 0);
		DateTimeFormatter dformatter = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss.SSS");
		LOG.debug("DateTime:"+ datetime.toString(dformatter));
		dt.setTimestamp(datetime);
		
		
		dt.setAlarm(getAlarms(getBytes(6)));
		dt.setStatus(getStatus(getBytes(6)));
		LOG.debug("Alarm String:" + dt.getAlarm());
		LOG.debug("Status String:" + dt.getStatus());

		LOG.debug("\n ================= Measurement Data ====================");
		dt.setRectifierPower(getFloatFromBytes(getBytes(2)));
		dt.setRectifierVolt(getFloatFromBytes(getBytes(2)));
		dt.setBattCapacity(getFloatFromBytes(getBytes(2)));
		dt.setBattCurrent(new BigInteger(getBytes(2)).intValue());
		dt.setRectCurrent(getFloatFromBytes(getBytes(2)));
		dt.setSolCurrent(getFloatFromBytes(getBytes(2)));
		dt.setWindCurrent(getFloatFromBytes(getBytes(2)));
		dt.setBattVolDc(getFloatFromBytes(getBytes(2)));
		dt.setTotLoadCurr(getFloatFromBytes(getBytes(2)));
		dt.setLoadCurr1(getFloatFromBytes(getBytes(2)));
		dt.setLoadCurr2(getFloatFromBytes(getBytes(2)));
		dt.setLoadCurr3(getFloatFromBytes(getBytes(2)));
		dt.setLoadCurr4(getFloatFromBytes(getBytes(2)));
		dt.setLoadCurr5(getFloatFromBytes(getBytes(2)));
		dt.setLoadCurr6(getFloatFromBytes(getBytes(2)));
		dt.setMainVolR(getFloatFromBytes(getBytes(2)));
		dt.setMainVolY(getFloatFromBytes(getBytes(2)));
		dt.setMainVolB(getFloatFromBytes(getBytes(2)));		
		dt.setMainCurrRY(getFloatFromBytes(getBytes(2)));
		dt.setMainCurrYB(getFloatFromBytes(getBytes(2)));
		dt.setMainCurrBR(getFloatFromBytes(getBytes(2)));
		dt.setMainFreq(getFloatFromBytes(getBytes(2)));
		dt.setGenVolR(getFloatFromBytes(getBytes(2)));
		dt.setGenVolY(getFloatFromBytes(getBytes(2)));
		dt.setGenVolB(getFloatFromBytes(getBytes(2)));
		dt.setGenCurrR(getFloatFromBytes(getBytes(2)));
		dt.setGenCurrY(getFloatFromBytes(getBytes(2)));
		dt.setGenCurrB(getFloatFromBytes(getBytes(2)));
		dt.setGenFreq(getFloatFromBytes(getBytes(2)));
		dt.setGenPower(getFloatFromBytes(getBytes(2)));
		dt.setSvrVol(getFloatFromBytes(getBytes(2)));
		dt.setSvrCurr(getFloatFromBytes(getBytes(2)));
		
		LOG.debug("Rectifier power:" + dt.getRectifierPower());
		LOG.debug("Rectifier IP / Voltage:" + dt.getRectifierVolt());
		LOG.debug("Battery Capacity:" + dt.getBattCapacity());
		LOG.debug("Battery Current:" + dt.getBattCurrent());
		LOG.debug("Rectifier Current:" + dt.getRectCurrent());
		LOG.debug("Solar Current:" + dt.getSolCurrent());
		LOG.debug("Wind Current:" + dt.getWindCurrent());
		LOG.debug("Site Battery voltage DC:" + dt.getBattVolDc());
		LOG.debug("Total Load Currnet:" + dt.getTotLoadCurr());
		LOG.debug("Load Current 1:" + dt.getLoadCurr1());
		LOG.debug("Load Current 2:" + dt.getLoadCurr2());
		LOG.debug("Load Current 3:" + dt.getLoadCurr3());
		LOG.debug("Load Current 4:" + dt.getLoadCurr4());
		LOG.debug("Load Current 5:" + dt.getLoadCurr5());
		LOG.debug("Load Current 6:" + dt.getLoadCurr6());
		LOG.debug("Mains Voltage R:" + dt.getMainVolR());
		LOG.debug("Mains Voltage Y:" + dt.getMainVolY());
		LOG.debug("Mains Voltage B :" + dt.getMainVolB());
		LOG.debug("Mains Current RY:" + dt.getMainCurrRY());
		LOG.debug("Mains Current YB:" + dt.getMainCurrYB());
		LOG.debug("Mains Currnet BR:" + dt.getMainCurrBR());
		LOG.debug("Mains Frequency :" + dt.getMainFreq());
		LOG.debug("Gen Voltage R:" + dt.getGenVolR());
		LOG.debug("Gen Voltage Y:" + dt.getGenVolY());
		LOG.debug("Gen Voltage B :" + dt.getGenVolB());
		LOG.debug("Gen Current R:" + dt.getGenCurrR());
		LOG.debug("Gen Current Y:" + dt.getGenCurrY());
		LOG.debug("Gen Current B:" + dt.getGenCurrB());
		LOG.debug("Gen Frequency :" + dt.getGenFreq());
		LOG.debug("Gen Power:" + dt.getGenPower());
		LOG.debug("SVR Voltage:" + dt.getSvrVol());
		LOG.debug("SVR Current:" + dt.getSvrCurr());

		LOG.debug("\n ======================== Battery Data ====================");		
		dt.setBattDod(getFloatFromBytes(getBytes(2)));
		dt.setBattSoc(getFloatFromBytes(getBytes(2)));
		dt.setShelRoomTemp(getFloatFromBytes(getBytes(2)));
		dt.setShelBattTemp(getFloatFromBytes(getBytes(2)));		
		dt.setGenFuel(getFloatFromBytes(getBytes(2)));
		dt.setGenBattVol(getFloatFromBytes(getBytes(2)));
		
		LOG.debug("Battery DOD:" + dt.getBattDod());
		LOG.debug("Battery SOC:" + dt.getBattSoc());
		LOG.debug("Shelter Room Temp C:" + dt.getShelRoomTemp());
		LOG.debug("Shelter Battery Temp :" + dt.getShelBattTemp());
		LOG.debug("Genset Fuel %:" + dt.getGenFuel());
		LOG.debug("Gen Batt Voltage:" + dt.getGenBattVol());

		LOG.debug("\n ======================== Run Hours Data ====================");
		dt.setMainRunHrs(getFloatFromIeee754(getBytes(4)));
		dt.setDgTotRunHrs(getFloatFromIeee754(getBytes(4)));
		dt.setDgAutoRunHrs(getFloatFromIeee754(getBytes(4)));
		dt.setDgManRunHrs(getFloatFromIeee754(getBytes(4)));
		dt.setBattRunHrs(getFloatFromIeee754(getBytes(4)));
		dt.setSolRunHrs(getFloatFromIeee754(getBytes(4)));
		dt.setWindRunHrs(getFloatFromIeee754(getBytes(4)));
		
		LOG.debug("EB / Mains Run Hours:" + dt.getMainRunHrs());
		LOG.debug("DG Running Total Run Hours:" + dt.getDgTotRunHrs());
		LOG.debug("DG Running Auto Mode:" + dt.getDgAutoRunHrs());
		LOG.debug("DG Running Manual Mode:" + dt.getDgManRunHrs());
		LOG.debug("Battery Run Hours:" + dt.getBattRunHrs());
		LOG.debug("Solar Run Hours:" + dt.getSolRunHrs());
		LOG.debug("Wind Run Hours:" + dt.getWindRunHrs());

		LOG.debug("\n ======================== Battery Data ====================");
		dt.setBattChgCycCnt(getFloatFromIeee754(getBytes(4)));
		dt.setBattDisChgCycCnt(getFloatFromIeee754(getBytes(4)));
		
		LOG.debug("Battery Charge Cycle Count:" + dt.getBattChgCycCnt());
		LOG.debug("Battery Discharge Cycle Count:" + dt.getBattDisChgCycCnt());

		LOG.debug("\n ======================== Energy Data ====================");
		dt.setTotRectEnergy(getFloatFromIeee754(getBytes(4)));
		dt.setIpMainKwh(getFloatFromIeee754(getBytes(4)));
		dt.setDgEnerKwh(getFloatFromIeee754(getBytes(4)));
		dt.setBattEnerKwh(getFloatFromIeee754(getBytes(4)));
		dt.setSolEnerKwh(getFloatFromIeee754(getBytes(4)));
		dt.setWinEnerKwn(getFloatFromIeee754(getBytes(4)));
		dt.setLoad1Kwh(getFloatFromIeee754(getBytes(4)));
		dt.setLoad2Kwh(getFloatFromIeee754(getBytes(4)));
		dt.setLoad3Kwh(getFloatFromIeee754(getBytes(4)));
		dt.setLoad4Kwh(getFloatFromIeee754(getBytes(4)));
		dt.setLoad5Kwh(getFloatFromIeee754(getBytes(4)));
		dt.setLoad6Kwh(getFloatFromIeee754(getBytes(4)));
		dt.setTotLoadEnergy(getFloatFromIeee754(getBytes(4)));
		
		LOG.debug("Total Rectifier Energy :" + dt.getTotRectEnergy());
		LOG.debug("I/P Mains Kwh:" + dt.getIpMainKwh());
		LOG.debug("DG Energy Kwh:" + dt.getDgEnerKwh());
		LOG.debug("Battery Energy Kwh:" + dt.getBattEnerKwh());
		LOG.debug("Solar Energy Kwh:" + dt.getSolEnerKwh());
		LOG.debug("Wind Energy Kwh:" + dt.getWinEnerKwn());
		LOG.debug("Load 1 Kwh:" + dt.getLoad1Kwh());
		LOG.debug("Load 2 Kwh:" + dt.getLoad2Kwh());
		LOG.debug("Load 3 Kwh:" + dt.getLoad3Kwh());
		LOG.debug("Load 4 Kwh:" + dt.getLoad4Kwh());
		LOG.debug("Load 5 kwh:" + dt.getLoad5Kwh());
		LOG.debug("Load 6 Kwh:" + dt.getLoad6Kwh());
		LOG.debug("Total Load Energy :" + dt.getTotLoadEnergy());

		LOG.debug("Future Data of RS485 (Modbus)");
		LOG.debug("Termination of String :" + getStringFromBytes(getBytes(1)));
	}
	
	private String getStringFromBytes(byte[] bs) {
		return new String(bs);
	}

	private int getIntegerFromBytes(byte[] bs) {
		return new BigInteger(bs).intValue();
	}
	
	private float getFloatFromBytes(byte[] bs) {
		return new Float(new BigInteger(bs).intValue())/10;
	}

	private float getFloatFromIeee754(byte[] bs) {
		return Float.intBitsToFloat(new BigInteger(bs).intValue());
	}

	private String getCommunicationMode(String mode) {

		switch(mode)
		{
		case "0" :	return "GPRS or SMS";
		case "1" :	return "GPRS";
		case "2" :	return "SMS";
		case "3" :	return "GPRS and SMS ";
		}

		return "";
	}

	private String getAlarms(byte[] bytes) {

		BigInteger bi = new BigInteger(bytes);
		
		String result = "";
		for(int i=0; i<48; i++)
		{
			if(bi.testBit(i))
			{
				switch(i+1+"")
				{
				case "1" :	result = result + "FIRE & SMOKE" +",";
							break;

				case "2" :	result = result + "DOOR OPEN" +",";
							break;
	
				case "3" :	result = result + "MAINS FAIL" +",";
							break;
				
				case "4" :	result = result + "GEN FAIL TO START" +",";
							break;

				case "5" :	result = result + "GEN FAILED" +",";
							break;

				case "6" :	result = result + "GEN EMERGENCY STOP" +",";
							break;

				case "7" :	result = result + "GEN ALTERNATOR FAIL" +",";
							break;

				case "8" :	result = result + "HCT ALARM" +",";
							break;

				case "9" :	result = result + "BATTERY LVD" +",";
							break;

				case "10" :	result = result + "SINGLE RECTIFIER FAIL" +",";
							break;

				case "11" :	result = result + "GEN IDLE RUN" +",";
							break;

				case "12" :	result = result + "GEN FAIL TO STOP" +",";
							break;

				case "13" :	result = result + "LLOP" +",";
							break;

				case "14" :	result = result + "MULTIPLE RECTIFIER FAIL" +",";
							break;

				case "15" :	result = result + "BATTERY FUSE FAIL" +",";
							break;

				case "16" :	result = result + "LOAD FUSE FAIL" +",";
							break;

				case "17" :	result = result + "AC1 FAIL" +",";
							break;

				case "18" :	result = result + "AC2 FAIL" +",";
							break;

				case "19" :	result = result + "NEUTRAL FAIL" +",";
							break;

				case "21" :	result = result + "GEN OVERLOAD" +",";
							break;

				case "22" :	result = result + "GEN MAX RUN TIME OVER" +",";
							break;

				case "23" :	result = result + "ALT FAILED" +",";
							break;

				case "24" :	result = result + "GEN BATTERY LOW" +",";
							break;

				case "25" :	result = result + "LOW FUEL LEVEL" +",";
							break;
				
				case "26" :	result = result + "ROOM TEMP HIGH" +",";
							break;

				case "30" :	result = result + "GEN FREQ NOT MATCHING" +",";
							break;

				case "31" :	result = result + "TAMPERING" +",";
							break;								
				}
			}			
		}
		if(result.endsWith(","))
			result = result.substring(0, result.length()-1);	
		
		return result;
	}

	private String getStatus(byte[] bytes) {

		BigInteger bi = new BigInteger(bytes);

		String result = "";
		for(int i=0; i<48; i++)
		{
			if(bi.testBit(i))
			{
				switch(i+1+"")
				{
				case "1" :	result = result + "Site on Battery" +",";
							break;

				case "2" :	result = result + "Load on Mains" +",";
							break;
	
				case "3" :	result = result + "MLoad on Gen" +",";
							break;
				
				case "4" :	result = result + "Gen Healthy" +",";
							break;

				case "5" :	result = result + "Mains OK" +",";
							break;

				case "6" :	result = result + "SVR OK" +",";
							break;	
							
				case "7" :	result = result + "Gen Running" +",";
							break;	

				case "8" :	result = result + "IIPMS Auto Mode" +",";
							break;	

				case "9" :	result = result + "IIPMS Manual Mode" +",";
							break;	

				case "10" :	result = result + "Dynamic Load Shift" +",";
							break;	

				case "11" :	result = result + "EB Healthy" +",";
							break;	

				case "12" :	result = result + "Gen auto Mode" +",";
							break;	
							
				case "13" :	result = result + "Gen Manual Mode" +",";
							break;					
				}							
			}
		}

		if(result.endsWith(","))
			result = result.substring(0, result.length()-1);	
		
		return result;
	}

	/**
	 * Provide the number of bytes of data requested
	 * @param numOfBytes
	 * @return
	 */
	public byte[] getBytes(int numOfBytes)
	{
		byte[] byteArr = new byte[numOfBytes];

		for(int i=0; i<numOfBytes; i++)
		{
			byteArr[i] = hexBytesArr[currentIndex++];
		}
		return byteArr;
	}

	public byte[] toByteArray(String s) {
		LOG.info("Parsing raw hex data:" + s);
//		return DatatypeConverter.parseHexBinary(s);
		return  null;
	}

	@Override
	public DeviceTelemetry getTelemetryData(String packet) {
		DeviceTelemetry dt = new DeviceTelemetry();
		processPacket(packet,dt);
		return dt;
	}

}