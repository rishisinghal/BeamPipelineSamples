package com.sample.beam.df.shared;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.AvroCoder;


@DefaultCoder(AvroCoder.class)
public class DeviceMessage {

	String sUrgentRFA;		
	float iload;	
	float battey1;		
	int ampereHourOut;		
	String surgeFail	;		
	float irect;		
	String sUrgentAlarm;	
	float tbatt;		
	int psys	;
	String smpsPort;	
	int stateofCharge;		
	float usys;		
	float ibatt;		
	float iload1	;		
	
	String spsPort;	
	String dgb;			
	float mainsEbVoltRN;		
	float mainsEbVoltYN;		
	float mainsEbVoltBN;		
	float freqeb;		
	float sysb;		
	float btsBatt;		
	float genDgVoltRN;		
	float genDgVoltYN;		
	float genDgVoltBN;
	
	public DeviceMessage()
	{
		
	}
	


	public DeviceMessage(String sUrgentRFA, float iload, float battey1, int ampereHourOut, String surgeFail,
			float irect, String sUrgentAlarm, float tbatt, int psys, String smpsPort, int stateofCharge, float usys,
			float ibatt, float iload1, String spsPort, String dgb, float mainsEbVoltRN, float mainsEbVoltYN,
			float mainsEbVoltBN, float freqeb, float sysb, float btsBatt, float genDgVoltRN, float genDgVoltYN,
			float genDgVoltBN) {
		super();
		this.sUrgentRFA = sUrgentRFA;
		this.iload = iload;
		this.battey1 = battey1;
		this.ampereHourOut = ampereHourOut;
		this.surgeFail = surgeFail;
		this.irect = irect;
		this.sUrgentAlarm = sUrgentAlarm;
		this.tbatt = tbatt;
		this.psys = psys;
		this.smpsPort = smpsPort;
		this.stateofCharge = stateofCharge;
		this.usys = usys;
		this.ibatt = ibatt;
		this.iload1 = iload1;
		this.spsPort = spsPort;
		this.dgb = dgb;
		this.mainsEbVoltRN = mainsEbVoltRN;
		this.mainsEbVoltYN = mainsEbVoltYN;
		this.mainsEbVoltBN = mainsEbVoltBN;
		this.freqeb = freqeb;
		this.sysb = sysb;
		this.btsBatt = btsBatt;
		this.genDgVoltRN = genDgVoltRN;
		this.genDgVoltYN = genDgVoltYN;
		this.genDgVoltBN = genDgVoltBN;
	}

	@Override
	public String toString() {
		return "DeviceMessage [sUrgentRFA=" + sUrgentRFA + ", iload=" + iload + ", battey1=" + battey1
				+ ", ampereHourOut=" + ampereHourOut + ", surgeFail=" + surgeFail + ", irect=" + irect
				+ ", sUrgentAlarm=" + sUrgentAlarm + ", tbatt=" + tbatt + ", psys=" + psys + ", smpsPort=" + smpsPort
				+ ", stateofCharge=" + stateofCharge + ", usys=" + usys + ", ibatt=" + ibatt + ", iload1=" + iload1
				+ ", spsPort=" + spsPort + ", dgb=" + dgb + ", mainsEbVoltRN=" + mainsEbVoltRN + ", mainsEbVoltYN="
				+ mainsEbVoltYN + ", mainsEbVoltBN=" + mainsEbVoltBN + ", freqeb=" + freqeb + ", sysb=" + sysb
				+ ", btsBatt=" + btsBatt + ", genDgVoltRN=" + genDgVoltRN + ", genDgVoltYN=" + genDgVoltYN
				+ ", genDgVoltBN=" + genDgVoltBN + "]";
	}

	// "MAINS(EB) Volt (L -N)": "RN = 229; YN = 219; BN = 226"
	void processMainVolt(String data)
	{
		String[] dataArr = data.split(";");
		for (String ele : dataArr)
		{
			if(ele.equalsIgnoreCase("rn"))
				setMainsEbVoltRN(Float.parseFloat(ele));
			else if(ele.equalsIgnoreCase("yn"))
				setMainsEbVoltYN(Float.parseFloat(ele));
			else if(ele.equalsIgnoreCase("bn"))
				setMainsEbVoltBN(Float.parseFloat(ele));		
		}
	}
	
	void processDgVolt(String data)
	{
		String[] dataArr = data.split(";");
		for (String ele : dataArr)
		{
			if(ele.equalsIgnoreCase("rn"))
				setGenDgVoltRN(Float.parseFloat(ele));
			else if(ele.equalsIgnoreCase("yn"))
				setGenDgVoltYN(Float.parseFloat(ele));
			else if(ele.equalsIgnoreCase("bn"))
				setGenDgVoltBN(Float.parseFloat(ele));		
		}
	}

	public String getsUrgentRFA() {
		return sUrgentRFA;
	}

	public void setsUrgentRFA(String sUrgentRFA) {
		this.sUrgentRFA = sUrgentRFA;
	}

	public float getIload() {
		return iload;
	}

	public void setIload(float iload) {
		this.iload = iload;
	}

	public float getBattey1() {
		return battey1;
	}

	public void setBattey1(float battey1) {
		this.battey1 = battey1;
	}

	public int getAmpereHourOut() {
		return ampereHourOut;
	}

	public void setAmpereHourOut(int ampereHourOut) {
		this.ampereHourOut = ampereHourOut;
	}

	public String getSurgeFail() {
		return surgeFail;
	}

	public void setSurgeFail(String surgeFail) {
		this.surgeFail = surgeFail;
	}

	public float getIrect() {
		return irect;
	}

	public void setIrect(float irect) {
		this.irect = irect;
	}

	public String getsUrgentAlarm() {
		return sUrgentAlarm;
	}

	public void setsUrgentAlarm(String sUrgentAlarm) {
		this.sUrgentAlarm = sUrgentAlarm;
	}

	public float getTbatt() {
		return tbatt;
	}

	public void setTbatt(float tbatt) {
		this.tbatt = tbatt;
	}

	public int getPsys() {
		return psys;
	}

	public void setPsys(int psys) {
		this.psys = psys;
	}

	public int getStateofCharge() {
		return stateofCharge;
	}

	public void setStateofCharge(int stateofCharge) {
		this.stateofCharge = stateofCharge;
	}

	public float getUsys() {
		return usys;
	}

	public void setUsys(float usys) {
		this.usys = usys;
	}

	public float getIbatt() {
		return ibatt;
	}

	public void setIbatt(float ibatt) {
		this.ibatt = ibatt;
	}

	public float getIload1() {
		return iload1;
	}

	public void setIload1(float iload1) {
		this.iload1 = iload1;
	}

	public String getDgb() {
		return dgb;
	}

	public void setDgb(String dgb) {
		this.dgb = dgb;
	}

	public float getMainsEbVoltRN() {
		return mainsEbVoltRN;
	}

	public void setMainsEbVoltRN(float mainsEbVoltRN) {
		this.mainsEbVoltRN = mainsEbVoltRN;
	}

	public float getMainsEbVoltYN() {
		return mainsEbVoltYN;
	}

	public void setMainsEbVoltYN(float mainsEbVoltYN) {
		this.mainsEbVoltYN = mainsEbVoltYN;
	}

	public float getMainsEbVoltBN() {
		return mainsEbVoltBN;
	}

	public void setMainsEbVoltBN(float mainsEbVoltBN) {
		this.mainsEbVoltBN = mainsEbVoltBN;
	}

	public float getFreqeb() {
		return freqeb;
	}

	public void setFreqeb(float freqeb) {
		this.freqeb = freqeb;
	}

	public float getSysb() {
		return sysb;
	}

	public void setSysb(float sysb) {
		this.sysb = sysb;
	}

	public float getBtsBatt() {
		return btsBatt;
	}

	public void setBtsBatt(float btsBatt) {
		this.btsBatt = btsBatt;
	}

	public float getGenDgVoltRN() {
		return genDgVoltRN;
	}

	public void setGenDgVoltRN(float genDgVoltRN) {
		this.genDgVoltRN = genDgVoltRN;
	}

	public float getGenDgVoltYN() {
		return genDgVoltYN;
	}

	public void setGenDgVoltYN(float genDgVoltYN) {
		this.genDgVoltYN = genDgVoltYN;
	}

	public float getGenDgVoltBN() {
		return genDgVoltBN;
	}

	public void setGenDgVoltBN(float genDgVoltBN) {
		this.genDgVoltBN = genDgVoltBN;
	}

	public String getSmpsPort() {
		return smpsPort;
	}

	public void setSmpsPort(String smpsPort) {
		this.smpsPort = smpsPort;
	}

	public String getSpsPort() {
		return spsPort;
	}

	public void setSpsPort(String spsPort) {
		this.spsPort = spsPort;
	}		
	
}
