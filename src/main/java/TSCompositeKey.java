import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * The composite key is logically made up of a group key and a value key. The group key is based upon how the time series
 * data is consolidated. It is a string made up of potentially the exchange and/or symbol (optional) and Year and then varies from nothing to 
 * a value for the Quarter, Month, Week or day depending on how data is consolidated. The fields that make up the group key are the strHdrKey
 * and the strPeriodKey.
 * 
 * For example on a yearly consolidation the group key would be SYM-YYYY or for example SPX-2015 or is the symbol is unused just 2015.
 * Similarly is the consolidation is quarterly then the quarters would vary from 01 to 04 and the group key might be something like
 * SPX-2015-01 or simply 2015-01. Months will vary from 01 to 12, Weeks will vary from 01 to 53, and days will vary from 1-366 (if leap year).
 * 
 * The value key can be used to sort any numeric value. This allows for sorting by date or value.
 * 
 * By using a composite key we can have Hadoop do the sorting for us before it is input to the reducer.
 * 
 */
public class TSCompositeKey implements WritableComparable<TSCompositeKey> {
	
	private String strXKey = "";
	private String strSKey = "";
	private String strPeriodKey = "";
	private double dValue = 0;
	private String strInFields = ""; // Put in key to avoid duplication in data.
	
	public void set(String strX, String strS, String strPeriod, double value) {

		this.set(strX, strS, strPeriod, value, "");

	}
	public void set(String strX, String strS, String strPeriod, double value, String strFlds) {

		this.strXKey =  strX;
		this.strSKey = strS;
		this.strPeriodKey = strPeriod;
		this.dValue = value;
		this.strInFields = strFlds;

	}

	public String getXKey()
	{
		return strXKey;
	}
	
	public String getSKey()
	{
		return strSKey;
	}
	
	public String getPeriodKey()
	{
		return strPeriodKey;
	}
	
	public String getHdrKey() {
		if (strXKey.equals(""))
		  return strSKey;
		else {
			return strXKey + "-" + this.strSKey;
		}
	}
	
	public String getGroupKey() {
	 String strHdrKey = getHdrKey();
	  if (strHdrKey.equals(""))
		 return this.strPeriodKey;
	 else
		return strHdrKey + "-" + this.strPeriodKey;
	}

	public double getValue() {
		return this.dValue;
	}	
	
	public String getInFields() {
		return this.strInFields;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {

		this.strXKey = in.readUTF();
		this.strSKey = in.readUTF();
		this.strPeriodKey = in.readUTF();
		this.dValue = in.readDouble();
		this.strInFields = in.readUTF();

	}
	
	public static TSCompositeKey read(DataInput datain) throws IOException {

		TSCompositeKey ts = new TSCompositeKey();
		ts.readFields(datain);
		return ts;

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(strXKey);
		out.writeUTF(strSKey);
		out.writeUTF(strPeriodKey);
		out.writeDouble(this.dValue);
		out.writeUTF(this.strInFields);
	}

	@Override
	public String toString() {
	
	StringBuilder sb = new StringBuilder();
	
	String strHdrKey = this.getHdrKey();
	if (strHdrKey.equals("") == false)
	   sb.append(strHdrKey + ",");
	
	sb.append(strPeriodKey).append(',').append(dValue).toString();
	 
	return sb.toString();
	}
	
	public int compareGroupKeyTo(TSCompositeKey other) {
        int res = 0;
        
        res = this.getHdrKey().compareTo(other.getHdrKey());
		if (res != 0)
		  return res;
		
		return this.strPeriodKey.compareTo(other.strPeriodKey);
	}
	
	@Override
	public int compareTo(TSCompositeKey other) {
        int res = this.compareGroupKeyTo(other);
		if (res != 0)
		  return res;
		
		if (this.dValue == other.dValue)
		  return 0;
		else
			return dValue < other.dValue ? -1 : 1;
	}

	public static class TSKeyComparator extends WritableComparator {
		public TSKeyComparator() {
			super(TSCompositeKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(TSCompositeKey.class,
				new TSKeyComparator());
	}


}
