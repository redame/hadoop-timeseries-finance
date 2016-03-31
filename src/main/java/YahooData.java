
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * 
 *  This class is used to parse and format the Yahoo data.
 *  
 *  Parsing different data file formats allows for taking output from one map-reduce
 *  and inputing the results into another. 
 *  
 * The YahooData class:
 * 
 * 1) Parses a line of the Yahoo finance CSV file into its data components. This is done via a configurable
 * template or via some fixed formats. See the methods parse_template and parse.
 * 
 * 2) Provides the methods to generates the group keys and data for the map-reduce functions.
 * 
 * 3) Provide methods to format a data record back to an output line according to a template. See the methods
 * format_hdr and format.
 * 
 * Note: This works on data down to daily level. Although it is relatively simple to extend this to hourly, minute or even second
 * time intervals. Daily data is primarily used for testing. Reducing daily (or other intervals) data back to the same interval of time
 * should lead to output that exactly matches the input for the TSPeriodReducer.
 * 
 * @author emoran
 * 
 */


public class YahooData {
	public String id = "";
	public long idn = 0;
    public String period = "";
	public String exchange = "";
	public String symbol = "";
	public long date = 0;
	public float open = 0.0f;
	public float high = 0.0f;
	public float low = 0.0f;
	public float close = 0.0f;
	public long volume = 0;
	public float adj_close = 0.0f;
	public Integer icsvcols = 0;
	public String strInFields = "";

	
	// public String segment = ""; // lookup
	
	public static final String TIMESERIES_COL_ID = "id";
	public static final String TIMESERIES_COL_IDN = "in";
	public static final String TIMESERIES_COL_XCHNG = "x";
	public static final String TIMESERIES_COL_SYM = "s";
	public static final String TIMESERIES_COL_DATE = "d";
	public static final String TIMESERIES_COL_TS = "t";
	public static final String TIMESERIES_COL_OPEN = "o";
	public static final String TIMESERIES_COL_CLOSE = "c";
	public static final String TIMESERIES_COL_HIGH = "h";
	public static final String TIMESERIES_COL_LOW = "l";
	public static final String TIMESERIES_COL_VOL = "v";
	public static final String TIMESERIES_COL_ADJCLOSE = "a";
	public static final String TIMESERIES_COL_KEY = "k";
	public static final String TIMESERIES_COL_HDR = "r";
	public static final String TIMESERIES_COL_PER = "p";
	public static final String TIMESERIES_COL_DEFAULT = "";
	
	public static final String TIMESERIES_PERIOD_YEAR = "y";
	public static final String TIMESERIES_PERIOD_QUARTER = "q";
	public static final String TIMESERIES_PERIOD_MONTH = "m";
	public static final String TIMESERIES_PERIOD_WEEK = "w";
	public static final String TIMESERIES_PERIOD_DAY = "d";
	
    // date (and time) formats could be made configurable but the Yahoo data is known. 
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	
	/*
	private static final String DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss";
	private static SimpleDateFormat sdf_t = new SimpleDateFormat(DATE_TIME_FORMAT);
    */
	
	public static final String TIMESERIES_DATE_HDR = "date";
	public static final String TIMESERIES_TS_HDR = "timestamp";

	public static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	private static final String DATE_FORMAT_YYMM = "yyyy-MM";
	private static final String DATE_FORMAT_YY = "yyyy";
	private static final String DATE_FORMAT_MM = "MM";
	private static final String DATE_FORMAT_DD = "dd";
	private static final String WEEK_OF_YEAR = "ww";
	private static final String DATE_FORMAT_DOY = "DDD";
	
	private static SimpleDateFormat sdf_ym = new SimpleDateFormat(DATE_FORMAT_YYMM);
	private static SimpleDateFormat sdf_y = new SimpleDateFormat(DATE_FORMAT_YY);
	private static SimpleDateFormat sdf_m = new SimpleDateFormat(DATE_FORMAT_MM);
	private static SimpleDateFormat sdf_d = new SimpleDateFormat(DATE_FORMAT_DD);
	private static SimpleDateFormat sdf_w = new SimpleDateFormat(WEEK_OF_YEAR);
	private static SimpleDateFormat sdf_dy = new SimpleDateFormat(DATE_FORMAT_DOY);
	
	private static DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
	

	public static Integer getYearNum(long lDate)
	{
		return Integer.parseInt(getYear(lDate));
	}
	
	public static Integer getQtrNum(long lDate)
	{
		Integer mi = getMonthNum(lDate);
		Integer qi = 1;
		if (mi > 3 && mi <= 6)
			qi = 2;
		else
		  if (mi > 6 && mi <= 9)
			qi = 3;
		  else 
			if (mi > 9 && mi <= 12)
				qi = 4;
		
	  return qi;
	}
	
	public static Integer getMonthNum(long lDate)
	{
		return Integer.parseInt(sdf_m.format(lDate));	
	}
	
	public static Integer getWeekNum(long lDate)
	{
		return  Integer.parseInt(sdf_w.format(lDate));
	}
	
	public static Integer getDayNum(long lDate)
	{
		return  Integer.parseInt(sdf_dy.format(lDate));
	}
	
	public static String getYear(long lDate) {
		return sdf_y.format(lDate);	
	}
	
	public static String getQtr(long lDate) {
		Integer qi = getQtrNum(lDate);
		return String.format("%02d", qi);
	}
	
	public static String getMonth(long lDate) {
		return sdf_m.format(lDate);
	}
	
	public static String getWeek(long lDate) {
		return sdf_w.format(lDate);
	}
	
	public static String getDay(long lDate) {
		return sdf_d.format(lDate);
	}
	
	
	public static String getYearMonth(long lDate) {

		return sdf_ym.format(lDate);

	}
	
	public static String getYearDay(long lDate) {

		return getYear(lDate) + "-" + sdf_dy.format(lDate);

	}
	
	public static String getYearWeek(long lDate) {
		String sy = getYear(lDate);	
		String sw = getWeek(lDate);
		String sm = getMonth(lDate);
		if (sm.equals("12") && sw.equals("01"))
		{
		// increment the year
		Integer yi = Integer.parseInt(sy);
		yi++;
		sy = String.format("%04d", yi);
		}
		
		return String.format("%s-%s",  sy, sw);
	}
	
	
	public static String getYearQtr(long lDate) {
		return String.format("%s-%s", getYear(lDate), getQtr(lDate));
	}

	public static String getDate(long lDate) {

		return sdf.format(lDate);
		
	}
	
	public static long getLongDate(String strDate) throws ParseException
	{
		return sdf.parse(strDate).getTime();
	}
	
	
	public static Integer getPeriodNum(String strPeriod, long lDate)
	{
	Integer pnum = -1;
	
	switch (strPeriod)
	{
	case TIMESERIES_PERIOD_YEAR:
		pnum = getYearNum(lDate);
	break;
	
	case TIMESERIES_PERIOD_QUARTER:
		pnum = getQtrNum(lDate);
	break;
	
	case TIMESERIES_PERIOD_MONTH:
		pnum = getMonthNum(lDate);
	break;
	
	case TIMESERIES_PERIOD_WEEK:
		pnum = getWeekNum(lDate);
	break;
	
	case TIMESERIES_PERIOD_DAY:
		pnum = getDayNum(lDate);
	break;
	
	case "":
		pnum = getMonthNum(lDate);
	break;
	}
	
	return pnum;
	}
	
	
	public String generatePeriodKey(String strPeriod)
	{
		String strPeriodKey = "";
		
		switch (strPeriod)
		{
		case TIMESERIES_PERIOD_YEAR:
			strPeriodKey = getYear(date);
		break;
		
		case TIMESERIES_PERIOD_QUARTER:
			strPeriodKey = getYearQtr(date);
		break;
		
		case TIMESERIES_PERIOD_MONTH:
			strPeriodKey = getYearMonth(date);
		break;
		
		case TIMESERIES_PERIOD_WEEK:
			strPeriodKey = getYearWeek(date);
		break;
		
		case TIMESERIES_PERIOD_DAY:
			strPeriodKey = getYearDay(date);
		break;
		
		case "":
			strPeriodKey = getYearMonth(date);
		break;
		
		default:
			strPeriodKey = "Unknown Period";
		}
		
		return strPeriodKey;
	}
	
	public int setCol(String strName, String strValue)
	{
	int ret = 0;
	strValue = strValue.trim();
	
	try {
	
	switch (strName)
	{
	case TIMESERIES_COL_IDN:
	  this.idn =  Long.valueOf(strValue);
	break;
	
	case TIMESERIES_COL_ID:
		this.id = strValue;
	break;
	
	case TIMESERIES_COL_PER:
		this.period = strValue;
	break;
	
	case TIMESERIES_COL_XCHNG:
		this.exchange = strValue;
	break;
		
	case TIMESERIES_COL_SYM:
		this.symbol = strValue;
	break;
	
	case TIMESERIES_COL_DATE:
		this.date = getLongDate(strValue);
	break;
	
	case TIMESERIES_COL_TS:
		this.date = Long.valueOf(strValue);
	break;
	
	case TIMESERIES_COL_OPEN:
		if (strValue.equals(""))
			this.open = 0.0f;
		else
		    this.open = Float.valueOf(strValue);
	break;
	
	case TIMESERIES_COL_HIGH:
		if (strValue.equals(""))
			this.high = 0.0f;
		else
		this.high = Float.valueOf(strValue);
	break;
	
	case TIMESERIES_COL_LOW:
		if (strValue.equals(""))
			this.low = 0.0f;
		else
		this.low = Float.valueOf(strValue);
	break;
	
	case TIMESERIES_COL_CLOSE:
		if (strValue.equals(""))
			this.close = 0.0f;
		else
		this.close = Float.valueOf(strValue);
	break;
	
	case TIMESERIES_COL_VOL:
		if (strValue.equals(""))
			this.volume = 0;
		else
		this.volume = Long.valueOf(strValue);
	break;
	
	case TIMESERIES_COL_ADJCLOSE:
	case TIMESERIES_COL_DEFAULT: //default
		if (strValue.equals(""))
			this.adj_close = 0.0f;
		else
		this.adj_close = Float.valueOf(strValue);
	break;
	
	default:
		ret = -2;
	break;
	}
	
	} catch (ParseException e) {
		e.printStackTrace();
		return -1;
	} 
	
	return ret;
	}
	
	public String getCol(String strName)
	{
		// Input columns plus also some generated ones.
	String strValue = "";
	
	strName = strName.trim();
	
	switch (strName)
	{
	//i, idn, k, r and p are useful for debug.
	case TIMESERIES_COL_IDN:
		strValue = String.format("%d", this.idn);
	break;
	
	case TIMESERIES_COL_ID:
		strValue = this.id;
	break;
	
	case TIMESERIES_COL_KEY:
		if (this.exchange.equals("") == false)
		  strValue = this.exchange + "-";
		if (this.symbol.equals("") == false)
		  strValue += this.symbol + "-";
		strValue += this.period;
	break;
	
	case TIMESERIES_COL_HDR: //header 
		if (this.exchange.equals("") == false)
		  strValue = this.exchange;
		if (this.symbol.equals("") == false)
		{
		if (strValue.equals("") == false)
		  strValue += "-";
		strValue += this.symbol;
		}
	break;
	
	case TIMESERIES_COL_PER:
		strValue = this.period;
	break;
	
	case TIMESERIES_COL_XCHNG:
	  strValue = this.exchange;	
	break;
	
	case TIMESERIES_COL_SYM:
		  strValue = this.symbol;
	break;
		
	case TIMESERIES_COL_DATE:
		strValue = getDate(date);
	break;
	
	case TIMESERIES_COL_TS: //default
		strValue = String.format("%d", this.date);
	break;
	
	case TIMESERIES_COL_OPEN:
		strValue = String.format("%.6f", this.open);
	break;
	
	case TIMESERIES_COL_HIGH:
		strValue = String.format("%.6f", this.high);
	break;
	
	case TIMESERIES_COL_LOW:
		strValue = String.format("%.6f", this.low);
	break;
	
	case TIMESERIES_COL_CLOSE:
		strValue = String.format("%.6f", this.close);
	break;
	
	case TIMESERIES_COL_VOL:
		strValue = String.format("%d", this.volume);
	break;
	
	case TIMESERIES_COL_ADJCLOSE:
	case TIMESERIES_COL_DEFAULT:	
		strValue = String.format("%.6f", this.adj_close);
	break;
	
	default:
		strValue = "";
	break;
	}
	
	return strValue;
	}
	
	public static YahooData parse_template(String csvRow, String strCsvCols) {

		// Parses each line using a template specified by a string. Each column/field is specified by a
		// character abbreviation. The expected character abbreviations are in the SetCol method.
		YahooData rec = null;
		
		
		String[] template = strCsvCols.split(",");
		String[] values = csvRow.split(",");
		
		int ret = 0;
		
		if (template.length == values.length)
		{
		rec = new YahooData();
		rec.strInFields = strCsvCols.trim();
		rec.icsvcols = template.length;
			
		for (int i = 0; i < template.length && ret == 0; i++)	
		  {
			ret = rec.setCol(template[i], values[i]);	
		  }
		
		if (ret != 0)
		  rec = null;
			
		}
		
		return rec;
	}

	
	public static YahooData parse(String csvRow) {
/* This version of parse infers the structure of the data based upon the number of columns/fields 
 in the file. Generally this is a bad idea as it provides numerous sources of error. However,
 this is just a sample program and can be convenient for quickly testing various formats without specifying a template.
*/		
		YahooData rec = new YahooData();

		String[] values = csvRow.split(",");

		if (values.length <= 1 || values.length > 9 || 
			(values.length <= 6 && values.length >= 4)) {
			return null;
		}

		int ret = 0;
		rec.icsvcols = values.length;
		
		switch (rec.icsvcols)
		{
		// Limited number of formats are expected.
		case 9:
			ret = rec.setCol(TIMESERIES_COL_XCHNG, values[0]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_SYM, values[1]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_DATE, values[2]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_OPEN, values[3]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_HIGH, values[4]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_LOW, values[5]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_CLOSE, values[6]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_VOL, values[7]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_ADJCLOSE, values[8]);
			rec.strInFields = "x,s,d,o,h,l,c,v,a";
		break;
		
		case 8:
			ret = rec.setCol(TIMESERIES_COL_SYM, values[0]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_DATE, values[1]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_OPEN, values[2]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_HIGH, values[3]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_LOW, values[4]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_CLOSE, values[5]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_VOL, values[6]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_ADJCLOSE, values[7]);
			rec.strInFields = "s,d,o,h,l,c,v,a";
		break;
		
		case 7:
			ret = rec.setCol("d", values[0]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_OPEN, values[1]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_HIGH, values[2]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_LOW, values[3]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_CLOSE, values[4]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_VOL, values[5]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_ADJCLOSE, values[6]);
			rec.strInFields = "d,o,h,l,c,v,a";
		break;
		
				
		// Either already map-reduce(ed) data or tic data.
		case 4:
			ret = rec.setCol(TIMESERIES_COL_SYM, values[0]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_DATE, values[1]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_VOL, values[2]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_ADJCLOSE, values[3]);
			rec.strInFields = "s,d,v,a";
		break;
		
		case 3:
			ret = rec.setCol(TIMESERIES_COL_DATE, values[0]);
			if (ret == 0)
			    ret = rec.setCol(TIMESERIES_COL_VOL, values[1]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_ADJCLOSE, values[2]);
			rec.strInFields = "d,v,a";
		break;
		
		case 2:
			ret = rec.setCol(TIMESERIES_COL_DATE, values[0]);
			if (ret == 0)
				ret = rec.setCol(TIMESERIES_COL_ADJCLOSE, values[1]);
			rec.strInFields = "d,a";
		break;		
		}
		
		if (ret != 0)
		  rec = null;

		return rec;

	}
	
	//exchange,stock_symbol.
	public static String getFullColName(String strName)
	{
	String strValue = "";
	
	strName = strName.trim();
	
	switch (strName)
	{
	case TIMESERIES_COL_IDN:
		strValue = "Id Num";
	break;
		
	case TIMESERIES_COL_ID:
		strValue = "Id";
	break;
	
	case TIMESERIES_COL_KEY:
		strValue = "Group Key";
	break;
	
	case TIMESERIES_COL_HDR:
		strValue = "Header Key";
	break;
	 
	case TIMESERIES_COL_PER:
		strValue = "Period Key";
	break;
	
	case TIMESERIES_COL_XCHNG:
	  strValue = "Exchange";	
	break;
	
	case TIMESERIES_COL_SYM:
		  strValue = "Stock_Symbol";
	break;
		
	case TIMESERIES_COL_DATE:
		strValue = "Date";
	break;
	
	case TIMESERIES_COL_TS: //default
		strValue = "Timestamp";
	break;
	
	case TIMESERIES_COL_OPEN:
		strValue = "Open";
	break;
	
	case TIMESERIES_COL_HIGH:
		strValue = "High";
	break;
	
	case TIMESERIES_COL_LOW:
		strValue = "Low";
	break;
	
	case TIMESERIES_COL_CLOSE:
		strValue = "Close";
	break;
	
	case TIMESERIES_COL_VOL:
		strValue = "Volume";
	break;
	
	case TIMESERIES_COL_ADJCLOSE:
	case TIMESERIES_COL_DEFAULT:	
		strValue = "Adj Close";
	break;
	
	default:
		strValue = "Unknown";
	break;
	}
	
	return strValue;
	}
	
	public static String format_hdr(String template)
	{
		String line = "";
		template = template.toLowerCase();
		String[] outflds = template.split(",");
		int l =  outflds.length;
		
		for (int i = 0; i < l; i++)
		{
		if (i == (l - 1))
		  line += getFullColName(outflds[i]);
		else  
			line += getFullColName(outflds[i]) + ",";
		}
		
		return line;	
	}
	
	public static String format(YahooData rec, String template)
	{
		String line = "";
		template = template.toLowerCase();
		String[] outflds = template.split(",");
		int l =  outflds.length;
		
		for (int i = 0; i < l; i++)
		{
		if (i == (l - 1))
		  line += rec.getCol(outflds[i]);
		else  
			line += rec.getCol(outflds[i]) + ",";
		}
		
		return line;
	}
}
