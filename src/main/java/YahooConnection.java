import java.net.*;
import java.io.*;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;
import org.apache.commons.configuration.*;
import org.apache.hadoop.fs.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;


/* 
 *  This class is used to make calls to Yahoo finance for historical data.
 * 
 *  Data can be retrieved as byte arrays, strings and/or saved to csv files.
 *  
 *  Downloads can be set up manually or via a xml specified format. The xml format can be used to specify:
 *  The list of symbols/files
 *  start and end dates for historical data. The keyword Now specifies up to current date of call
 *  The Output directory to place the saved .csv files.
 *  Example xml file comes with program.
 *  
 *  File names are derived from the symbol name used to retrieve the symbol.
 *  
 */

public class YahooConnection {
	
	private static final String Base_Hist = "http://ichart.finance.yahoo.com/table.csv?s=";
	private static final String Url_Dates = "&a=[startMonth]&b=[startDay]&c=[startYear]&d=[endMonth]&e=[endDay]&f=[endYear]";
	static TSConfProperties tsConf = null;
	
	static enum eYahooConfig
	{
		eManual,
		eXMLFile,
		eURL
	};
	
	private eYahooConfig eConfig = eYahooConfig.eManual;
	private String strSymbols = "";
	private String strOutputDir = "";
	private String strStartDate = "";
	private String strEndDate = "";
	private String strExtra = "";
    String strURL = "";

	String strOpOutputDir = "";
	String strOpStartDate = "";
	String strOpEndDate = "";
	String strOpExtra = "";
	
	XMLConfiguration XMLConfig = null;
	
	byte[] myDataBuffer = null;
	
	public YahooConnection(org.apache.hadoop.conf.Configuration conf)
	{
		tsConf = new TSConfProperties(conf);	
	}
	
	public eYahooConfig getYahooConfig()
	{
			return eConfig;
	}

	public void setYahooConfig(eYahooConfig eCfg)
	{
		eConfig = eCfg;
	}
	
	public String getSymbols()
	{
        return strSymbols;
	}

	public void setSymbols(String strSym)
	{
       strSymbols = strSym;
       eConfig = eYahooConfig.eManual;
	}

	
	public String getURL()
	{
			return strURL;
	}

	public void setURL(String strU)
	{
			strURL = strU;
			eConfig = eYahooConfig.eURL;
	}

	public String getOutputDir()
	{
			return strOutputDir;
	}
		
	public void setOutputDir(String strOut)
	{
			strOutputDir = strOut;
	}
	

	public String getStartDate()
	{
			return strStartDate; 
	}

	public void setStartDate(String strStartDt)
	{
	strStartDate = strStartDt;
	}

	public String getEndDate()
	{
		return strEndDate; 
	}

	public void setEndDate(String strEndDt)
	{
			strEndDate = strEndDt;
	}
	
	public String getExtra()
	{
			return strExtra;
	}

	public void setExtra(String strEx)
	{
			strExtra = strEx;
	}

	public String getOpOutputDir()
	{
			if (strOutputDir.equals("") == false)
			  return strOutputDir;
			else
			return strOpOutputDir;
	}
		
	
	public String getOpStartDate()
	{
		if (strStartDate.equals("") == false)
		  return strStartDate;
		else
		  return strOpStartDate; 
	}
	
	public String getOpEndDate()
	{
			if (strEndDate.equals("") == false)
			  return strEndDate;
			else
			return strOpEndDate; 
	}
	
	public String OpExtra()
	{
			if (strExtra.equals("") == false)
			  return strExtra;
			else
			return strOpExtra;
	}

	
	public String GetXMLProperty(String strXMLProperty)
	{
		String strValue = "";
		if (eConfig != eYahooConfig.eXMLFile || XMLConfig == null)
			return strValue;

	    strValue = XMLConfig.getString("Settings." + strXMLProperty);
	    
		return strValue;
	}

	
	public int GetXMLConfiguration(String XMLSymbolFile)
	{
        
        
		try {
        	XMLConfig = new XMLConfiguration(XMLSymbolFile);
        	
		    }
        	catch(ConfigurationException cex)
        	{
			cex.printStackTrace();
			return -1;
		    }
        
		eConfig = eYahooConfig.eXMLFile;

		strOpStartDate = GetXMLProperty("StartDate");
		strOpEndDate = GetXMLProperty("EndDate");
		strOpOutputDir = GetXMLProperty("OutputDirectory");
		strOpExtra = GetXMLProperty("Extra");

		return 0;
	}

	
	public String [] GetSymbolList()
	{
		if (eConfig == eYahooConfig.eXMLFile)
		  return XMLConfig.getStringArray("Symbols.Ticker[@symbol]");
		else
		  return GetSymbols().split(",");
	}

	public String GetSymbols()
	{
		StringBuilder sb = new StringBuilder();

		switch (eConfig)
		{
		    case eURL:
		    break;
		    
			case eManual:
				sb.append(strSymbols);
				break;

			case eXMLFile:
				String [] aSymbols = XMLConfig.getStringArray("Symbols.Ticker[@symbol]");
				
				if (aSymbols != null)
				{
				boolean bFirst = true;
				String strVal = "";
				for (int i = 0; i < aSymbols.length; i++)
				  {
					strVal = aSymbols[i];
				   if (bFirst)
					{
						bFirst = false;
						sb.append(strVal);
					}
					else
						sb.append("," + strVal);
				  }
				 }
				
				break;

		}

		return sb.toString();
	}
	
	
	public String BuildURLString(String BaseURL, String strSymbol, String strStartDate, String strEndDate)
	{
		if (strSymbol.equals(null) || strSymbol.equals(""))
			return "";

		String urlstring = BaseURL + strSymbol;
		String urlDates = "";


		if (strStartDate.equals("") == false)
		{
			Date startDate = null;
			Date endDate = new Date();
			try 
			{	
			DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
			startDate = df.parse(strStartDate);
			if (!(strEndDate.equals("") || strEndDate.equalsIgnoreCase("Now")))
			  endDate = df.parse(strEndDate);
						
			if (endDate.before(startDate))
			  return "";
			 } 
		    catch (ParseException e) 
			{
			e.printStackTrace();
			return "";
		    } 
			
			String [] arStartDate = YahooData.sdf.format(startDate).split("-");
			String [] arEndDate = YahooData.sdf.format(endDate).split("-");
			
			String startYear = String.valueOf(arStartDate[0]);
			int startMo = Integer.valueOf(arStartDate[1]) - 1;
			String startMonth = String.valueOf(startMo);
			String startDay = String.valueOf(arStartDate[2]);
			
			String endYear = String.valueOf(arEndDate[0]);
			int endMo = Integer.valueOf(arEndDate[1]) - 1;
			String endMonth = String.valueOf(endMo);
			String endDay = String.valueOf(arEndDate[2]);
			
			urlDates = Url_Dates.toString();
			urlDates = urlDates.replace("[startMonth]", startMonth);
			urlDates =urlDates.replace("[startDay]", startDay);
			urlDates = urlDates.replace("[startYear]", startYear);
			urlDates = urlDates.replace("[endMonth]", endMonth);
			urlDates = urlDates.replace("[endDay]", endDay);
			urlDates = urlDates.replace("[endYear]", endYear);

			urlstring += urlDates;
		}

		return urlstring;
	}

	public String getURLString()
	{
	  if (eConfig == eYahooConfig.eURL)
		{
			return strURL;
		}
	  else
		{
			return BuildURLString(Base_Hist, strSymbols, strStartDate, strEndDate);
		}
	 }

    public String getData()
    {
    	String strBuffer = "";
    	try
    	{
    		strBuffer = new String(myDataBuffer,"UTF-8");
    	}
    	catch  (Exception e)
    	{
    		e.printStackTrace();
    	}
    	
    	return strBuffer;
    }

	
	
	byte [] WebHttpRequest(String uri)
	{
		
		try
	    {
		HttpClient client = new HttpClient();

        GetMethod method = new GetMethod(uri);

        int statusCode = client.executeMethod(method);
        
        if (statusCode != 200) 
	      {
	    	 return null;
	      }
        
        InputStream rstream = method.getResponseBodyAsStream();

        myDataBuffer = IOUtils.toByteArray(new InputStreamReader(rstream));

	    }
	   catch  (Exception e)
	  	 {
	  		e.printStackTrace();
	  		return null;
	  	 }
	      
	      return myDataBuffer;
        
	}
	
	byte [] WebDownloadBytes()
	{
      return WebDownloadBytes(getURLString());
	}
	
	
	byte [] WebDownloadBytes(String uri)
	{
		try
	      {
	      URL url = new URL(uri);
	      
	      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	      int rc = conn.getResponseCode();
	      if ( rc != 200) 
	      {
	    	  System.out.println("Faild Http connection rc=" + String.valueOf(rc));
	    	    throw new IOException(conn.getResponseMessage());
	      }

	      // Get the result into the buffer.
	      myDataBuffer = IOUtils.toByteArray(conn.getInputStream());  
	      conn.disconnect();
	      }
	      catch  (Exception e)
	  	  {
	  		e.printStackTrace();
	  		return null;
	  	  }
	      
	      return myDataBuffer;
	}

	public String WebDownloadString()
	{
     return WebDownloadString(getURLString());
	}

	public String WebDownloadString(String uri)
	{
     WebDownloadBytes(uri);
	 return getData();
	}

	public int WebDownloadFile(String filepath)
	{
     return WebDownloadFile(getURLString(), filepath);
	}

	public int WebDownloadFile(String uri, String filepath)
	{
	WebDownloadBytes(uri);
    
    return SaveFile(filepath);
	}

	public int SaveFile(String filepath)
	{
		if (myDataBuffer == null)
			return 1;
		
		try
		{
		FileSystemPath fsp = new FileSystemPath(filepath, tsConf.conf);
		FSDataOutputStream ds = fsp.create();
		ds.write(myDataBuffer);
		ds.close();
		}
		catch  (Exception e)
    	{
    		e.printStackTrace();
    		return 2;
    	}
		

		return 0;
	}
	
	public int DownloadData(String strXMLFile)
	{
		int ret = GetXMLConfiguration(strXMLFile);
		if (ret == 0)
			ret = DownloadData();
		  
		return ret;
	}
	
	public int DownloadData()
	{
	String [] aSymbols = null;
	String strFilePath = "";
	String strFileName = "";
	int ecnt = 0;
    int ret = 0;
	
	YahooConnection YC = new YahooConnection(tsConf.conf);
	YC.setYahooConfig(eYahooConfig.eManual);
	YC.setStartDate(getOpStartDate());
	YC.setEndDate(getOpEndDate());
	aSymbols = GetSymbolList();
	for (int i = 0; i < aSymbols.length; i++)
	  {
	  strFileName = aSymbols[i];
	  YC.setSymbols(strFileName);	
	  if (strFileName.startsWith("^"))
		strFileName = strFileName.substring(1);
	  strFilePath = getOpOutputDir() + FileSystemPath.separator + strFileName + ".csv";
	  System.out.println("Downloading " + strFilePath);
	  ret = YC.WebDownloadFile(strFilePath);
	  if (ret != 0)
		 ecnt++;
	  }
	
	return ecnt;
   }
}
