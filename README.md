# hadoop-timeseries-finance
Using hadoop with timeseries data downloaded from yahoo finance.

This java program optionally downloads and reduces historical data from yahoo finance.

Daily data is reduced to weekly, monthly, quarterly and yearly data. Additionaly various timeseries attributes can be selected for such as lowest low, lowest closing, highest high, etc.

A number of command line options and properties are provided to explore the configuration capabilities of the hadoop map-reduce framework.

Although this is written for yahoo finance the configurable input and output as well as the period reductions can be applied to almost any timeseries data.

One of the interesting aspects relating to cognition is what can be characterized as the "moment of perception". Changing the timescales of timeseries data is a form of fractalization of the data. This naturally allows for lower timescale noise to be filtered out and trends to be discovered/perceived. In the case of yahoo finance data these trends only begin to emerge at the monthly level and become much more defined at quarterly and yearly level.

Usage:

properties controlling operation of program are located in resources tsconfig.xml
In addition properties can be set via the command line.

in tsconfig.xml the property timeseries.yahoo_download_xml sets the xml file to be used that specifies what ticker symbol data to download.
This is currently set to tracking2.xml

Command line interface properties are handled in the source file TSConfProperties.java file. 

Properties and command line options are available for specifying input and output colums to and from the csv files, sorting, downloading and copying files and many others to control the 
operation of the program by data.

In it simplest form all that is needed is the specification of input and output directory for the csv files and
processed data.