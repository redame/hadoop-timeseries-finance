import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*  Class to store and sort data from map to reduce.
 * 
 * This data point class causes data to be sorted by date/time. This is used to reduce the data to
 * weekly, monthly, quarterly and yearly openings and closings.
 */
public class TSData implements WritableComparable<TSData> {
//public class TSData  implements Writable, Comparable<TSData> {
		
		public long lDateTime = 0;
		//public float fValue;
		public float fOpen = 0.0f;
		public float fHigh = 0.0f;
		public float fLow = 0.0f;
		public float fClose = 0.0f;
		public long lVolume = 0;
		public float fAdj_Close = 0.0f;
		
		@Override
		public String toString() {
		
			YahooData ydata = new YahooData();
			
			  ydata.date = lDateTime;
			  ydata.open = fOpen;	
			  ydata.high = fHigh;
			  ydata.low =  fLow;
			  ydata.close = fClose;
			  ydata.volume = lVolume;
			  ydata.adj_close = fAdj_Close;
	          return YahooData.format(ydata, "d,o,h,l,c,v,a");
		}
		
		

		public String getDateTime() {

			return YahooData.sdf.format(lDateTime);

		}
		

		public void copy(TSData source) {

			this.lDateTime = source.lDateTime;
			this.fOpen = source.fOpen;
			this.fHigh = source.fHigh;
			this.fLow = source.fLow;
			this.fClose = source.fClose;
			this.lVolume = source.lVolume;
			this.fAdj_Close = source.fAdj_Close;

		}

		@Override
		public void readFields(DataInput datain) throws IOException {

			this.lDateTime = datain.readLong();
			this.fOpen = datain.readFloat();
			this.fHigh = datain.readFloat();
			this.fLow = datain.readFloat();
			this.fClose = datain.readFloat();
			this.lVolume = datain.readLong();
			this.fAdj_Close = datain.readFloat();
			
		}

		public static TSData read(DataInput datain) throws IOException {

			TSData ts = new TSData();
			ts.readFields(datain);
			return ts;

		}
		
		@Override
		public void write(DataOutput dataout) throws IOException {

			dataout.writeLong(lDateTime);
			dataout.writeFloat(fOpen);
			dataout.writeFloat(fHigh);
			dataout.writeFloat(fLow);
			dataout.writeFloat(fClose);
			dataout.writeLong(lVolume);
			dataout.writeFloat(fAdj_Close);
		}

		
		@Override
		public int compareTo(TSData ts2) {
			if (this.lDateTime < ts2.lDateTime) {
				return -1;
			} else if (this.lDateTime >ts2.lDateTime) {
				return 1;
			}

			//  equal
			return 0;
		}

	}

