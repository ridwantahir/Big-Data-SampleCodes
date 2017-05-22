package edu.mum.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair>{
	private String EventID;
	private String CoEventID;
	
	public Pair(){
		EventID="";
		CoEventID="";
	}
	
	public Pair(String eventid,String coeventid){
		
		this.EventID=eventid;
		this.CoEventID=coeventid;
	}

	public String getEventID() {
		return EventID;
	}

	public String getCoEventID() 
	{
		return CoEventID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((CoEventID == null) ? 0 : CoEventID.hashCode());
		result = prime * result + ((EventID == null) ? 0 : EventID.hashCode());
		return result;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "("+EventID + " ," + CoEventID+")";
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (CoEventID == null) {
			if (other.CoEventID != null)
				return false;
		} else if (!CoEventID.equals(other.CoEventID))
			return false;
		if (EventID == null) {
			if (other.EventID != null)
				return false;
		} else if (!EventID.equals(other.EventID))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		EventID =arg0.readUTF();
		CoEventID=arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
		arg0.writeUTF(EventID);
		arg0.writeUTF(CoEventID);
		
	}

	@Override
	public int compareTo(Pair p)
	{
		if(this.EventID.equals(p.EventID))
		{
			if(this.CoEventID.startsWith("*")) return -1;
			if(p.CoEventID.startsWith("*")) return 1;
			return this.CoEventID.compareTo(p.CoEventID);
			
		}
		return this.EventID.compareTo(p.EventID);
	}
	

}
