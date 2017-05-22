package edu.mum.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair>
{
	private Text word_main;
	private Text word_co;
	
	public Text getWord_main() {
		return word_main;
	}
	public Text getWord_co() {
		return word_co;
	}
	public Pair()
	{
		word_main=new Text();
		word_co=new Text();
	}	
	public Pair(Text word_main, Text word_co) 
	{
		super();
		this.word_main = word_main;
		this.word_co = word_co;
	}

	@Override
	public String toString() 
	{
		return "(" + word_main + ", " + word_co + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((word_co == null) ? 0 : word_co.hashCode());
		result = prime * result
				+ ((word_main == null) ? 0 : word_main.hashCode());
		return result;
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
		if (word_co == null) {
			if (other.word_co != null)
				return false;
		} else if (!word_co.equals(other.word_co))
			return false;
		if (word_main == null) {
			if (other.word_main != null)
				return false;
		} else if (!word_main.equals(other.word_main))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		this.word_main.readFields(in);
		this.word_co.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException 
	{
		this.word_main.write(out);
		this.word_co.write(out);

		
	}
	@Override
	public int compareTo(Pair other) 
	{
		if(this.word_main.toString().equals(other.word_main.toString()))
		{
			return this.word_co.toString().compareTo(other.word_co.toString());
		}
		return this.word_main.toString().compareTo(other.word_main.toString());
	}
	
}
