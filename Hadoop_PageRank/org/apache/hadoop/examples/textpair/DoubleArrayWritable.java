package org.apache.hadoop.examples.textpair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DoubleArrayWritable implements Writable{

	public double[] data;
	
	public void set(double[] data){
		this.data = data;
	}
	
	public double[] get(){
		return data;
	}
	
	public void readFields(DataInput in) throws IOException {
		data = new double[WritableUtils.readVInt(in)];
		for(int i=0; i<data.length; i++)
			data[i] = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, data.length);
		for(int i=0; i<data.length; i++)
			out.writeDouble(data[i]);
			//WritableUtils.writeVDouble(out, data[i]);
	}

	@Override
	public String toString() {
		return Arrays.toString(data);
	}	
}