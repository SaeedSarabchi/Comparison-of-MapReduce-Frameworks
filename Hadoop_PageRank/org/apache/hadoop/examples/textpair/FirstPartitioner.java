package org.apache.hadoop.examples.textpair;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * the hash partitioner
 * 
 * @author yingyib
 * 
 */
public class FirstPartitioner implements Partitioner<TextPair, Writable> {

        @Override
        public void configure(JobConf job) {
        }

        @Override
        public int getPartition(TextPair key, Writable value, int numPartitions) {
        	int range = 200000/numPartitions;		// calculate range
        	int reducer = 0;			
        	int partitionKey = key.getFirstInt();	// get key number
        	
        	reducer = partitionKey/range;	
        	
        	if(reducer >= numPartitions)
        		return (numPartitions-1);
        	else
        		return reducer;
        }
}
