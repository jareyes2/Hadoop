package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	  String line = value.toString().toLowerCase();
	  String temp = null;
	  boolean firstWord = true;
      for (String word : line.split("\\W+")){
    	  if(word.length() > 0){
    		  if(!firstWord){
    			  context.write(new Text(temp + "," + word), new IntWritable(1));
    		  }
    		  firstWord = false;
    		  temp = word;
    	  }
      }
    
  }
}
