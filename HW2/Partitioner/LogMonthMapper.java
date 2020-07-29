package stubs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LogMonthMapper extends Mapper<LongWritable, Text, Text, Text> {

  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   *
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	 
	  String line = value.toString();
	  int len = line.indexOf(" ");
	  
	  String s;
	  String p = "\\/(\\D*?)\\/";
      Pattern r = Pattern.compile(p);
      Matcher m = r.matcher(line);
	  
      if (m.find( )) {
    	  s = m.group(0);
      }
      else 
    	  throw new IllegalArgumentException("Not Valid Month Extracted");
    
	  context.write(new Text(line.substring(0,len)),new Text (s.substring(1,4)));
	  
  }
}
