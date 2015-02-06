

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;


public class from_doc_to_seq {
	public from_doc_to_seq()
	{
		
	}
	public static void main(String[] args) throws FileNotFoundException,IOException
	{
		Configuration conf =new Configuration();
		FileSystem fs =FileSystem.get(conf);
		Path input_path=new Path("/user/hadoop11/pa3_small/pa3_test_input");
		Path output_path=new Path("/user/hadoop11/pa3_small/seq_output");
		BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(input_path)));
		
		SequenceFile.Writer writer=new SequenceFile.Writer(fs, conf,output_path,IntWritable.class,Text.class);
	    String line;
	    IntWritable key;
	    Text val;
	    int count=0;
	    while((line=br.readLine())!=null)
	    {
	    	key=new IntWritable(count);
	    	val=new Text(line);
	    	writer.append(key, val);
	    	count+=1;
	    }
	    writer.close();
	}
}