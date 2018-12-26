package comp9313.proj1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * @Author: Jason
 * @Date: 2018/8/26 23:36
 */
public class Project1 {

    public static class TfIdfMapper extends Mapper<Object, Text, StringPair, DoubleWritable> {

        private final static DoubleWritable one = new DoubleWritable(1);
        private StringPair wordPair = new StringPair();
        private int count = 0;
      	Configuration conf = new Configuration();

        // mapper input: <line, (docId + doc)>
        // mapper output: <(term, docId), tf> and special key: <(term, *), df>
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
//            Configuration conf = new Configuration();
            while (itr.hasMoreTokens()){
                String line = itr.nextToken().toLowerCase();
                String[] token = line.split(" ");
                String docId = token[0];    // pick-up docId from termArray

                HashSet<String> termSet = new HashSet<String>(); // a set to keep track of DF

                // mapper output: <k, v> --> <(term, docId), 1>
                for (int i=1; i<token.length; i++){

                    // if the set contains a term, pass; otherwise keep track the df with the value of 1 (boolean)
                    if (!termSet.contains(token[i])){
                        termSet.add(token[i]);
                        //emit a special key --> <(term, "*"), 1>
                        wordPair.set(token[i], "*");
                        context.write(wordPair, one);
                    }

                    wordPair.set(token[i], docId);
                    context.write(wordPair, one);    // <(term, docId), 1>
                }
                
                count ++; // count the total count of doc lines
            }
        }
        
        // tranfer the number of doc lines to the combiner by set() method of Configuration 
        public void cleanup(Mapper<Object, Text, StringPair, DoubleWritable>.Context context) 
        		throws IOException, InterruptedException{
        	Configuration conf = context.getConfiguration();
        	conf.set("@", String.valueOf(count)); // <name="@", value="count">
        }
    }

    // combiner that represents mini-reduce, and weights are computed
    public static class TfIdfCombiner extends Reducer<StringPair, DoubleWritable, StringPair, DoubleWritable>{
        private DoubleWritable weight = new DoubleWritable();
        private int specialSum = 0;       
        
        // combiner input: <(term1, *), 1>,  <(term1, docId1), 1>, <(term2, docId2), 1> ...
        //                ... <(term2, *), 1>, <(term2, docId1), 1>, <(term2, docId2), 1> ...

        // combining: <(term1, *), specialCount>,  <(term1, docId1), count>, <(term2, docId2), count> ...
        //                ... <(term2, *), specialCount>, <(term2, docId1), count>, <(term2, docId2), count> ...

        // ==> compute tf: count
        // ==> compute df: specialCount / totalDocs
        // ==> compute idf: log10(1/df) = log10(totalDocs / specialCount)
        // ==> compute weight: tf*idf

        // combiner output: <(term1, docId), weight>  in ascending order of docId
        public void reduce(StringPair key, Iterable<DoubleWritable> values, Context context) 
        		throws IOException, InterruptedException{
        	// receive the number of doc lines from mapper
        	Configuration conf = context.getConfiguration();
        	int totalDocs = Integer.parseInt(context.getConfiguration().get("@"));
        	
        	int tf = 0;
            for (DoubleWritable value: values){
                tf += value.get();
            }
            
            String docId = key.getSecond();

            if (!docId.equals("*")){            	
                double idf = Math.log10((double)totalDocs/specialSum);
                weight.set(tf*idf);
                context.write(key, weight);
            } else if (docId.equals("*")){
            	specialSum = tf;
            }
        }
    }

    // partitioner to make sure the mapper output containing the same "term" are partitioned into the same reducer.
    public static class TfIdfPartitioner extends Partitioner<StringPair, DoubleWritable> {
        public int getPartition(StringPair key, DoubleWritable value, int numPartitions){
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class TfIdfReducer extends Reducer<StringPair, DoubleWritable, Text, Text> {
        // reducer input: <(term1, docId1), weight>, <(term2, docId2), weight> ...
        //                ... <(term2, docId1), weight>, <(term2, docId2), weight> ...

        // reducer output: <term1, (docId, weight)>  in ascending order of docId
    	
    	public void reduce(StringPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
    		String result;
    		String docId = key.getSecond();
    		double weight = 0;
    		for (DoubleWritable value: values){
    			weight += value.get();
    		}
    		result = docId + "," + weight;
    		context.write(new Text(key.getFirst()), new Text(result));
    	}
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(Project1.class);
        job.setMapperClass(TfIdfMapper.class);
        job.setCombinerClass(TfIdfCombiner.class);
        job.setPartitionerClass(TfIdfPartitioner.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
