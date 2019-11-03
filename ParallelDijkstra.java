import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ParallelDijkstra {

    public enum Counter{COUNT};
    public static class PDMap extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        
        public void map(IntWritable key, PDNodeWritable value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int sourceID = Integer.parseInt(conf.get("source"));    
            if(key.get() == sourceID) value.setDistance(0);     // set source id's distance to 0
            if(key.get() == 3) context.getCounter(Counter.COUNT).setValue(value.adjacencyListSize());
            context.write(key, value);      // pass along the node structure
            
            // For all adjacent list in the node structure, pass the distance + d
            
            int disFromSource = value.getDistance();    // distance of this node from source
            
            HashMap<Integer, Integer> adjacentlist = value.getAdjacencyList();
            for(int neighborid: adjacentlist.keySet()) {
                if(disFromSource >= Integer.MAX_VALUE) continue;  // if distance still infinity (not discovered yet) no need to pass it along
                int disToNode = adjacentlist.get(neighborid);
                int totalDistance = disToNode + disFromSource;
                PDNodeWritable node = new PDNodeWritable(-1, totalDistance);    //  creating new node structure to pass along that only contains distance
                context.write(new IntWritable(neighborid), node);
            }

        }
    }

    public static class PDReduce extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context) throws IOException, InterruptedException {
            int minimumDistance = Integer.MAX_VALUE;    // to be swapped with minimum values
            PDNodeWritable node = null;

            for(PDNodeWritable val: values) {

                if(val.getId() != -1) {
                    node = new PDNodeWritable(val); 
                    
                }  // copying structure
                else {
                    int tempDist = val.getDistance();
                    if(tempDist < minimumDistance) minimumDistance = tempDist;                    
                }

            }
            if(minimumDistance < node.getDistance()) node.setDistance(minimumDistance);
            
            context.write(key, node);
        }
    }

    public static Job iterateDijkstra(Configuration conf, String input, String output) throws Exception {
        Job job = new Job(conf, "Dijkstra");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);

        job.setJarByClass(ParallelDijkstra.class);
        job.setMapperClass(PDMap.class);
        job.setReducerClass(PDReduce.class);

        return job;
    }
    
    /**for outputing purpose */
    public static Job output(Configuration conf, String input, String output) throws Exception {
        Job job = new Job(conf, "Dijkstra");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);

        job.setJarByClass(ParallelDijkstra.class);
        job.setMapperClass(PDMap.class);
        job.setReducerClass(PDReduce.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("source", "1");
        FileSystem hdfs = FileSystem.get(conf);
        String input = "/user/hadoop/pd/input";
        String output = "/user/hadoop/pd/output";
        String tempPath = "/user/hadoop/pd/temp";

        Job preprocess = PDPreProcess.getPreProcess(conf, input, tempPath);
        preprocess.waitForCompletion(true);
        System.out.println(preprocess.getCounters().findCounter(PDPreProcess.Test.COUNT).getValue());
        
        Job dijk = output(conf, tempPath, output);
        dijk.waitForCompletion(true);
        System.out.println(dijk.getCounters().findCounter(Counter.COUNT).getValue());
        
        hdfs.delete(new Path(tempPath), true);
        
    }
}