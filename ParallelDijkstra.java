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

    public enum updateCounter{COUNT};
    public static class PDMap extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        
        public void map(IntWritable key, PDNodeWritable value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int sourceID = Integer.parseInt(conf.get("source"));    
            if(key.get() == sourceID) value.setDistance(0);     // set source id's distance to 0
            context.write(key, value);      // pass along the node structure
            
            // For all adjacent list in the node structure, pass the distance + d
            
            int disFromSource = value.getDistance();    // distance of this node from source
            
            MapWritable adjacentlist = value.getAdjacencyList();
            for(Writable neighborid: adjacentlist.keySet()) {
                if(disFromSource >= Integer.MAX_VALUE) continue;  // if distance still infinity (not discovered yet) no need to pass it along
                IntWritable disToNodetmp =(IntWritable) adjacentlist.get(neighborid);
                int disToNode = disToNodetmp.get();
                int totalDistance = disToNode + disFromSource;
                PDNodeWritable node = new PDNodeWritable(-1, totalDistance);    //  creating new node structure to pass along that only contains distance
                context.write((IntWritable) neighborid, node);
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
            if(minimumDistance < node.getDistance()) {
                node.setDistance(minimumDistance);
                context.getCounter(updateCounter.COUNT).increment(1);
            }
            context.write(key, node);
        }
    }


    public static class outputMap extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        public void map(IntWritable key, PDNodeWritable value, Context context) throws IOException, InterruptedException {
            if(value.getDistance() < Integer.MAX_VALUE) context.write(key, value);  // if distance is not infinity pass it along
        }

    } 

    public static class outputReduce extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context) throws IOException, InterruptedException {
            PDNodeWritable node = new PDNodeWritable();
            for(PDNodeWritable v: values) {
                node = v;
            }
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
        job.setMapperClass(outputMap.class);
        job.setReducerClass(outputReduce.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Getting parameters from command line
        String input = args[0];        
        String output = args[1];
        String sourceID = args[2];
        int iterations = Integer.parseInt(args[3]);
        String options = args[4];
        
        String temporaryPath = "/user/hadoop/pd/tmp";
        
        List<String> paths = new ArrayList<String>();
        FileSystem hdfs = FileSystem.get(conf);
        // setting configurations
        conf.set("options",options);
        conf.set("source", sourceID);

        // Preprocessing
        Job preprocess = PDPreProcess.getPreProcess(conf, input, temporaryPath);
        preprocess.waitForCompletion(true);
        paths.add(temporaryPath);


        String tempInputPath = temporaryPath;
        String tempOutputPath = "";
        if(iterations > 0 ){
            for(int i = 0; i < iterations; i++) {
                tempOutputPath = temporaryPath + i;
                Job dijk = iterateDijkstra(conf, tempInputPath, tempOutputPath);
                dijk.waitForCompletion(true);
                tempInputPath = tempOutputPath;
                paths.add(tempOutputPath);
            }
        }
        else {
            // iterate until no more changes
            long changes = 1L;
            int k = 0;
            while(changes != 0) {
                tempOutputPath = temporaryPath + k;
                k++;
                Job dijk = iterateDijkstra(conf, tempInputPath, tempOutputPath);
                dijk.waitForCompletion(true);
                tempInputPath = tempOutputPath;
                paths.add(tempOutputPath);
                changes = dijk.getCounters().findCounter(updateCounter.COUNT).getValue();
                System.out.println("Total Updates: "+ changes);
            }
        
        }

        // Try to output
        Job out = output(conf, tempOutputPath, output);
        out.waitForCompletion(true);


        for(String path: paths){
            Path p = new Path(path);
            hdfs.delete(p, true);
        }
    }

}