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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PDPreProcess {
    public enum Test{COUNT};
    // from text format and emit: nodeID -> (neighboroID, distance)
    public static class PreMapper extends Mapper<LongWritable, Text, IntWritable, MapWritable> {
        private HashMap<Integer, MapWritable> store;
        private List<String> line;  // for reading purpose

        public void setup(Context context) throws IOException, InterruptedException {
            store = new HashMap<Integer, MapWritable>();
            line = new ArrayList<String>();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            String[] tokens = content.split("\\s+");
            
            for(int i = 0 ; i < tokens.length ; i++ ) {
                // store until it holds 3 elements:
                if(tokens[i].isEmpty()) continue;
                line.add(tokens[i]);
                if(line.size() < 3) continue;

                // the 3 elements
                int id = Integer.parseInt(line.get(0));
                int neighborid = Integer.parseInt(line.get(1));
                int distance = Integer.parseInt(line.get(2));

                // gettign adjacency list if stored, else createnew
                MapWritable adjlist, neighboradjlist;
                if(store.containsKey(id)) adjlist = store.get(id);
                else{ 
                    adjlist =  new MapWritable();
                    store.put(id, adjlist);
                }

                if(!store.containsKey(neighborid)){     // storing the neighboring
                    neighboradjlist = new MapWritable();
                    store.put(neighborid, neighboradjlist);
                }
                

                adjlist.put(new IntWritable(neighborid), new IntWritable(distance));
                line.clear();
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // from store, just pass everything
            for(Integer id: store.keySet()) {
                IntWritable k = new IntWritable(id);
                MapWritable v = store.get(id);
                context.write(k, v);
                
            }
        }

    }

    public static class PreReducer extends Reducer<IntWritable, MapWritable, IntWritable, PDNodeWritable> {
        private MapWritable comb;

        protected void merge(MapWritable other) {
            for(Writable id: other.keySet()) {
                IntWritable count = (IntWritable) other.get(id);
                comb.put(id, count);
            }
        }

        

        public void reduce (IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
            comb = new MapWritable();
            for(MapWritable v: values){
                merge(v);
            }
            // create node writable now
            PDNodeWritable node = new PDNodeWritable(key.get());
            node.addAllToAdjacency(comb);

            if(key.get() == 5) context.getCounter(Test.COUNT).setValue(node.adjacencyListSize());
            context.write(key, node);
        }
    }
    /**preprocessing job */
    public static Job getPreProcess(Configuration conf, String input, String output) throws Exception {
        Job job = new Job(conf, "Preprocess");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);

        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        return job;
    }

}