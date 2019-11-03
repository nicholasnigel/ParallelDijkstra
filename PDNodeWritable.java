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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PDNodeWritable implements Writable{
    private int id;
    private int distance;
    private HashMap<Integer, Integer> adjacencyList;     

    public PDNodeWritable(){
        adjacencyList = new HashMap<Integer,Integer>();
    }
    public PDNodeWritable(int id) {
        this();
        this.id = id;
        this.distance = Integer.MAX_VALUE;  //   default should be infinite
    }
    public PDNodeWritable(int id, int distance){
        this(id);
        this.distance = distance;
    }
    /**copy the PDNodewritable Object */
    public PDNodeWritable(PDNodeWritable other) {
        this(other.getId(), other.getDistance());
        adjacencyList.putAll(other.getAdjacencyList());
    }

    public int getId(){
        return this.id;
    }
    public void setId(int id){
        this.id = id;
    }
    public int getDistance(){
        return this.distance;
    }
    public void setDistance(int distance) {
        this.distance = distance;
    }
    public HashMap<Integer, Integer> getAdjacencyList(){
        return this.adjacencyList; 
    }
    public void addToAdjacencyList(int k, int v){
        adjacencyList.put(k,v);
    }
    public void addAllToAdjacency(MapWritable other){
        for(Writable key: other.keySet()) {
            IntWritable value = (IntWritable) other.get(key);
            adjacencyList.put(((IntWritable)key).get(), value.get());
        }
    
    }
    public void putAllToAdjacency(HashMap other) {
        adjacencyList.putAll(other);
    }
    public int adjacencyListSize(){
        return adjacencyList.size();
    }

    
    public void write(DataOutput out) throws IOException {
        // format: <id> <distance> <mapsize> <mapkey1><mapvalue1> .. <mapkeyn><mapvaluen>
        out.writeInt(id);
        out.writeInt(distance);
        out.writeInt(adjacencyListSize());
        for(Entry<Integer, Integer> entry: adjacencyList.keySet()) {
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }
    
    
    public void readFields(DataInput in) throws IOException {
        // format: <id> <distance> <mapsize> <mapkey1><mapvalue1> .. <mapkeyn><mapvaluen>
        id = in.readInt();
        distance = in.readInt();
        int size = in.readInt();
        for(int i = 0 ; i < size ; i++) {
            int k = in.readInt();
            int v = in.readInt();
            adjacencyList.put(k, v);
        }
    }

    /** returns string format */
    @Override
    public String toString() {
        String s = "";
        for(int key: adjacencyList.keySet()) {
            int value = adjacencyList.get(key);
            s += "(" + key+ " "+ value+")";
        }
        s += "distance: " + distance + " id: " + id;
        return s;
    }

}