package cn.nci.jc5b.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFMapReduceCore {

    public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
        
        private final Text one = new Text("1");
        private Text label = new Text();
        
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            label.set(tokenizer.nextToken().split(":")[0]);
            context.write(label, one);
        }
    }
    
    public static class IDFReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text label = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            if (values == null) {
                return;
            }
            
            int fileCount = 0;
            for (Text value : values) {
                fileCount += Integer.parseInt(value.toString());
            }
            
            label.set(String.join(":", key.toString(), "!"));
            
            int totalFileCount = Integer.parseInt(context.getProfileParams()) - 1;
            double idfValue = Math.log10(1.0 * totalFileCount / (fileCount + 1));
            
            context.write(label, new Text(String.valueOf(idfValue)));
        }
    }
}
