import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalCount = 0;
        for (IntWritable count : values) {
            totalCount += count.get();
        }
        context.write(word, new IntWritable(totalCount));
    }
}
