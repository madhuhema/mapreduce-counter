import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Main(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] inputs) throws Exception {
        List<String> paths = parseInputs(inputs);
        if (Objects.isNull(paths)) {
            System.exit(0);
        }
        String input = paths.get(0);
        String output = paths.get(1);
        String skip = paths.get(2);
        Job job = Job.getInstance(getConf(), "wordcount");
        if (!skip.isEmpty()) {
            job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            job.getConfiguration().set("wordcount.skip.value", skip);
        }
        job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private List<String> parseInputs(String[] userInputs) {
        List<String> inputs = Arrays.asList(userInputs);
        List<String> parsedOutputs = new ArrayList<>(3);
        if (inputs.size() % 2 == 0) {
            if (inputs.contains("-i")) {
                int i = inputs.indexOf("-i") + 1;
                parsedOutputs.add(0, inputs.get(i));
            } else {
                parsedOutputs.add(0, "inputs");
            }
            if (inputs.contains("-o")) {
                int i = inputs.indexOf("-o") + 1;
                parsedOutputs.add(1, inputs.get(i));
            } else {
                parsedOutputs.add(1, "outputs");
            }
            if (inputs.contains("-skip")) {
                int i = inputs.indexOf("-skip") + 1;
                parsedOutputs.add(2, inputs.get(i));
            } else {
                parsedOutputs.add(2, "skip");
            }
            return parsedOutputs;
        }
        System.out.println("wrong arguments");
        System.out.println("Example output: javac Main -i inputs -o -skip stopwords");
        return null;
    }
}
