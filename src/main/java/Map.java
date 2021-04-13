import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable intWriter = new IntWritable(1);
    private Text word = new Text();
    private String input;
    private Set<String> patternsToSkip = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getInputSplit() instanceof FileSplit) {
            this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
        } else {
            this.input = context.getInputSplit().toString();
        }
        if (context.getConfiguration().getBoolean("wordcount.skip.patterns", false)) {
            String filePath = context.getConfiguration().get("wordcount.skip.value");
            readStopWords(filePath);
        }
    }

    private void readStopWords(String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                List<Path> paths = Files.walk(Path.of(path)).collect(Collectors.toList());
                for (Path p : paths) {
                    if (p.toFile().isFile()) {
                        String fileContent = String.join("\n", Files.readAllLines(p));
                        collectStopWords(fileContent);
                    }
                }
            } else {
                String fileContent = String.join("\n", Files.readAllLines(Path.of(path)));
                collectStopWords(fileContent);
            }
        }
    }

    private void collectStopWords(String fileContent) {
        StringTokenizer tokenizer = new StringTokenizer(fileContent);
        while (tokenizer.hasMoreElements()) {
            patternsToSkip.add(tokenizer.nextToken());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text;
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreElements()) {
            String string = (String) tokenizer.nextElement();
            if (string.isEmpty() || patternsToSkip.contains(string)) {
                continue;
            }
            text = new Text(string);
            context.write(text, intWriter);
        }
    }
}
