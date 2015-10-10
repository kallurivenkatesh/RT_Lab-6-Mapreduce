/**
 * Created by kalluri on 10/10/15.
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class tfidfMain {
    public static String corpusPath;

    public static void main(String[] args) throws IllegalArgumentException,
            ClassNotFoundException, IOException, InterruptedException {

        corpusPath = args[0];

        Configuration conf = new Configuration();

        Job job = new Job(conf, "TF");

        job.setJarByClass(tfidfMain.class);

        runJob1(job, args[0], args[1]);

        job = new Job(conf, "IDF");

        job.setJarByClass(tfidfMain.class);

        runJob2(job, args[1], args[2]);

        job = new Job(conf, "IDF");

        job.setJarByClass(tfidfMain.class);

        runJob3(job, args[2], args[3]);

    }

    private static void runJob1(Job job, String inputPath, String outputPath)
            throws IllegalArgumentException, IOException,
            ClassNotFoundException, InterruptedException {

        System.out.println("Starting job 2...");
        System.out.println("Input: " + inputPath);
        System.out.println("output: " + outputPath);

        Path inputputDir = new Path(inputPath);
        Path outputDir = new Path(outputPath);

        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(mapperTf.class);
        job.setReducerClass(reducerTf.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, inputputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }

    private static void runJob2(Job job, String inputPath, String outputPath)
            throws IllegalArgumentException, IOException,
            ClassNotFoundException, InterruptedException {

        System.out.println("Starting job 2...");
        System.out.println("Input: " + inputPath);
        System.out.println("output: " + outputPath);

        Path inputputDir = new Path(inputPath);
        Path outputDir = new Path(outputPath);

        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(mapperIdf.class);
        job.setReducerClass(reducerIdf.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, inputputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);

    }

    private static void runJob3(Job job, String inputPath, String outputPath)
            throws IllegalArgumentException, IOException,
            ClassNotFoundException, InterruptedException {

        System.out.println("Starting job 3...");
        System.out.println("Input: " + inputPath);
        System.out.println("output: " + outputPath);

        Path inputputDir = new Path(inputPath);
        Path outputDir = new Path(outputPath);

        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(mapperTfidf.class);
        job.setReducerClass(reducerTfidf.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, inputputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);

    }

}
