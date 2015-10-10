/**
 * Created by kalluri on 10/10/15.
 */

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class reducerTf extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable TF = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable frq : values) {
            sum += frq.get();
        }

        TF.set(sum);

        context.write(key, TF);
    }

}
