package hadoop;

/**
 * Created by WCMinor on 22/08/2014.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
public class NLinesInputFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NLinesRecordReader();
    }
}