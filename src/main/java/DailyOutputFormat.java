import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DailyOutputFormat extends FileOutputFormat<Text, Text> {

    private final Map<String, FSDataOutputStream> dateOutputStreams = new HashMap<>();

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException {
        Path outputDir = FileOutputFormat.getOutputPath(context);
        FileSystem fs = FileSystem.get(context.getConfiguration());

        if (!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
        }

        return new RecordWriter<Text, Text>() {
            @Override
            public void write(Text key, Text value) throws IOException {
                // 避免使用 split("_") 创建正则对象和数组
                // 格式：YYYYMMDD_HHMMSS
                String keyStr = key.toString();
                int separatorIndex = keyStr.indexOf('_');

                if (separatorIndex == -1) return; // 容错

                String datePart = keyStr.substring(0, separatorIndex);
                // 这里保持原逻辑取MMDD;
                String mmdd = datePart.substring(4);
                String timePart = keyStr.substring(separatorIndex + 1);

                FSDataOutputStream out = dateOutputStreams.get(mmdd);
                if (out == null) {
                    Path filePath = new Path(outputDir, mmdd + ".csv");
                    out = fs.create(filePath, true);
                    out.writeBytes("tradeTime,alpha_1,alpha_2,alpha_3,alpha_4,alpha_5,alpha_6,alpha_7,alpha_8,alpha_9,alpha_10,alpha_11,alpha_12,alpha_13,alpha_14,alpha_15,alpha_16,alpha_17,alpha_18,alpha_19,alpha_20\n");
                    dateOutputStreams.put(mmdd, out);
                }

                out.writeBytes(timePart + "," + value.toString() + "\n");
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException {
                for (FSDataOutputStream out : dateOutputStreams.values()) {
                    out.close();
                }
                dateOutputStreams.clear();
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        Path outputDir = FileOutputFormat.getOutputPath(context);
        if (outputDir == null) {
            throw new InvalidJobConfException("Output directory not set");
        }
        FileSystem fs = FileSystem.get(context.getConfiguration());
        outputDir = fs.makeQualified(outputDir);
        FileOutputFormat.setOutputPath((Job) context, outputDir);
    }
}