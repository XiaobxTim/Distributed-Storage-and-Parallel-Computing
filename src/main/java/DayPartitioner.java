import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DayPartitioner extends Partitioner<IntWritable, Factor> {

    @Override
    public int getPartition(IntWritable key, Factor value, int numPartitions) {
        // 直接使用整数Key，避免字符串解析
        int compactTime = key.get();
        int dayCode = CompactTimeUtil.getDayCode(compactTime);

        // 使用更均匀的哈希算法减少数据倾斜
        return (dayCode ^ (dayCode >>> 16)) % numPartitions;
    }
}