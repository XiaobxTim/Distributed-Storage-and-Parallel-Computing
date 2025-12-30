import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FactorCombiner extends Reducer<IntWritable, Factor, IntWritable, Factor> {
    private Factor sumFactor = new Factor(); // 复用Combiner中的Factor对象

    @Override
    protected void reduce(IntWritable key, Iterable<Factor> values, Context context)
            throws IOException, InterruptedException {
        // 重置sumFactor，避免每次创建新对象
        sumFactor.setCount(0);
        for (int i = 0; i < 20; i++) {
            sumFactor.getFactorValues()[i] = 0;
        }

        for (Factor factor : values) {
            sumFactor.merge(factor);
        }
        context.write(key, sumFactor);
    }
}