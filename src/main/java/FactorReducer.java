import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Arrays;

public class FactorReducer extends Reducer<IntWritable, Factor, Text, Text> {
    private Factor sumFactor = new Factor();

    // 缓冲区：Key (YYYYMMDD_HHMMSS = 15 chars) + Value (20 floats * 20 chars)
    // 分给 Key 32字节，Value 1024字节，足够了
    private byte[] keyBuffer = new byte[32];
    private byte[] valueBuffer = new byte[1024];

    private Text outputKeyText = new Text();
    private Text outputValueText = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Factor> values, Context context)
            throws IOException, InterruptedException {

        // --- 1. 聚合逻辑 ---
        sumFactor.setCount(0);
        // 如果 Factor 类没有 reset 方法，请加上，或者手动把 internal array 归零
        // 这里假设 merge 会正确处理累加
        Arrays.fill(sumFactor.getFactorValues(), 0);

        for (Factor factor : values) {
            sumFactor.merge(factor);
        }
        double[] avgFactors = sumFactor.getAverageFactors();

        // --- 2. Value 格式化 (使用 RyuFloat 极速写入) ---
        int valOffset = 0;
        for (int i = 0; i < 20; i++) {
            if (i > 0) {
                valueBuffer[valOffset++] = ',';
            }
            // 强转 float 写入，实现 Zero-Allocation
            valOffset = RyuFloat.floatToBytes((float)avgFactors[i], valueBuffer, valOffset);
        }
        // 直接设置 byte[]，避免 String 转换
        outputValueText.set(valueBuffer, 0, valOffset);

        // --- 3. [关键修复] Key 格式化 ---
        // 必须还原为 "YYYYMMDD_HHMMSS" 格式，否则 DailyOutputFormat 会丢弃数据
        int compactTime = key.get();
        int tradingDay = CompactTimeUtil.decodeTradingDay(compactTime);
        int tradeTime = CompactTimeUtil.decodeTradeTime(compactTime);

        int keyOffset = 0;
        // 写入 YYYYMMDD
        keyOffset = writeIntToBytes(tradingDay, keyBuffer, keyOffset);
        // 写入分隔符 '_'
        keyBuffer[keyOffset++] = '_';
        // 写入 HHMMSS (需要补0，确保是6位)
        keyOffset = writeTime6Digits(tradeTime, keyBuffer, keyOffset);

        outputKeyText.set(keyBuffer, 0, keyOffset);

        // --- 4. 输出 ---
        context.write(outputKeyText, outputValueText);
    }

    /**
     * 快速写入整数到字节数组 (正整数)
     */
    private int writeIntToBytes(int val, byte[] buf, int offset) {
        if (val == 0) { buf[offset++] = '0'; return offset; }

        // 计算位数
        int temp = val;
        int len = 0;
        while (temp > 0) { temp /= 10; len++; }

        int end = offset + len;
        int cursor = end - 1;

        // 倒序填充
        while (val > 0) {
            buf[cursor--] = (byte) ('0' + (val % 10));
            val /= 10;
        }
        return end;
    }

    /**
     * 写入时间，强制填充为6位 (HHMMSS)，例如 93000 -> 093000
     */
    private int writeTime6Digits(int val, byte[] buf, int offset) {
        int end = offset + 6;
        int cursor = end - 1;

        // 填充最后面的数字
        for (int i = 0; i < 6; i++) {
            buf[cursor--] = (byte) ('0' + (val % 10));
            val /= 10;
        }
        return end;
    }
}