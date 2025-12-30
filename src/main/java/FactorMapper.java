import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;

public class FactorMapper extends Mapper<LongWritable, Text, IntWritable, Factor> {

    // --- 静态 Dummy 对象 (全0)，用于消除 if (prev == null) ---
    private static final SnapshotData DUMMY_SNAPSHOT = new SnapshotData();

    // --- Primitive Map 实现 (IntFactorMap / IntSnapshotMap) ---

    private static class IntFactorMap {
        private int[] keys;
        private Factor[] values;
        private int size = 0;
        private int mask;
        public IntFactorMap(int capacity) {
            int cap = 1; while (cap < capacity) cap <<= 1;
            keys = new int[cap]; values = new Factor[cap]; mask = cap - 1;
            Arrays.fill(keys, -1);
        }
        public Factor get(int key) {
            int idx = key & mask;
            while (keys[idx] != -1) { if (keys[idx] == key) return values[idx]; idx = (idx + 1) & mask; }
            return null;
        }
        public void put(int key, Factor value) {
            int idx = key & mask;
            while (keys[idx] != -1) { if (keys[idx] == key) { values[idx] = value; return; } idx = (idx + 1) & mask; }
            keys[idx] = key; values[idx] = value; size++;
        }
        public void clear() { Arrays.fill(keys, -1); size = 0; }
        public int size() { return size; }
        public void flush(Context context, IntWritable outKey) throws IOException, InterruptedException {
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] != -1) { outKey.set(keys[i]); context.write(outKey, values[i]); }
            }
        }
    }

    private static class IntSnapshotMap {
        private int[] keys;
        private SnapshotData[] values;
        private int mask;
        public IntSnapshotMap(int capacity) {
            int cap = 1; while (cap < capacity) cap <<= 1;
            keys = new int[cap]; values = new SnapshotData[cap]; mask = cap - 1;
            Arrays.fill(keys, -1);
        }
        public SnapshotData get(int key) {
            int idx = key & mask;
            while (keys[idx] != -1) { if (keys[idx] == key) return values[idx]; idx = (idx + 1) & mask; }
            return null;
        }
        public void put(int key, SnapshotData value) {
            int idx = key & mask;
            while (keys[idx] != -1) { if (keys[idx] == key) return; idx = (idx + 1) & mask; }
            keys[idx] = key; values[idx] = value;
        }
    }

    // --- Mapper 成员 ---
    private IntFactorMap factorCache;
    private IntSnapshotMap prevSnapshotCache;
    private IntWritable outputKey = new IntWritable();
    private SnapshotData currentSnapshot = new SnapshotData();
    private Factor tempFactor = new Factor();
    private static final int CACHE_FLUSH_THRESHOLD = 50000;

    /**
     * Override run()
     * 绕过 map() 的虚方法调用和迭代器封装，直接在循环中处理
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        // 1. Setup
        setup(context);

        try {
            // 2. Loop (手动控制迭代，减少栈帧深度)
            while (context.nextKeyValue()) {
                // 直接获取 Value，忽略 Key (LongWritable 偏移量通常无用)
                Text value = context.getCurrentValue();

                // --- 逻辑内联 Start ---

                // A. 解析
                currentSnapshot.reset();
                boolean parsed = currentSnapshot.parseFromBytes(
                        value.getBytes(), 0, value.getLength());

                if (!parsed) continue; // 替代 return

                int code = currentSnapshot.code;
                int tradingDay = currentSnapshot.tradingDay;
                long tradeTime = currentSnapshot.tradeTime;

                // B. 时间编码
                int compactTime;
                try {
                    compactTime = CompactTimeUtil.encode(tradingDay, (int) tradeTime);
                } catch (IllegalArgumentException e) {
                    continue;
                }

                // C. 获取前一帧 (Branch Prediction Optimization)
                SnapshotData prevSnapshot = prevSnapshotCache.get(code);
                // 如果为空，指向 Dummy (全0)，避免 if (prev != null) 检查
                // Factor 计算中会根据 tradeTime != 0 来处理逻辑
                SnapshotData calcPrev = (prevSnapshot == null) ? DUMMY_SNAPSHOT : prevSnapshot;

                // D. 计算 (使用扁平化字段)
                tempFactor.calculateFrom(currentSnapshot, calcPrev);

                if (Factor.hasInvalidValue(tempFactor.getFactorValues())) {
                    updatePrevSnapshot(code, currentSnapshot);
                    continue;
                }
                tempFactor.setCount(1);

                // E. 聚合
                Factor cachedFactor = factorCache.get(compactTime);
                if (cachedFactor == null) {
                    cachedFactor = new Factor();
                    cachedFactor.copyFrom(tempFactor);
                    factorCache.put(compactTime, cachedFactor);
                } else {
                    cachedFactor.merge(tempFactor);
                }

                // F. Flush Check
                if (factorCache.size() >= CACHE_FLUSH_THRESHOLD) {
                    factorCache.flush(context, outputKey);
                    factorCache.clear();
                }

                // G. Update Prev
                updatePrevSnapshot(code, currentSnapshot);

                // --- 逻辑内联 End ---
            }
        } finally {
            // 3. Cleanup
            cleanup(context);
        }
    }

    // 辅助方法：更新缓存
    private void updatePrevSnapshot(int code, SnapshotData current) {
        SnapshotData prev = prevSnapshotCache.get(code);
        if (prev == null) {
            prev = new SnapshotData();
            prevSnapshotCache.put(code, prev);
        }
        prev.copyFrom(current);
    }

    @Override
    protected void setup(Context context) {
        factorCache = new IntFactorMap(65536);
        prevSnapshotCache = new IntSnapshotMap(16384);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        factorCache.flush(context, outputKey);
    }
}