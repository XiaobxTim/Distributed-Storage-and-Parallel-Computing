import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FactorCalculationJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FactorCalculationJob <input_path> <output_path>");
            return -1;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = getConf();

        // 虽然这个是针对Local模式的优化，但在集群模式下也不会有负面影响，故保留。
        String mapRunnerClass = conf.get("mapreduce.job.map.runner.class", "");
        boolean isLocalMode = mapRunnerClass.contains("LocalJobRunner")
                || conf.get("mapreduce.framework.name", "local").equals("local");

        if (isLocalMode) {
            int localMapMax = conf.getInt("mapreduce.local.map.tasks.maximum", 1);
            if (localMapMax == 1) {
                int cpuCores = Runtime.getRuntime().availableProcessors();
                conf.setInt("mapreduce.local.map.tasks.maximum", cpuCores);
                conf.setInt("mapreduce.local.reduce.tasks.maximum", cpuCores);
            }
        }

        // ------------------ 压缩与IO优化 ------------------
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        // ------------------ 内存与JVM优化 ------------------
        // 容器内存（物理限制）
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");

        // JVM堆内存（逻辑限制，通常为容器内存的75%-80%）
        // -XX:+UseG1GC: 推荐用于大内存和混合对象生命周期的场景
        conf.set("mapreduce.map.java.opts", "-Xmx1638m -XX:+UseG1GC");
        conf.set("mapreduce.reduce.java.opts", "-Xmx3276m -XX:+UseG1GC");

        // Shuffle 参数优化
        conf.set("mapreduce.task.io.sort.mb", "512"); // 增大排序缓存，减少溢写
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "10");

        // ------------------ Job配置 ------------------
        Job job = Job.getInstance(conf, "CSI300 Factor Calculation");
        job.setJarByClass(FactorCalculationJob.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024);
        CombineTextInputFormat.setMaxInputSplitSize(job, 512 * 1024 * 1024);

        job.setMapperClass(FactorMapper.class);
        job.setReducerClass(FactorReducer.class);
        job.setPartitionerClass(DayPartitioner.class);
        job.setCombinerClass(FactorCombiner.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Factor.class);

        // 显式设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(DailyOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
        Path output = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, output);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        if (success) {
            System.out.println("Done! Time: " + (endTime - startTime) / 1000.0 + "s");
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FactorCalculationJob(), args);
        System.exit(exitCode);
    }
}