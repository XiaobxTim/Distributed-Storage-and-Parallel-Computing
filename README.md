# Distributed-Storage-and-Parallel-Computing
Based on the HDFS+MapReduce framework (Java implementation), this project conducts distributed computing of 20 Limit Order Book (LOB)-related quantitative factors using Shenzhen Stock Exchange Level-10 high-frequency snapshot data (3-second frequency). Key parameters are set as n=5 (order levels) and ∆t=1 (time interval), with 1e-7 added to denominators to avoid division by zero. Core tasks include calculating the average factor value sequences of CSI 300 index stocks (training phase: data from Jan 2-8, 2024) and outputting sequences for new data (testing phase), evaluated by technical documentation, presentations, and code performance (1% error tolerance, speed ranking). Through multi-dimensional optimizations—including CSV parsing enhancement, object reuse, memory control, Key compression, Shuffle optimization, concurrency tuning, and instruction-level improvements—computational efficiency is significantly improved while ensuring accuracy, enabling efficient distributed processing of high-frequency quantitative factor data.

## Code Detailed
1. `FactorCombiner.java`: A Hadoop Combiner class that merges Factor objects output by Mappers based on their keys, accumulating factor values and counts to reduce data transfer in the Reduce phase.

2. `DailyOutputFormat.java`: A custom Hadoop output format that writes results to CSV files corresponding to the date part (MMDD) in the key, including headers and specific data rows.

3. `Factor.java`: A factor class implementing the Writable interface, containing 20 factor values and a count, with methods for calculation, merging, serialization, etc., used to store and process factor data.

4. `FactorCalculationJob.java`: A Hadoop job configuration class that sets various parameters of the MapReduce job (input/output formats, Mapper/Reducer classes, compression methods, memory configuration, etc.) and starts the job.

5. `CompactTimeUtil.java`: A time encoding utility class that compresses dates and times into 26-bit integers, supporting encoding, decoding, and time comparison to optimize storage and processing efficiency of time data.

6. `SnapshotData.java`: A snapshot data container class that stores transaction snapshot information (such as bid/ask prices, trading volumes, etc.) and provides methods for parsing byte arrays and copying data.

7. `FastParser.java`: A fast CSV parsing utility class that provides methods for parsing dates, long integers, strings, etc., with optimizations like loop unrolling to improve parsing efficiency.

8. `RyuFloat.java`: A floating-point to byte conversion utility class based on the Ryu algorithm, enabling efficient conversion of floating-point numbers to byte arrays with reduced memory allocation.

9. `FactorMapper.java`: A Hadoop Mapper class that parses input snapshot data, calculates factor values, caches them, and outputs to the Combiner/Reducer when a threshold is reached, including a custom cache structure for performance optimization.

10. `FactorReducer.java`: A Hadoop Reducer class that performs final aggregation on factor data output by the Combiner, calculates averages, and formats them for output.

11. `DayPartitioner.java`: A Hadoop partitioner class that partitions data based on the date part in the compressed time to optimize data distribution in the Reduce phase.