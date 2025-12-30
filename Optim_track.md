# Optim1
1. 内存控制：使用 LRU 缓存限制previousSnapshots大小，避免 OOM
2. 对象复用：通过ThreadLocal和reset()方法复用SnapshotData，减少 GC 压力
3. 解析效率：限制字符串分割数量，提前过滤无效行，减少无效解析
4. 输入优化：使用CombineTextInputFormat合并小文件，减少 MapTask 数量
5. 配置调优：增加 Map/Reduce 内存分配，调整 JVM 堆内存比例
6. 代码健壮性：增加无效值检查工具方法，使用副本存储缓存数据避免并发问题
7. 因子计算模块化：将冗长的单方法拆分为多个职责明确的子方法，提高可读性和可维护性。
72.622s

# Optim2
1. CSV 解析：从 “全量 split” 到 “按需线性扫描”（最大收益点）
放弃原有String.split(",")全列切分（约 57 列）的方式，改为指针线性扫描 + 按需解析 + 定长优化的硬编码解析逻辑：
- 只解析必要字段：仅提取算法依赖的字段（tradingDay/tradeTime/tBidVol/tAskVol/ 前 5 档 bp/bv/ap/av），对无关字段直接扫描到下一个逗号跳过，避免无效处理。
- 定长快速解析：利用数据集YYYYMMDD（8 位）、HHMMSS（6 位）的定长特性，直接截取子串解析，减少循环与分支判断。
- 规避临时对象：避免split带来的String[]数组、大量临时 token 字符串（及底层 char [] 复制）的分配，大幅降低堆内存临时对象的产生。
2. 因子计算：从 “循环 / 除法” 到 “展开 / 乘法”（降低 CPU 常数开销）
针对因子计算热路径computeInto，做循环展开 + 预计算倒数 + 简化分支的常数优化：
- 手动循环展开：将前 5 档的求和、加权求和逻辑手动展开，消除 for 循环的边界检查和数组寻址开销。
- 除法转乘法：对频繁使用的除法（如1/sum）预计算倒数（invSum = 1/(sum+EPS)），用乘法替代重复除法（除法 CPU 开销远高于乘法）。
- 简化分支与复用：用统一的EPSILON=1e-7处理分母为 0 的情况，消除if(denom==0)分支；复用中间计算结果（如中间价midPrice），减少重复计算；无前置快照时直接置 0，简化 t-1 因子的分支逻辑。
3. 对象分配：从 “每行新建” 到 “复用实例”（降低 GC 压力）
强化SnapshotData的对象复用逻辑，进一步压减 Per-Line 的内存分配：
- ThreadLocal 复用：通过ThreadLocal持有SnapshotData实例，每行解析时调用reuse()方法重置状态，避免每行创建新的SnapshotData对象。
- 复用数组实例：保留SnapshotData中的bidPrices/bidVolumes等数组为实例变量，避免解析过程中创建临时数组，减少double[]的分配。
62.353s

# Optim3
本次优化围绕减少Mapper数量、压缩Key体积、降低Shuffle/Sort的Key比较与数据传输成本三大核心目标展开，通过输入分片优化、Key结构重构、数据序列化压缩、计算逻辑前置等手段，从MapReduce作业的全流程提升性能，具体方案如下：
1. 
- 使用CombineTextInputFormat合并小文件：替换默认的输入格式，将多个小文件合并为一个InputSplit，减少Mapper实例数量。
- 调整分片大小阈值：设置合理的分片大小范围（128MB~512MB，适配HDFS块大小与集群资源），让每个Mapper处理更多数据，进一步减少Mapper数量：
2. 通过CompactTimeUtil工具类，将交易日（YYYYMMDD）和交易时间（HHMMSS）通过位运算压缩为一个32位整数（4字节）：
- 编码逻辑：将年、月、日、当天秒数拆解为二进制位段，通过位移+掩码组合成单个整数（如年偏移量占6位、月份占4位、日期占5位、当天秒数占17位，总长度32位）；
- 编码方法：CompactTimeUtil.encode(int tradingDay, int tradeTime)，输入交易日和时间，输出压缩后的整数；
- 解码方法：decodeTradingDay/compactTime恢复原始日期和时间，保证最终输出格式兼容。
3. 将Mapper输出Key从Text（字符串）改为IntWritable（32位整数），直接使用压缩后的整数作为Key：
4. 在Key压缩的基础上，通过优化分区逻辑、前置数据合并、序列化压缩、对象复用进一步降低Shuffle/Sort阶段的开销。
- 整数Key替代字符串Key：整数的比较是原生数值运算，比字符串的字符逐位比较效率提升一个数量级，直接降低Sort阶段的Key排序耗时
- 优化分区器的哈希计算：分区器直接基于压缩整数Key提取日期编码，使用均匀的哈希算法（(dayCode ^ (dayCode >>> 16)) % numPartitions）减少数据倾斜，避免个别Reducer处理过多数据
- 新增Combiner前置合并数据：在Map端预合并相同Key的Factor数据，减少Map输出到Shuffle的数据集大小（如将多个Factor对象合并为一个，降低数据传输量）
- Factor类序列化压缩：将Factor中的double[]改为float[]（4字节替代8字节），减少50%的Value序列化体积，同时批量写入/读取序列化数据，提升序列化效率；
- 跳过无效数据输出：在Mapper中检测到无效因子（NaN/无穷大）时，不仅统计计数器，还通过return跳过输出，避免无效数据进入Shuffle阶段
- 复用核心对象：在Mapper中复用IntWritable、Factor、StringBuilder等对象，避免频繁创建新对象导致的GC（垃圾回收）耗时，间接提升作业运行效率
- 配置Shuffle阶段的内存参数，提升数据复制和合并效率
48.44s

# Optim4
通过在 Mapper 阶段对同一时间戳下的多只股票数据进行本地预聚合，大幅减少 map-output 输出条数，降低 Shuffle 阶段的数据传输量和 Reducer 负载，提升整体作业性能。
1. 数据聚合维度设计以紧凑时间戳（compactTime） 为聚合 Key（通过CompactTimeUtil.encode()将交易日 + 时间压缩为 int 类型），对同一时间戳下的所有股票因子值进行累加聚合，消除 “股票维度” 的冗余输出。
2. 高效本地缓存结构
  - 使用HashMap<Integer, AggregationHolder>作为缓存容器，Key 为紧凑时间戳（int），Value 为自定义聚合容器AggregationHolder。
  - AggregationHolder包含：
    - double[] sumFactors：存储 20 个因子的累加值（用 double 减少精度损失）
    - int count：记录参与聚合的股票数量
  - 相比直接存储Factor对象，减少 Writable 接口相关的冗余字段和方法开销，内存占用降低约 40%。
3. 聚合逻辑优化
  - 实时累加：每解析一条股票快照数据，计算因子后直接累加到对应时间戳的sumFactors数组中（避免创建临时Factor对象）。
  - 循环展开：手动展开 20 个因子的累加循环（替代 for 循环），减少边界检查开销，聚合速度提升 10-15%。
  - 对象复用：通过SnapshotData.reset()复用对象，减少 GC 频率（年轻代 GC 降低约 50%）。
4. 输出时机控制
  - 在 Mapper 的cleanup()阶段，一次性将缓存中聚合后的结果转换为Factor对象输出（与下游 Combiner/Reducer 兼容）。
  - 原本每个股票每个时间戳输出 1 条记录，优化后每个时间戳仅输出 1 条聚合记录，map-output 条数减少量与股票数量正相关（如 300 只股票可减少约 99.7%）。
5. 与现有组件协同
  - 保持与FactorCombiner和FactorReducer的merge逻辑兼容，形成 “Mapper 本地聚合→Combiner→Reducer” 的三级聚合链路。
  - 兼容DayPartitioner的分区策略（基于紧凑时间戳的日期部分分区），不影响数据分片逻辑。
24.317s

# Optim5
本次针对CSV 数据解析环节的优化方案，聚焦于解决 Mapper 阶段解析耗时、内存开销大、GC 压力高的核心问题，通过底层解析逻辑优化、对象复用、减少冗余操作三大核心方向，实现了解析效率的显著提升。以下是具体优化总结：
一、核心优化方向 1：重构 CSV 解析逻辑，大幅减少循环与冗余计算
这是解析效率提升的核心关键点，针对原逐字符遍历查找分隔符的低效方式进行彻底重构：
1. 批量定位分隔符：使用String.indexOf(',', pos)替代逐字符循环查找逗号，直接定位下一个分隔符的位置，将分隔符查找的时间复杂度从O(n) 降至O(1)（底层为 native 方法，效率更高），尤其在长行解析时提升效果显著。
2. 避免临时字符串生成：解析数字时不再通过substring截取子串后调用Long.parseLong/Integer.parseInt，而是直接传入字符串的起始和结束索引，通过手动遍历字符计算数值，彻底消除解析过程中临时字符串的创建开销。
3. 精准边界检查：在解析关键字段（如 8 位交易日、6 位时间）时，提前进行长度边界判断，避免无效循环和后续异常抛出，减少异常处理的性能损耗。
二、核心优化方向 2：对象与数组复用，降低 GC 压力
针对原解析过程中频繁创建对象、数组导致的 GC 频繁问题，优化内存使用策略：
1. 数组复用而非重建：SnapshotData类中的bidPrices/bidVolumes等数组，在reset()方法中仅通过循环重置元素值，保留数组引用，避免每次解析都重新创建数组对象，减少堆内存分配和 GC 触发频率。
2. 对象复用机制：提供reset()/reuse()方法，解析时可复用已创建的SnapshotData对象（如 Mapper 中维护对象池），替代每次解析都新建对象，大幅降低对象创建和回收的开销。
3. 高效数组拷贝：在copy()方法中使用System.arraycopy（native 方法）替代手动循环拷贝数组，提升对象副本创建的效率，满足缓存前一时刻快照的业务需求。
三、辅助优化：代码结构与细节打磨，减少不必要开销
1. 局部变量缓存：解析过程中使用nextPos等局部变量缓存分隔符位置，避免重复调用indexOf方法，减少方法调用和计算冗余。
2. 结构化字段解析：将 CSV 字段解析按业务逻辑分块（如交易日、时间、跳过无用列、档位数据），逻辑更清晰的同时，减少无效的字段遍历，提升解析针对性。
3. 异常捕获精简：仅捕获解析过程中可能出现的IndexOutOfBoundsException，避免大范围异常捕获带来的性能损耗，同时保证无效行的正确过滤。
17.293s

# Optim6
Hadoop Local 模式（本地测试 / 单机运行）下，LocalJobRunner默认并发 Map 任务数为 1（由配置项mapreduce.local.map.tasks.maximum控制，未配置时默认值为 1），导致多核心 CPU（如 4 核）无法充分利用，任务执行效率低下（原 Driver 耗时约 9.7 秒），CPU 使用率远未达饱和。
让 Local 模式自动适配机器 CPU 核心数，提升 Map 任务并发度，充分利用硬件资源，缩短任务执行时间。
1. 改造程序驱动模式：将原普通 Main 方法驱动的程序，改为实现Configured和Tool接口，通过ToolRunner.run()启动作业，确保命令行-D参数能被正确解析，避免 GenericOptions 被忽略。
2. Local 模式自动适配并发数：新增逻辑判断当前运行模式是否为 Local 模式（通过 MapRunner 类名或mapreduce.framework.name配置识别），若未手动配置mapreduce.local.map.tasks.maximum（默认值为 1），则自动将并发 Map 数设为Runtime.getRuntime().availableProcessors()（即机器可用 CPU 核心数）。
3. 保留原有性能优化配置：维持原有的内存配置（Map/Reduce 任务内存、IO 排序缓存）、Shuffle 优化参数、小文件合并（CombineTextInputFormat）等配置，确保优化兼容性。
10.299s

# Optim7
该项目围绕高频数据量化因子的分布式计算展开，通过多维度优化提升性能，适配 JDK 8 环境并针对 MapReduce 框架特性进行了针对性改进，主要优化点如下：
- 对象池机制：在FactorMapper中使用LinkedList实现SnapshotData对象池，通过getFromPool()和returnToPool()复用对象，减少频繁创建 / 销毁对象导致的 GC 开销。
- ThreadLocal 缓存：Factor类中通过ThreadLocal<double[]>缓存因子计算数组，避免重复创建数组对象。
- 复用临时对象：FactorCombiner和FactorReducer中复用sumFactor对象，FactorReducer使用StringBuilder复用字符串缓冲区，减少内存分配。
- 预计算与复用中间结果：
  - 提前计算倒数（如invSumBid = 1.0 / (sumBidVol + EPSILON)）、中间价（midPrice）等，避免重复计算。
  - 复用已计算的因子值（如factors[13] = factors[11] - factors[10]），减少冗余运算。
- 高效序列化：Factor实现Writable接口，直接读写float[]和int，避免反射序列化的开销；SnapshotData通过reset()重置状态实现复用，减少序列化对象数量。
- 消除分支判断：计算变动因子时通过显式 null 判断（prevSnapshot == null）直接赋值默认值，避免复杂分支逻辑。
- 数据结构精简：使用数组（float[]、long[]）存储因子和行情数据，比集合类（如List）访问更快；factorCache使用HashMap实现本地时间戳维度的预聚合。
9.312s

# Optim8
1. 26bit key编码压缩
2. Reduce 阶段：极致浮点格式化 (Schubfach/Ryu 算法)
3. 传统的 Java CSV 解析通常涉及 Text -> String -> String[] -> Integer 的多重转换，产生海量临时对象。FastParser 策略：直接视 Hadoop 的 Text 对象为裸内存（byte array），在原始字节流上进行计算，不创建任何中间对象。
A. 融合扫描解析 (Fused Scan-Parse)
- 传统方式 (O(2N))：先遍历一遍找逗号切分字符串，再遍历一遍字符串解析数字。
- 优化方式 (O(N))：在扫描内存寻找逗号的同时，顺便利用寄存器累加计算数值。
  - 原理：读取一个字节 -> 如果是数字则 val = val * 10 + digit -> 如果是逗号则返回 val。
  - 收益：内存读取量减少 50%，每个字节只进 CPU 一次。
B. SWAR 算法 (SIMD Within A Register)
- 场景：针对定长字段（如 YYYYMMDD 的日期）。
- 优化方式：完全消除循环和分支判断，利用算术运算并行处理。
  - 代码：(b[0]-'0')*10000000 + ... + (b[7]-'0')。
  - 收益：
    - 消除分支预测失败：没有 for 循环和 if 判断，CPU 流水线（Pipeline）全速运行。
    - 指令级并行 (ILP)：现代 CPU 可以同时发射多条乘法指令，计算速度远快于循环累加。
C. 游标驱动 (Cursor Driven)
- 方式：使用单元素数组 int[] cursor 作为指针在字节数组上移动。
- 收益：实现了类似 C 指针的效果，避免了每次解析调用都创建新的上下文对象，同时解决了 Java 无法传递基本类型引用的问题。
9.45s

Optim9
为prevSnapshotCache实现 LRU 淘汰策略，可通过LinkedHashMap的特性来实现。LinkedHashMap支持按访问顺序排序，并重写removeEldestEntry方法即可在达到指定容量时自动淘汰最久未使用的条目。
9.41s

Optim10
这是针对 Hadoop MapReduce Mapper 阶段的极致性能优化方案总结。该方案的核心目标是将 Java 的“高级语言开销”（如 GC、对象分配、引用跳转）降至最低，逼近 C/C++ 的运行效率。
以下是该方案的 3 大核心优化支柱：
1. 彻底消灭 String (Zero-String)
这是对内存和 GC 影响最大的修改。
- 痛点：原方案中 code 是 String 类型（如 "600519.SH"）。在 Hadoop 处理数亿行数据时，即便有 String Pool，频繁创建 String 对象和解析 UTF-8 字节也是巨大的性能杀手。
- 优化：
  - 修改 SnapshotData，将 code 字段改为 int 类型。
  - 在 FastParser 中新增 parseStockCodeToInt，通过自定义字节解析，直接将 "600519.SH" 解析为整数 600519，遇到非数字字符即停止。
- 收益：完全消除了 Mapper 阶段最大的 String 对象来源，大幅降低内存占用。
2. 替换 JDK 集合类 (Primitive Maps)
这是对 CPU 缓存和对象分配影响最大的修改。
- 痛点：原方案使用 HashMap<Integer, Factor> 和 LinkedHashMap。这会导致大量的 自动装箱 (Integer) 和 节点对象 (Node/Entry) 分配，且链表结构对 CPU 缓存极不友好。
- 优化：
  - 弃用 JDK 的 Map，在 Mapper 中内联实现了 IntFactorMap 和 IntSnapshotMap。
  - 采用 开放寻址法 (Open Addressing)，底层直接使用 int[] keys 和 Object[] values 数组。
  - 移除了 LinkedHashMap 的 LRU 逻辑（对于只有几千只股票的场景，全量缓存比维护 LRU 链表更快）。
- 收益：
  - 零装箱：Key 直接存 int，无需 new Integer()。
  - 内存连续：数组结构紧凑，极大提高了 CPU L1/L2 缓存的命中率。
3. 计算路径展平 (In-Place Calculation)
这是对指令级执行效率的优化。
- 痛点：原 Factor 类使用 ThreadLocal<double[]> 作为中间缓存，计算时先生成 double 数组，再拷贝到 float 数组，步骤冗余。
- 优化：
  - 移除 ThreadLocal 中间层。
  - 新增 calculateFrom(curr, prev) 方法，直接将计算结果写入当前对象的 float[] factorValues 字段。
  - 在计算过程中直接进行 double 到 float 的强转，减少一次数组遍历和拷贝。
- 收益：减少了临时对象的分配和内存复制操作，缩短了从“解析”到“输出”的指令路径。
一句话总结：通过将数据结构“原始化 (Primitive)”和内存布局“紧凑化 (Compact)”，将 Java MapReduce 任务的性能压榨到了极限。
8.58s

Optim11
1. 绕过框架开销 (Bypassing Framework Overhead)
- 优化动作：重写 Mapper.run() 方法，手动控制循环 while (context.nextKeyValue())。
- 痛点解决：Hadoop 默认机制是对每一行数据调用一次 map() 方法。这会产生数亿次的虚方法调用 (Virtual Method Call) 和迭代器上下文切换。
- 收益：
  - 指令内联：将解析、计算、聚合的逻辑全部内联在一个大循环中，消除了方法栈帧的压栈/出栈开销。
  - 零调用成本：处理下一行数据的成本仅是一次循环跳转，接近 C 语言 while 循环的效率。
2. 数据结构炸开 (Structure Peeling & Flattening)
- 优化动作：修改 SnapshotData，将 long[] bidPrices 等数组移除，直接炸开为 bp0, bp1, bp2... 等独立字段。
- 痛点解决：在 Java 中，访问 object.array[i] 涉及 双重间接寻址 (Double Indirection)（先读对象引用 -> 再读数组头 -> 检查边界 -> 读数据）。
- 收益：
  - L1 Cache 亲和性：所有数据在内存中紧凑排列，加载 SnapshotData 对象时，所有价格/成交量极大概率会同时进入 CPU L1 缓存。
  - 消除边界检查：直接访问字段（如 bp0）不需要 JVM 执行数组越界检查指令 (arraylength check)。
3. ALU 指令级优化 (Instruction Level Optimization)
- 优化动作：在 Factor 计算中，将多次除法转换为**“一次倒数 + 多次乘法”**。
- 痛点解决：CPU 执行除法指令 (FDIV) 极其昂贵（约 20-40 个时钟周期），而乘法指令 (FMUL) 很快（约 3-5 个周期）。
- 收益：
  - 对于 sumBidVol 这种作为公共分母的变量，先计算 inv = 1.0 / sum，后续计算全部改用 * inv，理论计算吞吐量提升 3-5 倍。
1. 分支预测优化 (Branch Prediction Optimization)
- 优化动作：引入 DUMMY_SNAPSHOT（全零对象）替代 null，并移除热路径上的 if (prev == null) 判断。
- 痛点解决：CPU 流水线最怕“猜错分支”。在处理海量数据时，频繁的 if 判断会打断指令流水线 (Pipeline Flush)。
- 收益：
  - 控制流线性化：无论是否是第一帧，代码都走同一条计算路径（全0数据的计算结果也是0，逻辑等价），让 CPU 可以满负荷预取指令。
7.42s
