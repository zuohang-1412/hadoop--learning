# Hadoop MapReduce

## Hadoop 1.x

### 模型

1) JobTracker:
    1) 资源管理
    2) 任务调度
2) TaskTracker:
    1) 任务管理
    2) 资源回报
3) Client:

### 架构

1) Cli:
    1) cli 根据jar 中数据 去向NameNode 进行元数据访问，然后通过splits 得到一个【清单】。  
       其中包括 splits 为逻辑上的块，block 是物理上定义的块， block 上会存储 offset 信息和 locations 信息 （副本位置）。因此在splits
       和 block 之间存在映射关系后，也会获取到offset 和location 。  
       这样就可以支持 计算向数据移动了。
    2) 生成计算所需的配置文件 .xml
    3) 为了移动的可靠性， 会把jar，清单，.xml 存放到 hdfs 中，并进行 数据备份（默认备份数是10）。
    4) 最后通知 JobTracker 可以进行计算了， 并告知文件存放 hdfs 路径位置。

2) JobTracker
    1) 从 hdfs 中获取 splits 清单。
    2) 根据 TaskTracker 心跳反馈，获取其资源情况，确认 每一个splits 具体执行的 TaskTracker 位置。
    3) 等到第二次 TaskTracker 心跳，分配计算任务。

3) TaskTracker
    1) 通过心跳取回任务信息
    2) 从 hdfs 中下载jar 文件和配置信息
    3) 执行 MapTask/ ReduceTask 任务

### JobTracker 会出现三大问题：

1) 单点故障：JobTracker挂掉，所有计算任务都取消
2) 压力过大：JobTracker 负责管理 任务调度和资源管理 两大模块。
3) 因为集成了资源调度和任务管理，存在耦合。 未来新的资源管理不能复用，并且在同一批硬件中会出现资源争抢的现象。

## hadoop 2.x Yarn 资源管理

### 模型

1) container 容器
    1) 虚拟的：是一个对象，属性定义了 CPU、内存、IO量等， 并归属与哪一个NodeManager。
    2) 物理的：
        1) JVM -> 操作系统进程（NodeManager 会有线程监控container资源情况）
        2) 利用cGroup内核技术，在进行JVM启动时 由kernel 对 内存约束， 这样就不需要监控的存在了。

### 架构 MapReduce On Yarn

1) ResourceManager (RM): 负责整体资源管理
2) NodeManager (NM): 负责给RM 进行心跳汇报， 告知 资源情况
3) On Yarn
    1) cli (切片清单/.xml/jar 上传到hdfs)
    2) RM 选择一台不太忙的节点，让其中的NM 启动一个 container， 并反射一个 application master (AppMaster) 类.
    3) 启动AppMaster 后，去hdfs 中下载切片清单，然后向RM 申请资源。
    4) RM 根据资源情况，得到一个确定清单， 通知 NM 启动 container
    5) container 启动后会反向注册到 AppMaster 中
    6) AppMaster 在收到注册后 会发送Task任务到 container 中
    7) container 在接收到 相应Task类 后调用方法执行 。
    8) Task 任务失败会在其他NM 中再创建container 再去执行。

### 总结

在 1.x 中 JobTracker 和 TaskTracker 都是长服务
在 2.x 中 cli、AppMaster、container 都是临时服务

### 注意

1) Yarn 中可以配置 HA 模块，所以不用单独配置 HA了。

## 源码解析

### 知识点：ReflectionUtils 反射

### Client

1) 没有方法计算
2) 支撑了计算向数据移动和并行度
3) waitForCompletion 方法-> submit
    1) Checking the input and output specifications of the job.
    2) Computing the InputSplits for the job.
    3) Setup the requisite accounting information for the DistributedCache of the job, if necessary.
    4) Copying the job's jar and configuration to the map-reduce system directory on the distributed file-system.
    5) Submitting the job to the JobTracker and optionally monitoring it's status.
4) split -> writeSplits(FileInputFormat.java)
    1) minSize = 1
    2) maxSize = Long.MAX_VALUE
    3) blockSize = fs.getBlockSize()
    4) splitSize = Math.max(minSize, Math.min(maxSize, blockSize)) // 默认切片大小等于块大小
    5) 调大 splitSize = 调大 minSize; 调小 splitSize = 调小 maxSize
    6) 四大属性: splits.add(makeSplit(path, length-bytesRemaining, splitSize, blkLocations\[blkIndex].getHosts(),
       blkLocations\[blkIndex].getCachedHosts()))
        1) 切片归属于哪一个文件
        2) 获取 offset 偏移量
        3) 获取 splitSize
        4) blkLocations\[blkIndex].getHosts() 位置信息

### MapTask

1) input -> map -> output
2) 如果没有reduceTask: mapPhase = getProgress().addPhase("map", 1.0f)
3) 如果有reduceTask, 就需要排序优化: mapPhase = getProgress().addPhase("map", 0.667f); sortPhase = getProgress()
   .addPhase("sort", 0.333f);
4) LineRecordReader类
    1) init 时 会对文件进行按行划分。  
       如果出现 第一个block 结尾为"he"， 第二个block 开头为"llo ... "的情况，  
       在运行第一个 map 时从开头一直读取到结尾的offset 向下偏移一行，第二个map offset 也会向下偏移一行再开始读取。换言之就是会多读取一行防止字段被截取。
    2) nextKeyValue(): 每一条数据进行key，value 赋值， 并返回boolean
       getCurrentKey：获取key
       getCurrentValue：获取value
5) hashCode -> partitions 保证相同 key 在同一个分区中
6) MapOutputBuffer 类：
    1) init:
        1) spillper :0.8 溢写80%， 防止map 溢写100% 时阻塞
        2) sortmb： 100M 环形缓冲区大小 在赤道两侧进行kv写入和索引（partitions[Int],Key Start[Int],Value Start[Int],Value
           Start[Int]）
        3) sorter：QuickSort 快排
        4) comparator 排序比较器: job.getOutputKeyComparator()
            1) 自定义排序比较器
            2) 默认排序比较器
        5) combine：预聚合减少IO传输
            1) minSpillsForCombine = 3
        6) SpillTread: 溢写线程
            1) sortAndSpill()
            2) combinerRun
7) 具体操作：
    1) 每次溢写时，会将80%环形缓冲区进行加锁，同时map 输出线程会向剩余20%位置重新划分赤道，在赤道两侧写入 kv和 索引
    2) 快速排序80%的数据， 排序移动的是 索引而不是kv，因为索引是固定16字节。
    3) 溢写按照排好序的索引进行数据拉取，这样文件中的数据就是有序的
    4) 这样就成了 分区有序，分区内分组有序， reduce 只进行归并排序就可以了。
8) 调优手段：combiner
    1) 其实就是map 中的reduce
    2) 发生的时间节点：
        1) 内存溢写数据之前排序之后每，减少溢写io
        2) 最终map 端输出过程中， buffer 会溢写成多个小文件，
           当 minSpillsForCombine = 3， map 会把溢写的小文件合并成为一个大文件，避免小文件碎片化对未来 reduce 拉取数据造成随机读写。
    3) 要注意的是 combiner 需要注意数据聚合是否是幂等：求和计算（可以）；平均值计算（不可以）

### ReduceTask

1) input-> reduce-> output
2) doc:
    1) shuffle: 相同的key 拉取到同一分区
    2) sort: 对已经排好序的数据进行归并排序
    3) reduce:
3) 具体操作：
    1) reduce 拉取数据，把数据包装成一个rIter，防止大量数据内存溢出的现象
    2) comparator 分组比较器：
        1) 自定义分组比较器
        2) 自定义排序比较器
        3) 默认排序比较器
    3) 创建第二个迭代器，只负责对同一组中的数据进行迭代，
        1) hasNext(): 判断 nextKeyIsSame, 下一条数据是否是一组
        2) next(): 负责调取nextKeyValue 方法，读取rIter迭代器，并更新 nextKeyIsSame。