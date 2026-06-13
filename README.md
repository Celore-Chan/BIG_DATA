```markdown
# 大数据管理与分析基本算法

本项目包含两个大数据基本算法：**PageRank** 网页排名算法 和 **InvertedIndex** 倒排索引构建，均基于 Hadoop MapReduce 实现。

## 算法列表

```
| 算法 | 描述 | 核心算法 |
|------|------|----------|
| PageRank | 网页重要性排序 | 迭代计算 + 阻尼系数 |
| InvertedIndex | 文档倒排索引构建 | MapReduce 词频统计 + 文档聚合 |

## 环境要求

| 组件 | 版本 |
|------|------|
| Hadoop | 3.x |
| Java | 8 / 11 |
| Maven | 3.x (可选) |
```
## 项目结构

bigdata-lab/
├── pagerank/
│   ├── pagerank.java          # PageRank 主程序
│   ├── DataSet                # 测试输入数据
│   └── part-r-00000           # 测试输出数据
├── invertedindex/
│   └── InvertedIndex.java     # 倒排索引主程序
└── README.md                  # 本文件
```

## 快速开始

### 1. 编译打包

```bash
# 编译 PageRank
javac -classpath $(hadoop classpath) pagerank/*.java
jar cvf pagerank.jar pagerank/*.class

# 编译 InvertedIndex
javac -classpath $(hadoop classpath) invertedindex/*.java
jar cvf invertedindex.jar invertedindex/*.class
```

### 2. 准备输入数据

```bash
# 上传输入文件到 HDFS
hdfs dfs -mkdir -p /user/hadoop/input/pagerank
hdfs dfs -put pagerank/input/* /user/hadoop/input/pagerank
```

### 3. 运行实验

#### PageRank

```bash
hadoop jar pagerank.jar pagerank.PageRank /user/hadoop/input/pagerank /user/hadoop/output/pagerank
```

#### InvertedIndex

```bash
hadoop jar invertedindex.jar invertedindex.InvertedIndex /user/hadoop/input/invertedindex /user/hadoop/output/invertedindex
```

### 4. 查看结果

```bash
hdfs dfs -cat /user/hadoop/output/pagerank/part-r-00000
hdfs dfs -cat /user/hadoop/output/invertedindex/part-r-00000
```

## 算法说明

### PageRank

- 核心公式：`PR(A) = (1-d) + d × Σ(PR(Ti)/C(Ti))`
- 阻尼系数 `d` 通常取 0.85
- 支持多轮迭代直到收敛

### InvertedIndex

- 输入：多个文档文件
- 输出：`词 -> 文档ID:词频` 的倒排索引
- 核心流程：Map 阶段提取词 → Shuffle 阶段按词分组 → Reduce 阶段聚合文档信息

## 常见问题

| 问题 | 解决方案 |
|------|----------|
| `ClassNotFoundException` | 检查编译和打包命令，确保 class 文件在 jar 中 |
| 输出目录已存在 | 运行前删除输出目录：`hdfs dfs -rm -r /path/to/output` |
| 内存不足 | 调整 YARN 配置或减小输入数据量 |

## 参考

- [Hadoop 官方文档](https://hadoop.apache.org/)
- [PageRank 论文 - Brin & Page (1998)](http://infolab.stanford.edu/pub/papers/google.pdf)

## 许可证

本项目仅供学习交流使用。
```
## 环境

- Hadoop 3.x
- Java 8+

## 项目结构

├── pagerank/          # PageRank 源码
└── invertedindex/     # InvertedIndex 源码
