# flink sql 程序样例
## 运行环境
- flink1.12.1
- jdk1.8
## 处理流程
1. 接收kafka中的数据
2. 实时统计每分钟的pv、uv值
3. 将统计结果存入mysql中

## 快速开始
### 构建
　1. 下载代码：
```
 git clone https://github.com/suola-opensource/flink-sql-submit.git
```
  2. 编译打包
```
 mvn clean package
```
  3. 安装kafka，修改**env.sh**文件,指定flink和kafka主目录
```
FLINK_DIR=/Users/develop/flink/flink-1.12.1
KAFKA_DIR=/Users/develop/flink/kafka_2.11-2.2.0
```

  4. 执行程序
```
# 启动kafka
./01start-kafka.sh
# 发送数据到kafka,脚本的路径适当修改
./02source-generator.sh
# 执行flink程序
./03run.sh
```
