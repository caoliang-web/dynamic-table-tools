## dynamic-table-tools
根据指定数据源库表自动创建doris库表


## 前置条件

### 1. 下载并安装JDK1.8

可参考[Download and Installation Instructions](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)

### 2. 下载安装 Maven

[download maven 3.6.3](https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz)

### 3. 配置参数

#### 3.1 配置dynamic-tools.conf

```java
source_type=mysql
source_ip=127.0.0.1
source_port=3306
source_user=root 
source_password=123456
source_database=demo|sys
source_including_table=.*|.*
source_excluding_table=


target_user=root
target_password=
target_database=
target_url=jdbc:mysql://127.0.0.1:9030
table_prefix=
table_suffix=
table_conf={"replication_num":"1"}
```


| 参数名                    | 参数值                                       |
|------------------------|-------------------------------------------|
| source_type            | 来源                                        |
| source_ip              | ip                                        |
| source_port            | 端口                                        |
| source_user            | 用户名                                       |
| source_password        | 密码                                        |
| source_database        | 数据库                                       |
| source_including_table | 需要同步的 MySQL 表，可以使用`｜`分隔多个表，并支持正则表达式。比如：table1 |
| source_excluding_table | 不需要同步的表，用法同上。                             |
| target_user            | doris用户名                                  |
| target_password        | doris密码                                   |
| target_database        | 指定doris数据库名称                              |
| target_url             | jdbc:mysql://127.0.0.1:9030               |
| table_prefix           | Doris 表前缀名，例如 --table-prefix ods_|
| table_suffix           | 同上，Doris 表的后缀名|
| table_conf             | Doris 表的配置项，即 properties 中包含的内容（其中 table-buckets 例外，非 properties 属性）。例如{"replication_num":"1","table-buckets":"tbl1:10,tbl2:20,a.:30,b.:40,.*:50","table-partitions":"tbl1:dt_column:month,tb2:dt_column:day","convert-uniq-to-pk":"true"} 表示按照正则表达式顺序指定不同表的 buckets 数量，如果没有匹配到则采用 BUCKETS AUTO 建表                   |



### 4. 基本使用
git clone https://github.com/caoliang-web/dynamic-table-tools.git

sh flink-tools.sh