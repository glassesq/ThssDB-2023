# ThssDB-2023

**Course Project For 2023 THSS Database**

## Requirement

《数据库原理》2023春大作业要求

### 元数据管理模块
在存储引擎的基础上，实现元数据管理模块。
框架中的schema包定义了一些基本概念：
- Table类：表的信息。一个Database可以有多张Table。
- Database类：数据库的信息。用户可以创建多个Database。
- Manager类：管理Database。

#### 基础要求
- [ ] 实现表的创建、删除；
- [ ] 实现数据库的创建、删除、切换；
- [ ] 实现表和数据库的元数据（有哪些数据库，数据库里有哪些表，每个表的结构如何）的持久化。
- [ ] 重启数据库时从持久化的元数据中恢复系统信息。
 
###### SQL
实现元数据管理模块后，ThssDB应当能够支持下列SQL语句（不区分大小写）：
- [x] 创建数据库 ``create database dbName;``
- [ ] 删除数据库 ``drop database dbName;``
- [x] 切换数据库 ``   use dbName;``
- [ ] 创建表：实现“NOT NULL”和“PRIMARY KEY”这两个关键字，Type为Int，Long，Float，Double，String（必须给出长度）之一。主键仅在某一列定义。
```
   CREATE TABLE tableName(
   attrName1 Type1,
   attrName2 Type2,
   attrNameN TypeN NOT NULL,     
   …,
   PRIMARY KEY(attrName1)
   );
   示例: CREATE TABLE person (
        name String(256), ID Int not null, PRIMARY KEY(ID))
```
- [ ] 删除表：删除某张表的模式信息`` DROP TABLE tableName;``
- [ ] 查找表：展示某张表的模式信息(每一列的名字及其数据类型，格式自定);`` SHOW TABLE tableName; ``
 
#### 进阶要求
- [ ] 外键约束的实现
- [ ] 实现表结构的修改（alter table）
  - [ ] 添加、删除、修改列
  - [ ] 添加、删除表上的约束

###  存储模块
完成存储引擎，实现对数据的基本管理。框架中的schema包定义了一些基本概念：
- Column类：一张表的某一列的元信息。成员变量包括该列的名字（name）、数据类型（type）、是否为主键（primary）、是否可以为空（notNull）以及最大长度（maxLength，仅限于String类型）。
- Entry类：某一行记录在表中某一列的数据。
- Row类：某一行记录在表中所有列的数据。

此外，框架中的B+树索引，已在index模块中实现，包含的类有：
- BPlusTree类：用主键entry做key，用真实的row作为value。
- BPlusTreeNode类：B+树结点抽象类。
- BPlusTreeInternalNode类：B+树内部结点类。存储key数组和children数组来支持B+树的索引。
- BPlusTreeLeafNode类：B+树叶子结点类。存储key数组和value数组来支持B+树的索引和真实数据的存储，同时维护一个next指针以支持所有叶子结点可以高效遍历。
- BPlusTreeIterator类：B+树的迭代器类。通过在B+树最底层的的叶子结点维护一个链表就可以高效的实现范围查询和遍历。

#### 基础要求
- [ ] 实现数据记录的持久化（补充说明：后续测试过程中是内存受限场景，无法全部存储在内存中）。
- [ ] 实现对记录的增加、删除、修改。
- [ ] 支持五种数据类型：Int，Long，Float，Double，String。
- [ ] 实现存储管理模块后，ThssDB应当能够支持下列SQL语句（不区分大小写）：

##### SQL
- [ ] 数据写入：字符串需要用单引号包围。
```
INSERT INTO [tableName(attrName1, attrName2,…, attrNameN)] VALUES (attrValue1, attrValue2,…, attrValueN);
示例：INSERT INTO person VALUES (‘Bob’, 15)
INSERT INTO person(name) VALUES (‘Bob’)会提示字段ID不能为空
```
- [ ] 数据删除：
```
DELETE FROM tableName WHERE attrName = attValue;
```
- [ ] 数据更新：
``` 
UPDATE tableName SET attrName=attrValue 
WHERE attrName=attrValue; 
```

#### 进阶要求
- [ ] 尝试实现更高效的文件存储格式，并在设计文档中说明设计思路及原理，可以考虑以下优化点：
    - [ ] 提高I/O效率
    - [ ] 减小外存空间占用（合理的编码方式、数据压缩）
    - [x] 页式存储

### 查询模块
实现查询引擎，包括SQL解析和查询执行两部分。其中，parser包给出了利用antlr4实现SQL解析的例子。框架中的query包提供了一些可能用到的基础类：
- MetaInfo类：存储一个表的名字和所有列，用来在Queryresult中构建用户想要获取的列的索引。
- QueryResult类：选择运算中选择某些列或全部列的结果，需要对歧义列名或不存在列名、不存在表名等错误情况进行捕捉。
- QueryTable类：选择运算中选择表的方法类，用迭代器的方式从当前表中不断获取新的行并判断其是否满足where中条件。
  负责SQL解析的Parser包提供了一些可能用于生成语法树的基础类，同学可根据需要新增SQL语句：
- parser/ImpVisitor类处理语法树，语法树的根结点对应一个数据操纵语句即DML语句（select_stmt, delete_stmt等），parser/ImpVisitor类根据树根结点选择不同函数分别执行（visitSelect_stmt调用query/QueryTable类，delete_stmt调用schema/Manager类等）。ImpVisitor类中已经给出实现实例（Drop Table等），请完成剩余部分。
- query/QueryTable类处理select_stmt语句，扫描From子句中对应的表，生成查询结果，并以QueryResult的形式返回，返回值为一个Table。已给出程序框架，请完成剩余部分。
- query/QueryResult类：create Table等语句的返回结果为一个Message，以表示执行是否成功，而不是一个Table。query/QueryResult类用来保留任意语句的执行结果。已给出程序框架，请完成剩余部分。

#### 基础要求
- [ ] 经过查询模块的实现，ThssDB应当能够支持下列SQL语句（不区分大小写）：
```
SELECT attrName1, attrName2, … attrNameN FROM tableName [WHERE attrName1 = attrValue];

SELECT tableName1.AttrName1, tableName1.AttrName2…, tableName2.AttrName1, tableName2.AttrName2,… FROM tableName1 JOIN tableName2 ON  tableName1.attrName1=tableName2.attrName2 [WHERE  attrName1 = attrValue];
```
- 上述语句中，Where子句至多涉及一个比较，并且关系为 ‘’<“,”>”,”<=”,”>=”,”=”,”<>”之一。 
- Select子句不包含表达式。
- Join至多涉及2张表，On子句的限制同Where子句。

#### 进阶要求
- [ ] 应用课程中介绍的查询优化技术
- [ ] 支持多列主键
- [ ] 实现三张表以上的join
- [ ] 触发器实现

### 并发控制模块
实现简单的事务处理模块，支持小规模的并发。
####  基础要求
- [ ] 实现begin transaction和commit；
- [ ] 采用普通锁协议，实现read committed，serializable的隔离级别；
#### 进阶要求
- [ ] 实现2PL或MVCC协议。

### 重启恢复模块
#### 基础要求
- [ ] 实现单一事务的WAL机制，要求实现写log和读log，在重启时能够恢复记录的数据即可。
#### 进阶要求
- [ ] 多事务并发下的WAL机制。
- [ ] 实现rollback、savepoint等功能。

## Reference

- 《数据库原理》2023春大作业要求
https://apache-iotdb.feishu.cn/docx/EuVyd4o04oSHzZxRtBFcfRa0nab

- 《数据库原理》ThssDB2023 开发指南
https://apache-iotdb.feishu.cn/docx/RHnTd3Y3tocJQSxIIJFcDmlHnDd
