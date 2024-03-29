# Hive_interview-question
**总结典型的hive面试题**。

SQL面试题

HQL面试题

## 需求分类：
    窗口分析
    行列转换
    求TopN
    自连接
    group by

## 窗口分析

[窗口分析](interview-question/窗口分析.md)

1. 窗口函数_聚合函数「sum\max\min\avg」讲解
2. 面试题_1
   - ​	「求出每个店铺的当月销售额和累计到当月的总销售额。」
3. 面试题_2
   - ​	「求出每个用户截止到每月为止的最大单月访问次数和累计到该月的总访问次数。」
4. 面试题_3
   - ​	「按照day和mac分组，求出每组的销量累计总和，追加到每条记录的后面。」

[窗口分析2lag/lead](interview-question/窗口分析2lag-lead.md)

​	1.面试题

​			「求出连续三天登陆的用户id」

## 行列转换

[行转列](interview-question/行转列.md)

多行转到某行的一列上。

1. 面试题_4

   - ​	「求出所有数学课程成绩 大于 语文课程成绩的学生的学号。」

   - ​		case···when语句

2. 面试题_5

   - ​	「以1\0的形式展示出学生的选课情况。」

   - ​		collect_set()\collect_list()函数

   - ​		array_contains()函数

   - ​		if()函数

## 求TopN

[TopN](interview-question/TopN.md)

1. 窗口函数_几种序列函数「row_number()\rank()\dense_rank() 」

   - ​	Row_number()函数精讲

2. 面试题_6

   - ​	「求每年最高温度及其日期。」

3. 面试题_7

   - ​	「求每种爱好中年龄最大的那个人、年龄排名前2的人」

   - ​	[列转行](./interview-question/列转行.md)（虚拟视图+炸裂函数）

## 自连接

[自连接](./interview-question/自连接.md)

1. 面试题_8

   ​	「查找所有至少**连续三次**出现的数字。」

   - 笛卡尔积
   - 连接查询
   - lag、lead

2. 面试题_9

   ​	「求每个学生成绩最好的课程及分数、最差的课程及分数、平均分数」

   - 炸裂函数-->列转行
   - 窗口函数-->分组排序
   - case..when、concat()、max+group by

## group by

[gropu-by](./interview-question/group-by.md)

1. 面试题_10

   「输出每个产品，在2018年期间，每个月的净利润，日均成本。」

   「输出每个产品，在2018年3月中每一天与上一天相比，成本的变化。」

   「输出2018年4月，有多少个产品总收入大于22000元，必须用一句SQL语句实现，且不允许使用关联表查询、子查询。」

   「输出2018年4月，总收入最高的那个产品，每日的收入，成本，过程使用over()函数。」

