

## 窗口分析2 lag/lead

### 窗口函数——上下「lag/lead」

LAG(col,n,DEFAULT) 用于统计窗口内往上第n行值

> 第一个参数为列名，
> 第二个参数为往上第n行（可选，默认为1），
> 第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）

LEAD(col,n,DEFAULT) 用于统计窗口内往下第n行值

> 第一个参数为列名，
> 第二个参数为往下第n行（可选，默认为1），
> 第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL）

### 日期函数

<u>**函数日期均只能为'yyyy-MM-dd'格式 & 'yyyy-MM-dd HH:mm:ss'格式**</u>

**datediff(endDate, startDate)**

返回endDate和startDate相差的天数。

**date_add（start_date, num_days）**

返回初试日期n天后（负数为之前）的日期。

### 第一道面试题

- 需求：编写Hive的HQL语句求出连续三天登陆的用户id

- 元数据. (user_id，login_date)

  ```sql
  -- 创建表，并导入数据
  create table dc_dwd.login_log as
  select 1 as user_id, "2020-01-01" as login_date
  union all
  select 1 as user_id, "2020-01-02" as login_date
  union all
  select 1 as user_id, "2020-01-07" as login_date
  union all
  select 1 as user_id, "2020-01-08" as login_date
  union all
  select 1 as user_id, "2020-01-09" as login_date
  union all
  select 1 as user_id, "2020-01-10" as login_date
  union all
  select 2 as user_id, "2020-01-01" as login_date
  union all
  select 2 as user_id, "2020-01-02" as login_date
  union all
  select 2 as user_id, "2020-01-04" as login_date
  ```

- 查看表数据

  ```sql
  hive> select * from login_log;
  OK
  1	2020-01-01
  1	2020-01-02
  1	2020-01-07
  1	2020-01-08
  1	2020-01-09
  1	2020-01-10
  2	2020-01-01
  2	2020-01-02
  2	2020-01-04
  ```

#### 1、lag/lead函数+datediff方法

- 查询语句

- ```sql
  --A 首先查询 该记录的用户ID、上一条时间、本条时间、下一条时间。
  select 
  user_id,
  lag(login_date,1) over(partition by user_id order by login_date) as lag_date,
  login_date,
  lead(login_date,1) over(partition by user_id order by login_date) as lead_date
  from login_log;
```
  
  A:
  
  ```
  1	NULL	2020-01-01	2020-01-02
  1	2020-01-01	2020-01-02	2020-01-07
  1	2020-01-02	2020-01-07	2020-01-08
  1	2020-01-07	2020-01-08	2020-01-09
  1	2020-01-08	2020-01-09	2020-01-10
  1	2020-01-09	2020-01-10	NULL
  2	NULL	2020-01-01	2020-01-02
  2	2020-01-01	2020-01-02	2020-01-04
  2	2020-01-02	2020-01-04	NULL
  ```
  
- ```sql
-- B 使用datediff函数判断过滤（当前日期和前一天日期差一天、当前日期和后一天日期差一天），说明为连续三天登陆
  -- datediff()函数，注意用新的一天减去旧的一天
  select user_id from (
  select 
  user_id,
  lag(login_date,1) over(partition by user_id order by login_date) as lag_date,
  login_date,
  lead(login_date,1) over(partition by user_id order by login_date) as lead_date
  from login_log
  ) aa
  where datediff(login_date,lag_date) = 1 and datediff(lead_date,login_date) = 1;
  ```
  
  B:
  
  ```
  1
  1
  ```
  
- 最后通过distinct 或者 group by去重即可。

#### 2、date_add方法+row_number函数

- 查询语句

  首先查询出每个用户id、登陆日期、登陆日期排名（row_number）

  ```sql
  select 
  user_id,
  login_date,
  row_number() over(partition by user_id order by login_date) as rk
  from login_log
  ```

  结果：

  ```
  1	2020-01-01	1
  1	2020-01-02	2
  1	2020-01-07	3
  1	2020-01-08	4
  1	2020-01-09	5
  1	2020-01-10	6
  2	2020-01-01	1
  2	2020-01-02	2
  2	2020-01-04	3
  ```

  第二步，使用date_add函数。

  获取user_id,login_date,**归一化日期（如果用户是连续登陆，这个日期是同一天）**

- ```sql
  select user_id,login_date,date_add(login_date,1-rk) as con_date
  from 
  (
  select 
  user_id,
  login_date,
  row_number() over(partition by user_id order by login_date) as rk
  from login_log
  ) bb
```
  
  结果：
  
  ```
  1	2020-01-01	2020-01-01
  1	2020-01-02	2020-01-01
  1	2020-01-07	2020-01-05
  1	2020-01-08	2020-01-05
  1	2020-01-09	2020-01-05
  1	2020-01-10	2020-01-05
  2	2020-01-01	2020-01-01
  2	2020-01-02	2020-01-01
  2	2020-01-04	2020-01-02
  ```
  
  最后，查询用户、归一化日期、登陆天数
  
  ```sql
  select user_id,con_date,count(*) as days
  from
  (
  select user_id,login_date,date_add(login_date,1-rk) as con_date
  from 
  (
  select 
  user_id,
  login_date,
  row_number() over(partition by user_id order by login_date) as rk
  from login_log
  ) bb
  ) CC
  group by user_id,con_date
  ```
  
- 查询结果展示

  ```
1	2020-01-01	2
  1	2020-01-05	4
2	2020-01-01	2
  2	2020-01-02	1
```
  

