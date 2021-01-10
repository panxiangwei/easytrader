import pymysql

# 打开数据库连接
db = pymysql.connect("localhost", "root", "root", "xueqiu")

# 使用cursor()方法获取操作游标
cursor = db.cursor()

# SQL 插入语句
sql_account = """INSERT IGNORE  INTO xq_account(
ACCOUNT_NAME,INIT_BALANCE, TOTAL_BALANCE, MARKET_VALUE, ENABLE_BALANCE, TOTAL_REVENUE, TODAY_REVENUE, SELECT_REVENUE, CREATE_TIME, UPDATE_TIME,IS_DEL)
VALUES ('Mac', 100000, 10000, 2000, 2000,10,20,30,'2021-01-01 12:12:12','2021-01-01 12:12:12','0')"""

sql_history = """INSERT IGNORE  INTO xq_history(
ACCOUNT_ID,STOCK_NAME, STOCK_CODE, STOCK_COUNT, START_REPERTORY, HISTORY_TIME, IS_HAS, IS_DEL)
VALUES ('1', '平安', '1111', 2000,30,'2021-01-01 12:12:12','1','0')"""

sql_operation = """INSERT IGNORE  INTO xq_operation(
ACCOUNT_ID,STOCK_NAME, STOCK_CODE, STOCK_OPERATION, STOCK_PRICE, STOCK_COUNT, START_REPERTORY, END_REPERTORY, OPERATION_TIME, IS_DEL)
VALUES ('1', 'Mac', '1111','买入', 13.14, 2000,20,30,'2021-01-01 12:12:12','0')"""
try:
    cursor.execute(sql_account)  # 执行sql语句
    cursor.execute(sql_history)  # 执行sql语句
    cursor.execute(sql_operation)  # 执行sql语句
    db.commit()  # 提交到数据库执行
except:
    db.rollback()  # 如果发生错误则回滚

# 关闭数据库连接
db.close()


