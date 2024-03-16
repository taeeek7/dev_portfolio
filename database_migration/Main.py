from DatabaseMigration import SetOutputDatabase, EditDatabase

## 데이터베이스 insert 구문 사용 클래스
edit_class = EditDatabase(
        "host"
        ,"user"
        ,"password"
        ,"db_name"
)


### 테이블 삽입

table = SetOutputDatabase(
        "host"
        ,"user"
        ,"password"
        ,"db_name"
        ,
        """
        select * from table 
        ;
        """
)

branch_query = f"""
REPLACE 
	INTO db_name.table_name ()
        VALUES
                {table.insert_setting()}
"""

edit_class.insert_data_value(table= "branch", value= table.insert_setting(), query= branch_query)