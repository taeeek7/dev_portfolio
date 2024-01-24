### 키퍼 가입퍼널 분석 rawdata 업로드 ###

from fn_sql_gspread  import sql_gspread_reset_v2, sql_gspread_append


##키퍼개인정보 -- [raw]키퍼정보

sql_gspread_reset_v2("키퍼개인정보"
                    , """ 
                    WITH keeper_table AS (
                        SELECT 
                        mk.member_keeper_id AS keeper_id, 
                        mk.name AS keeper_name,
                        g.grade_name AS grade,
                        REPLACE(concat(mk.name,"(",g.grade_name,")")," ","") AS name_grade, 	   
                        IFNULL(fn_get_code_name('KEEPER_STATUS', mk.state_code), '강제삭제') as state_code,
                        IFNULL(fn_get_code_name('FUNNEL', mk.funnel), '알수없음') as funnel,
                        CASE WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (1,3)  
                                        THEN '남성'
                                WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (2,4)
                                        THEN '여성'
                                ELSE '알수없음' END AS gender,
                        CASE WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (1,2)  
                                        THEN DATE_FORMAT(CURRENT_DATE(), '%Y') - (LEFT(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),2) + 1900)
                                WHEN mid(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),7,1) IN (3,4)
                                        THEN DATE_FORMAT(CURRENT_DATE(), '%Y') - (LEFT(CAST(AES_DECRYPT(UNHEX(mk.personal_id), 'personal_id') AS CHAR),2) + 2000)
                                ELSE 0 END AS age,
                        DATE_FORMAT(mk.create_at, '%Y-%m-%d') AS join_at
                        FROM member_keeper mk
                        LEFT JOIN grade g
                                        ON mk.grade_id = g.grade_id
                        LEFT JOIN branch b 
                                        ON mk.cl_cd = b.cl_cd
                                        AND mk.branch_id = b.branch_id
                        WHERE mk.LEVEL = 30 
                        and mk.name not like '%키퍼%'
                        and mk.name not like '%keeper%'
                        and mk.name not like '%룸세팅패스%'
                        ),
                        cleaning_table AS (
                        SELECT  op.member_keeper_id AS keeper_id,
                                        count(keeper_order_id) AS cnt
                        FROM order_party op
                        GROUP BY 1
                        )
                        SELECT  kt.keeper_id,
                                        kt.keeper_name,
                                        kt.grade,
                                        kt.name_grade,
                                        kt.state_code,
                                        kt.funnel,
                                        CASE WHEN kt.age >= 100 
                                                        THEN '알수없음'
                                                ELSE kt.gender
                                                END AS gender,
                                        CASE WHEN kt.age = 0 
                                                        THEN '알수없음'
                                                WHEN kt.age BETWEEN 20 AND 29 
                                                        THEN '20대'
                                                WHEN kt.age BETWEEN 30 AND 39 
                                                        THEN '30대'
                                                WHEN kt.age BETWEEN 40 AND 49 
                                                        THEN '40대'
                                                WHEN kt.age BETWEEN 50 AND 59 
                                                        THEN '50대'
                                                WHEN kt.age BETWEEN 60 AND 69 
                                                        THEN '60대'
                                                WHEN kt.age >= 100 
                                                        THEN '알수없음'
                                                ELSE '그외'		 	
                                                END AS age_range,
                                        CASE WHEN ct.cnt > 0
                                                        THEN '수행완료'
                                                ELSE '수행건없음'
                                                END AS cleaning_yn,
                                        kt.join_at,
                                        DATE_FORMAT(kt.join_at, '%Y') AS year,
                                        DATE_FORMAT(kt.join_at, '%m') AS month,
                                        WEEK(kt.join_at,3) AS week 
                        FROM keeper_table kt
                        LEFT JOIN cleaning_table ct
                                        ON kt.keeper_id = ct.keeper_id
                        ORDER BY keeper_id ASC;   
                    """
                    , "/home/ubuntu/AUSTIN/keeper-data-4c16ed1166b5.json"
                    , "1GLWPyJP9jLSwARAccpOj9nxper29yg0dvLWUtq66f4A"
                    , "[raw]키퍼정보"
            )




##신규가입지점 -- [raw]신규가입지점

sql_gspread_append("신규가입지점_timestamp"
                    , """ 
                    SELECT 
                    mk.member_keeper_id AS keeper_id, 
                    mk.name AS keeper_name,
                    IFNULL(fn_get_code_name('FUNNEL', mk.funnel), '알수없음') as funnel,
                    c.cl_cm AS client,
                    b.name AS branch,
                    b.region,
                    DATE_FORMAT(mk.create_at, '%Y-%m-%d') AS join_at,
                    DATE_FORMAT(mk.create_at, '%Y') AS year,
                    DATE_FORMAT(mk.create_at, '%m') AS month,
                    WEEK(mk.create_at,3) AS week
                    FROM (SELECT *
                            FROM member_keeper
                            WHERE LEVEL = 30 
                            AND name not like '%키퍼%'
                            AND name not like '%keeper%'
                            AND name not like '%룸세팅패스%'
                            ) AS mk
                    LEFT JOIN client c
                            ON mk.cl_cd = c.cl_cd
                    LEFT JOIN branch b 
                            ON mk.cl_cd = b.cl_cd
                            AND mk.branch_id = b.branch_id
                    WHERE DATE_FORMAT(mk.create_at, '%Y-%m-%d') = DATE_FORMAT(current_date-1, '%Y-%m-%d') 
                    order by join_at asc;
                    """
                    , "/home/ubuntu/AUSTIN/keeper-data-4c16ed1166b5.json"
                    , "1GLWPyJP9jLSwARAccpOj9nxper29yg0dvLWUtq66f4A"
                    , "[raw]신규가입지점"
            )