### 퍼널 대시보드 매출 및 클리닝 건수 분석 ###


from fn_sql_gspread_v2  import sql_gspread_convert_int_reset

sql_gspread_convert_int_reset(
"매출&클리닝수행건 by funnel"
,""" 
with funnel_table as (
select 
op.keeper_order_id,
IFNULL(fn_get_code_name('FUNNEL', mk.funnel), '알수없음') as funnel,
DATE_FORMAT(oc.end_at, '%Y') AS year,
DATE_FORMAT(oc.end_at, '%Y-%m') AS month, 
oc.depth2_cost
from (select keeper_order_id, member_keeper_id
      from order_party
      ) as op
left join member_keeper as mk 
      on op.member_keeper_id = mk.member_keeper_id
inner join (select *
            from order_complete
            where end_at is not null
            and cancel_id is null
            and work_name not in ('룸세팅', '(긴급) 룸세팅')
            ) as oc
      on op.keeper_order_id = oc.keeper_order_id
),
order_table as (
select 
op.keeper_order_id, 
count(*) as keeper_cnt
from (select keeper_order_id, member_keeper_id
      from order_party
      ) as op
group by 1
)
select 
ft.YEAR,
ft.month,
ft.funnel,
CONVERT(ifnull(sum(ft.depth2_cost/ot.keeper_cnt),0),INT) as total_rev,
CONVERT(ifnull(count(ot.keeper_order_id),0),INT) as cleaning_cnt
from funnel_table as ft
left join order_table ot 
      on ft.keeper_order_id = ot.keeper_order_id
group by 1,2,3
ORDER BY 2;
"""
,"C:/Users/austi/Desktop/SERVICE_KEY/keeper-data-4c16ed1166b5.json"
,"1GLWPyJP9jLSwARAccpOj9nxper29yg0dvLWUtq66f4A"
,"[raw]매출&수행건"
,[3,4]
,"D"
)


