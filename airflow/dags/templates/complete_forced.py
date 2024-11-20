from utils.sql import SqlUtils
from utils.gsheet import GsheetUtils
from pandasql import sqldf
import pandas as pd

class CompleteForcedData :
    # 가감비용 보정 데이터
    def set_benefit_cost(sheet_key, sheet_id, sheet_name) :
        # 키퍼 등급별 정산비율
        calculate_data = SqlUtils.get_source_data(
            conn_id= "cleanops",
            sql = """
            SELECT 
                mk.member_keeper_id,
                g.calculate 
            FROM member_keeper AS mk 
            INNER JOIN grade AS g 
                ON mk.grade_id = g.grade_id 
            ;
            """
        )
        calculate_df = pd.DataFrame(calculate_data)
    
        cal_rename = {
            0: "memberKeeperId",
            1: "calculate"
        }
        calculate_df.rename(columns= cal_rename, inplace= True)

        # 발행 대기 시트
        gsheet_df = GsheetUtils.read_gsheet(
            sheet_key= sheet_key,
            sheet_id= sheet_id,
            sheet_name= sheet_name
        )
        gs_rename = {
            0: "clCd", 
            1: "branchId", 
            2: "roomId", 
            3: "ticketCode", 
            4: "searchDate", 
            5: "memberKeeperId", 
            6: "completeCode", 
            7: "completeComment", 
            8: "depth2Rate", 
            9: "depth3Rate", 
            10: "keeperRate", 
            11: "benefitCost"
        }
        gsheet_df.rename(columns= gs_rename, inplace= True)

        # 등급별 정산비율 <> 발행대기시트 join pandasql
        join_query = """
        select 
            gdf.*
            ,10000 * gdf.keeperRate/100 
            (10000 * cdf.calculate/100 * (gdf.keeperRate/100)) as benefitCostReform
        from gsheet_df as gdf
        inner join calculate_df as cdf 
            on gdf.memberKeeperId = cdf.memberKeeperId 
        ;
        """
        return sqldf(join_query)