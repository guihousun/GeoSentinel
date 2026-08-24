import re
from typing import List
from google.cloud import bigquery
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# 1. 加载环境变量 (确保 .env 中有 GOOGLE_APPLICATION_CREDENTIALS 路径)
load_dotenv()

# 2. 定义输入 Schema
class BigQuerySearchSchema(BaseModel):
    query: str = Field(
        ..., 
        description="SQL query for GDELT. Narrow the SQLDATE and select specific columns to keep scan size < 200GB."
    )

# 3. 核心查询逻辑
def query_bigquery_safe(query: str) -> str:
    """
    带有安全检查的 BigQuery 查询函数。
    1. 仅限 GDELT 数据集。
    2. 自动修正 LIMIT。
    3. Dry-run 检查扫描量（阈值 200GB）。
    4. 设定付费上限，防止意外扣费。
    """
    
    # --- A. 基础权限校验 ---
    # 仅允许查询 GDELT 公共数据集
    allowed_patterns = [r"gdelt-bq\.gdeltv2\.", r"gdelt-bq\.full\."]
    if not any(re.search(p, query, re.IGNORECASE) for p in allowed_patterns):
        return ("Error: Access Denied. Currently ONLY supports GDELT datasets (gdelt-bq.gdeltv2.events). "
                "Ensure your FROM clause is correct.")

    # --- B. 强制规则修正 ---
    # 1. 强制 SQLDATE 检查 (虽然不物理分区，但逻辑上必须有)
    if "SQLDATE" not in query.upper():
        return "Error: Query rejected. You MUST include a 'SQLDATE' filter for better data management."

    # 2. 强制 LIMIT 检查 (最大 300 条)
    limit_match = re.search(r"LIMIT\s+(\d+)", query, re.IGNORECASE)
    if not limit_match:
        query = query.rstrip().rstrip(';') + " LIMIT 50;"
    elif int(limit_match.group(1)) > 300:
        query = re.sub(r"LIMIT\s+\d+", "LIMIT 300", query, flags=re.IGNORECASE)

    # --- C. 初始化客户端 ---
    try:
        client = bigquery.Client()
    except Exception as e:
        return f"Authentication Error: {str(e)}. Check your GOOGLE_APPLICATION_CREDENTIALS."

    # --- D. 成本预估 (Dry Run) ---
    # GDELT 的 SQLDATE 不是物理分区，查询特定字段会扫描该字段的全量历史数据
    # 常用 7-10 个字段的扫描量通常在 130GB - 150GB 左右
    MAX_SAFE_GB = 200 
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    
    try:
        query_job = client.query(query, job_config=job_config)
        estimated_gb = query_job.total_bytes_processed / (1024**3)
        
        if estimated_gb > MAX_SAFE_GB:
            return (f"Error: Query size ({estimated_gb:.2f} GB) exceeds the {MAX_SAFE_GB}GB safety limit. "
                    "To fix this, please select fewer columns (e.g., only 3-5 necessary fields).")
    except Exception as e:
        return f"Pre-query Check Error: {str(e)}"

    # --- E. 正式执行查询 ---
    try:
        # 设置硬性的付费限额 (单位：字节)
        # 140GB 约为 150323855360 字节，这里设为 200GB 确保通过
        safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_SAFE_GB * 1024**3)
        
        query_job = client.query(query, job_config=safe_config)
        data = query_job.to_dataframe()
        
        if data.empty:
            return f"Query successful (Scanned {estimated_gb:.2f} GB) but no records found."
        
        # 返回 Markdown 表格供模型阅读
        return data.to_markdown(index=False)
        
    except Exception as e:
        return f"BigQuery Execution Error: {str(e)}"

# 4. 封装成 StructuredTool
google_bigquery_search = StructuredTool.from_function(
    func=query_bigquery_safe,
    name="Google_BigQuery_Search",
    description=(
        "Query global event data via Google BigQuery. "
        "LIMITATION: Only supports 'gdelt-bq.gdeltv2.events'. "
        "COST NOTE: This table is not physically partitioned by date; any query scanning common columns "
        "will cost ~135GB - 150GB. The 200GB limit allows ~7 FREE queries per month via Google's 1TB free tier. "
        "To minimize cost: ONLY SELECT essential columns like [GlobalEventID, SQLDATE, Actor1Name, ActionGeo_FullName, SOURCEURL]."
    ),
    args_schema=BigQuerySearchSchema
)

# --- 5. 测试调用区域 ---
# if __name__ == "__main__":
#     # 模拟测试：查询 2024 年 10 月 1 日上海相关的影响力前 5 的事件
#     test_query = """
#     SELECT 
#         GlobalEventID, 
#         SQLDATE, 
#         Actor1Name, 
#         ActionGeo_FullName, 
#         NumMentions, 
#         SOURCEURL 
#     FROM `gdelt-bq.gdeltv2.events` 
#     WHERE SQLDATE = 20241001 
#       AND ActionGeo_FullName LIKE '%Shanghai%' 
#     ORDER BY NumMentions DESC 
#     LIMIT 5
#     """
    
#     print("--- 正在启动 BigQuery 安全查询测试 ---")
#     result = google_bigquery_search.func(query=test_query)
#     print(result)