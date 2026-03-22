import os
def update_repo():
    path = 'src/dao/sec_edgar_repo.py'
    with open(path, 'r') as f:
        content = f.read()
    content = content.replace('get_latest_ts_df_by_figi', 'get_latest_ts_df_by_cik')
    content = content.replace('QUERY_LATEST_TS_BY_FIGI_SQL', 'QUERY_LATEST_TS_BY_CIK_SQL')
    content = content.replace('按 FIGI 分组', '按 CIK 分组')
    with open(path, 'w') as f:
        f.write(content)

update_repo()
