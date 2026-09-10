import snowflake.connector
conn = snowflake.connector.connect(connection_name='MyConnection')
conn.cursor().execute('USE ROLE SALES_STREAMLIT_RL')
conn.cursor().execute('USE WAREHOUSE SNOWADHOC')
cs = conn.cursor()
src = '/Users/nathomas/Cortex Code Projects/Peak Qualify and Commit/peak_app_sis.py'
cs.execute(f"PUT 'file://{src}' @SNOWPUBLIC.STREAMLIT.PEAK_QC_FORECAST_STAGE OVERWRITE=TRUE AUTO_COMPRESS=FALSE")
print(cs.fetchall())
conn.close()
print('Done')
