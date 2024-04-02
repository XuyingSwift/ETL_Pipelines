from extraction.extraction_functional import  extracted_data

df_parquet, df_table, df_csv, df_api, df_web_page= extracted_data()
print("parquet data source")
print(df_parquet.head())
print("news.db source")
print(df_table.head())
print("csv data source")
print(df_csv)
print("df_api data source")
print(df_api)
print("web page data source")
print(df_web_page)
