# Databricks notebook source
df_user = spark.table("spotify_catalog.gold.dimuser")
df_artist = spark.table("spotify_catalog.gold.dimartist")
df_date = spark.table("spotify_catalog.gold.dimdate")
df_track = spark.table("spotify_catalog.gold.dimtrack")
df_factstream = spark.table("spotify_catalog.gold.factstream")
df_jinja = spark.table("spotify_catalog.gold.jinja_join")

# COMMAND ----------

output_path = "abfss://gold@spotifyadlsdata.dfs.core.windows.net/"

df_user.write.mode("overwrite").parquet(output_path+"DimUser")
df_artist.write.mode("overwrite").parquet(output_path+"DimArtist")
df_date.write.mode("overwrite").parquet(output_path+"DimDate")
df_track.write.mode("overwrite").parquet(output_path+"DimTrack")
df_factstream.write.mode("overwrite").parquet(output_path+"FactStream")
df_jinja.write.mode("overwrite").parquet(output_path+"JinjaJoin")