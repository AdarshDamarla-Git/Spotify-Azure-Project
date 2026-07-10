# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG spotify_catalog 
# MAGIC MANAGED LOCATION 'abfss://root@spotifyadlsdata.dfs.core.windows.net/';