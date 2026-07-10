# Databricks notebook source
from jinja2 import Template

# COMMAND ----------

parameters = [
    {
        "table" : "spotify_catalog.silver.factstream",
        "alias" : "factstream",
        "cols" : "factstream.stream_id, factstream.listen_duration"
    },
    {
        "table" : "spotify_catalog.silver.dimuser",
        "alias" : "dimuser",
        "cols" : "dimuser.user_id, dimuser.user_name",
        "condition" : "factstream.user_id = dimuser.user_id"
    },
    {
        "table" : "spotify_catalog.silver.dimtrack",
        "alias" : "dimtrack",
        "cols" : "dimtrack.track_id, dimtrack.track_name",
        "condition" : "factstream.track_id = dimtrack.track_id"
    }
]

# COMMAND ----------

query_text = """

            SELECT 
                {% for param in parameters %}
                    {{ param.cols }} 
                        {% if not loop.last %}
                            , 
                        {% endif %}
                {% endfor %}
            FROM 
                {% for param in parameters %}
                    {% if loop.first %}
                        {{ param['table'] }} AS {{ param['alias'] }}
                    {% endif %}
                {% endfor %}
                {% for param in parameters %}
                    {% if not loop.first %}
                    LEFT JOIN 
                        {{ param['table'] }} AS {{ param['alias'] }} 
                    ON 
                        {{ param['condition'] }}
                    {% endif %}
                {% endfor %}
            
"""

# COMMAND ----------

jinja_sql_str = Template(query_text)
query = jinja_sql_str.render(parameters=parameters)
print(query)

# COMMAND ----------

display(spark.sql(query))

# COMMAND ----------

df = spark.sql(query)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("spotify_catalog.gold.jinja_join")

# COMMAND ----------

