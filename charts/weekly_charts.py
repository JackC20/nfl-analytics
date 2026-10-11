# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly NFL charts
# MAGIC
# MAGIC Run by the `nfl-weekly` job after the pipeline refresh. It renders every chart from the
# MAGIC gold tables and shows them below, so they can be viewed on the run page. The job's
# MAGIC success email links straight here. To save one for posting, open the image and save it.
# MAGIC
# MAGIC The charts themselves are defined in `nfl_charts.py`; this notebook only displays them.

# COMMAND ----------

import os
import sys

# The notebook's folder holds nfl_charts.py and scatter.py. It is normally the
# working directory already; adding it to the import path makes that certain.
sys.path.insert(0, os.getcwd())

from IPython.display import Image, Markdown, display  # noqa: E402

from nfl_charts import render  # noqa: E402

title, paths = render()

# COMMAND ----------

display(Markdown(f"## {title}"))
for path in paths:
    display(Image(filename=str(path)))
