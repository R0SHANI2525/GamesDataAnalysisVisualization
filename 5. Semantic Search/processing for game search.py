# Generated from: processing for game search.ipynb
# Converted at: 2026-02-24T05:09:01.133Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType


# --- 1. SETUP PATHS ---
PATH_SCORES      = "/Volumes/workspace/default/rawdata/steam/combined_reviews.csv"
PATH_REVIEWS     = "/Volumes/workspace/default/rawdata/steam/processed/reviews.csv"
PATH_APPS_MAIN   = "/Volumes/workspace/default/rawdata/steam/applications.csv"
PATH_APPS_SHOTS  = "/Volumes/workspace/default/rawdata/steam/processed/appid and screenshots.csv"
PATH_OUTPUT_BASE = "/Volumes/workspace/default/rawdata/steam/processed/combined_reviews"


# 1. Load your uploaded CSV (combined_reviews.csv)
# (Make sure to upload the file to DBFS or use the correct path)
df_local = spark.read.csv("/Volumes/workspace/default/rawdata/steam/combined_reviews.csv", header=True, inferSchema=True)

# 2. Load the main Reviews table (the one that HAS the appid)
# Replace 'reviews' with your actual table name if it's different
df_main =  (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load("/Volumes/workspace/default/rawdata/steam/processed/reviews.csv")
)

# 3. Join them on 'recommendationid' to get the 'app_id'
# Note: Check if your table uses 'app_id' or 'appid'. I use 'app_id' below.
df_joined = df_local.join(df_main, "recommendationid", "left") \
    .select(
        df_local["*"],                 # Keep your score/category
        df_main["appid"].alias("appid") # Add the App ID
    )

# 4. Save the result to a single CSV file
# We use coalesce(1) to force it into one file for easy download
output_path = "/Volumes/workspace/default/rawdata/steam/processed/combined_reviews_with_appid"
df_joined.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

print(f"✅ Success! File saved to: {output_path}")
print("Go to the DBFS File Browser, download the part-00000...csv file, rename it to 'combined_reviews_fixed.csv', and give it to the bot.")

# --- 2. GET TARGET LIST (Games to process) ---
print("⏳ Building Target Game List...")
df_local = spark.read.csv(PATH_SCORES, header=True, inferSchema=False)
df_reviews = spark.read.option("header", "true").csv(PATH_REVIEWS)

# Filter IDs to be numbers only
df_local = df_local.filter(F.col("recommendationid").rlike("^[0-9]+$"))
df_reviews = df_reviews.filter(F.col("appid").rlike("^[0-9]+$"))

# Join to get the list of AppIDs
df_targets = df_local.join(df_reviews, "recommendationid", "left").select(
    df_reviews["appid"]
).distinct().filter(F.col("appid").isNotNull())



# --- 3. GENERATE FILE 1: IMAGES (One row per app) ---
print("🖼️ Generating Images File (Header & Background)...")

df_apps_main = spark.read.option("header", "true").option("multiLine", "true").csv(PATH_APPS_MAIN)
# Clean column names
for c in df_apps_main.columns: df_apps_main = df_apps_main.withColumnRenamed(c, c.strip())

df_img_out = df_targets.join(df_apps_main, "appid", "inner").select(
    F.col("appid"),
    F.col("header_image"),
    F.col("background")
).distinct()

# Save
df_img_out.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{PATH_OUTPUT_BASE}_images")






# --- 4. GENERATE FILE 2: SCREENSHOTS (Multiple rows per app - URLs only) ---
print("📸 Generating Screenshots File (Extracted URLs)...")

df_shots_raw = spark.read.option("header", "true").option("multiLine", "true").option("escape", "\"").csv(PATH_APPS_SHOTS)
# Clean column names
for c in df_shots_raw.columns: df_shots_raw = df_shots_raw.withColumnRenamed(c, c.strip())

# Define Schema to parse the JSON string
# We only care about "path_full" inside the array of objects
json_schema = ArrayType(StructType([
    StructField("path_full", StringType())
]))

# 1. Join with targets
# 2. Parse the JSON string into an Array
# 3. Explode the Array (Create 1 row per screenshot)
# 4. Select the URL
df_shots_out = df_targets.join(df_shots_raw, "appid", "inner") \
    .withColumn("parsed_json", F.from_json(F.col("screenshots"), json_schema)) \
    .withColumn("exploded_shot", F.explode(F.col("parsed_json"))) \
    .select(
        F.col("appid"),
        F.col("exploded_shot.path_full").alias("screenshot_url")
    )

# Save
df_shots_out.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{PATH_OUTPUT_BASE}_screenshots")

print("✅ DONE! You have two files to download:")
print(f"1. {PATH_OUTPUT_BASE}_images (Headers/Backgrounds)")
print(f"2. {PATH_OUTPUT_BASE}_screenshots (Clean URLs)")

df_shots_out.display(5)

df_img_out.display(5)

# --- MERGE SCORES + IMAGES INTO ONE MASTER FILE ---
print("🖼️ Generating Master File (Scores + AppID + Images)...")

# 1. Prepare the Applications Data
df_apps_main = spark.read.option("header", "true").option("multiLine", "true").csv(PATH_APPS_MAIN)
for c in df_apps_main.columns: df_apps_main = df_apps_main.withColumnRenamed(c, c.strip())

# 2. Re-create the joined reviews (Scores + AppID)
# We join 'df_local' (your scores) with 'df_reviews' (the bridge)
df_joined_scores = df_local.join(df_reviews, "recommendationid", "left").select(
    df_local["*"],
    df_reviews["appid"]
).filter(F.col("appid").isNotNull())

# 3. Join Scores with Images
# We use a LEFT join so we don't lose any review scores even if the game is missing an image
df_master = df_joined_scores.join(df_apps_main, "appid", "left").select(
    df_joined_scores["*"],
    df_apps_main["header_image"],
    df_apps_main["background"]
)

# 4. Save
path_master = f"{PATH_OUTPUT_BASE}_master"
df_master.coalesce(1).write.mode("overwrite").option("header", "true").csv(path_master)

print(f"✅ Master File Saved: {path_master}")
print("   (This file contains Scores, AppIDs, and Images all in one)")

# --- CLEANUP: REMOVE _c0 COLUMN ---
PATH_MASTER = "/Volumes/workspace/default/rawdata/steam/processed/combined_reviews_master"

print(f"🧹 Cleaning file at: {PATH_MASTER} ...")

# 1. Read the file
df_master = spark.read.option("header", "true").option("multiLine", "true").csv(PATH_MASTER)

# 2. Drop the unwanted column
if "_c0" in df_master.columns:
    df_clean = df_master.drop("_c0", "category")
    print("   ✅ Found and dropped '_c0' column.")
else:
    df_clean = df_master
    print("   ℹ️ '_c0' column not found (already clean).")

# 3. Overwrite the file with the clean version
df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(PATH_MASTER)

print("✅ Cleanup Done! You can now download the clean file.")