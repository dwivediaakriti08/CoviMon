import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt

# Step 1: Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["bigdata_db"]
collection = db["covid_data"]

# Step 2: Load OWID dataset
df = pd.read_csv("owid_covid_data.csv")

# Keep only useful columns
df = df[["date", "location", "new_cases", "new_deaths", "total_cases", "total_deaths"]]

# Convert date to string (for MongoDB storage)
df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

# Step 3: Insert into MongoDB
data_dict = df.to_dict("records")
collection.delete_many({})  # Clear old data
collection.insert_many(data_dict)
print("OWID Data Inserted Successfully!")

# ------------------ ANALYSIS ------------------

# 1. Total cases per country
pipeline_cases = [
    {"$group": {"_id": "$location", "total_cases": {"$max": "$total_cases"}}},
    {"$sort": {"total_cases": -1}},
    {"$limit": 10}
]
top_countries_cases = list(collection.aggregate(pipeline_cases))
print("\nTop 10 Countries by Total Cases:")
for country in top_countries_cases:
    print(country)

# 2. Total deaths per country
pipeline_deaths = [
    {"$group": {"_id": "$location", "total_deaths": {"$max": "$total_deaths"}}},
    {"$sort": {"total_deaths": -1}},
    {"$limit": 10}
]
top_countries_deaths = list(collection.aggregate(pipeline_deaths))
print("\n Top 10 Countries by Total Deaths:")
for country in top_countries_deaths:
    print(country)

# 3. Daily cases trend for India
india_data = list(collection.find({"location": "India"}, {"date": 1, "new_cases": 1, "_id": 0}))
india_df = pd.DataFrame(india_data)

plt.figure(figsize=(10, 5))
plt.plot(india_df["date"], india_df["new_cases"], label="India Daily Cases")
plt.xticks(rotation=45)
plt.xlabel("Date")
plt.ylabel("New Cases")
plt.title(" Daily COVID-19 Cases in India")
plt.legend()
plt.tight_layout()
plt.show()
