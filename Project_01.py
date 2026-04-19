# =============================
# MongoDB Assignment - Clean Working Version
# =============================

from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

# -----------------------------
# 1. Load Environment Variables
# -----------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# -----------------------------
# 2. Connect to MongoDB
# -----------------------------
try:
    client = MongoClient(MONGO_URI)
    client.admin.command('ping')
    print("✅ Connected to MongoDB successfully")
except Exception as e:
    print("❌ Connection failed:", e)
    exit()

# -----------------------------
# 3. Select Database
# -----------------------------
db = client["college_db"]

# -----------------------------
# 4. Load JSON Data Function
# -----------------------------
def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# -----------------------------
# 5. Insert Data Function
# -----------------------------
def insert_data(collection_name, file_path):
    try:
        data = load_json(file_path)
        db[collection_name].delete_many({})
        db[collection_name].insert_many(data)
        print(f"✅ {collection_name} data inserted successfully")
    except Exception as e:
        print(f"Error inserting {collection_name}:", e)

# -----------------------------
# 6. Insert All Collections
# -----------------------------
insert_data("students", "students.json")
insert_data("faculty", "faculty.json")
insert_data("courses", "courses.json")
insert_data("enrollments", "enrollments.json")
insert_data("activities", "activities.json")

# -----------------------------
# 7. Query 1: Students with high attendance & skills
# -----------------------------
print("\n Query 1: Students with attendance > 85 and skills MongoDB & Python")

query1 = db.students.find(
    {
        "attendance": {"$gt": 85},
        "skills": {"$all": ["MongoDB", "Python"]}
    },
    {"name": 1, "attendance": 1, "skills": 1, "_id": 0}
)

for doc in query1:
    print(doc)

# -----------------------------
# 8. Query 2: Faculty teaching more than 2 courses
# -----------------------------
print("\n Query 2: Faculty teaching more than 2 courses")

pipeline2 = [
    {
        "$project": {
            "name": 1,
            "total_courses": {"$size": "$courses"}
        }
    },
    {
        "$match": {
            "total_courses": {"$gt": 2}
        }
    }
]

for doc in db.faculty.aggregate(pipeline2):
    print(doc)

# -----------------------------
# 9. Query 3: Student with enrolled courses (JOIN)
# -----------------------------
print("\n Query 3: Students with enrolled course names")

pipeline3 = [
    {
        "$lookup": {
            "from": "enrollments",
            "localField": "_id",
            "foreignField": "student_id",
            "as": "enrollments"
        }
    },
    {"$unwind": "$enrollments"},
    {
        "$lookup": {
            "from": "courses",
            "localField": "enrollments.course_id",
            "foreignField": "_id",
            "as": "course"
        }
    },
    {"$unwind": "$course"},
    {
        "$group": {
            "_id": "$name",
            "courses": {"$push": "$course.title"}
        }
    }
]

for doc in db.students.aggregate(pipeline3):
    print(doc)

# -----------------------------
# 10. Query 4: Course analytics
# -----------------------------
print("\n Query 4: Course analytics (students count & avg marks)")

pipeline4 = [
    {
        "$group": {
            "_id": "$course_id",
            "total_students": {"$sum": 1},
            "avg_marks": {"$avg": "$marks"}
        }
    },
    {
        "$lookup": {
            "from": "courses",
            "localField": "_id",
            "foreignField": "_id",
            "as": "course"
        }
    },
    {"$unwind": "$course"},
    {
        "$project": {
            "course_name": "$course.title",
            "total_students": 1,
            "avg_marks": 1,
            "_id": 0
        }
    }
]

for doc in db.enrollments.aggregate(pipeline4):
    print(doc)

# -----------------------------
# 11. Query 5: Top 3 students
# -----------------------------
print("\n Query 5: Top 3 students by average marks")

pipeline5 = [
    {
        "$group": {
            "_id": "$student_id",
            "avg_marks": {"$avg": "$marks"}
        }
    },
    {
        "$lookup": {
            "from": "students",
            "localField": "_id",
            "foreignField": "_id",
            "as": "student"
        }
    },
    {"$unwind": "$student"},
    {"$sort": {"avg_marks": -1}},
    {"$limit": 3},
    {
        "$project": {
            "name": "$student.name",
            "avg_marks": 1,
            "_id": 0
        }
    }
]

for doc in db.enrollments.aggregate(pipeline5):
    print(doc)

# -----------------------------
# 12. Query 6: Department analysis
# -----------------------------
print("\n Query 6: Students per department")

pipeline6 = [
    {
        "$group": {
            "_id": "$department",
            "total_students": {"$sum": 1}
        }
    },
    {"$sort": {"total_students": -1}}
]

results = list(db.students.aggregate(pipeline6))

for doc in results:
    print(doc)

if results:
    print("\n Department with highest students:")
    print(results[0])

# -----------------------------
# END
# -----------------------------
