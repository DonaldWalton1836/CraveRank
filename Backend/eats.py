from flask import Flask, request, jsonify
import json
from flask_cors import CORS
from pymongo import MongoClient
from bson import ObjectId  # Import ObjectId from bson

app = Flask(__name__)

# Database connection function (MongoDB)
def database_connection():
    # Connect to MongoDB Atlas
    client = MongoClient("mongodb+srv://jaynaspikes53:STOHwWSzeeZjgVMk@craverank-1.dq9ic.mongodb.net/CraveRank_Restaurants?retryWrites=true&w=majority")
    db = client.get_database("CraveRank_Restaurants")  # Connect to CraveRank_Restaurants database
    return db

#Test route to confirm Flask is working
@app.route("/test", methods=["GET"])
def test():
    return "Flask is working!"

# Function to load JSON data from a file (line by line)
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = []
        for line in file:
            try:
                data.append(json.loads(line))  # Parse each line as a JSON object
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        return data

# Test loading JSON data to confirm that it loads correctly
@app.route("/load_test", methods=["GET"])
def load_test():
    file_path = '/Users/jaynaspikes/Downloads/Yelp JSON/yelp_academic_dataset_business.json'
    try:
        data = load_json_data(file_path)
        # Show the first 5 records of the data for testing
        return jsonify(data[:5])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Test query parameters to make sure Flask is receiving them correctly
@app.route("/query_test", methods=["GET"])
def query_test():
    location = request.args.get("location", "Unknown Location")
    category = request.args.get("category", "Unknown Category")
    min_rating = request.args.get("min_rating", "0", type=float)
    return jsonify({
        "location": location,
        "category": category,
        "min_rating": min_rating
    })

#Function to convert ObjectId to string
def mongo_to_dict(mongo_obj):
    """
    Recursively convert MongoDB document's ObjectId to string.
    This ensures all ObjectId fields are converted to string before returning them as JSON.
    """
    if isinstance(mongo_obj, dict):
        return {key: mongo_to_dict(value) for key, value in mongo_obj.items()}
    elif isinstance(mongo_obj, list):
        return [mongo_to_dict(item) for item in mongo_obj]
    elif isinstance(mongo_obj, ObjectId):
        return str(mongo_obj)
    else:
        return mongo_obj

@app.route("/search_restaurants", methods=["GET"])
def search_restaurants():
    location = request.args.get("location")
    category = request.args.get("category")
    min_rating = request.args.get("min_rating", 0, type=float)

    try:
        # Debugging: Log the query parameters
        print(f"Location: {location}, Category: {category}, Min Rating: {min_rating}")

        # Connect to MongoDB
        db = database_connection()
        collection = db.Restaurants  # Collection name in MongoDB

        # Build MongoDB query dynamically
        query = {"categories": {"$regex": "Restaurants", "$options": "i"}}  # Match 'Restaurants' in categories

        # Debugging: Check if location is present and adjust query accordingly
        if location:
            print(f"Searching for location (state) = {location}")
            query["state"] = {"$regex": location, "$options": "i"}  # Case-insensitive search for state (location)
        if category:
            print(f"Searching for category = {category}")
            query["categories"] = {"$regex": category, "$options": "i"}  # Case-insensitive search for category
        if min_rating:
            print(f"Searching for minimum rating = {min_rating}")
            query["stars"] = {"$gte": min_rating}  # Filter by minimum rating

        # Perform MongoDB query
        cursor = collection.find(query)  # No limit 

        # Convert the cursor to a list of businesses
        businesses = list(cursor)

        # Debugging: Log the number of businesses found and show the first few
        print(f"Found {len(businesses)} businesses.")
        if businesses:
            print("First few results:")
            for business in businesses[:5]:  # Print the first 5 businesses for inspection
                print(business)
        else:
            print("No businesses found.")

        # Convert ObjectId fields to string before returning
        businesses = mongo_to_dict(businesses)

        if not businesses:
            return jsonify({"error": "No businesses found."}), 404

        # Return filtered businesses as JSON
        return jsonify(businesses)

    except Exception as e:
        # If there's any exception, log it and return a 500 error
        print(f"Error occurred: {e}")
        return jsonify({"error": "An error occurred while processing the request."}), 500



@app.route('/')
def home():
    return "Welcome to CraveRank"

if __name__ == "__main__":
    app.run(debug=True)



                                   
                        




    

#YELP_API_URL = 'https://api.yelp.com/v3/businesses/search'


