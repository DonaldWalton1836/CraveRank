from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
import json
import psycopg2
from psycopg2.extras import Json
from flask_cors import CORS
import os 
import ast  # For safely evaluating string literals into Python dictionaries


app = Flask(__name__)
CORS(app)

def database_connection():
    try:
        print("Attempting to connect to the database...")
        conn = psycopg2.connect(
            host="cr.chie24ys0emx.us-east-1.rds.amazonaws.com", 
            port="5432", 
            user="CR1", 
            password="Crave0413*", 
            dbname="craverank"
        )
        print("Connection established")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        return None

@app.route("/test_db_connection", methods=["GET"])
def test_db_connection():
    conn = database_connection()
    if conn:
        return "Database connection successful!"
    else:
        return "Database connection failed!"
    
@app.route("/test_db", methods=["GET"])
def test_db():
    conn = database_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500
    else:
        conn.close()
        return jsonify({"message": "Database connection successful!"})

# Test route
@app.route("/test", methods=["GET"])
def test():
    return "Flask is working!"

# Load JSON data line by line (Memory Efficient)
def load_json_data(file_path):
    """Load JSON data from file line by line to avoid memory overload."""
    try:
        with open(file_path, 'r', encoding="utf-8") as file:
            for line in file:
                try:
                    yield json.loads(line)  # Process line-by-line
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except Exception as e:
        print(f"Error opening file: {e}")
        return None
    
# Test loading JSON data to confirm that it loads correctly
@app.route("/load_test", methods=["GET"])
def load_test():
    file_path = '/Users/jaynaspikes/Downloads/Yelp JSON/yelp_academic_dataset_business.json'
    try:
        # Convert the generator to a list
        data = list(load_json_data(file_path))

        # Show the first 5 records of the data for testing
        return jsonify(data[:5])

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def insert_restaurants(data):
    conn = database_connection()
    if conn is None:
        print("Database connection failed during insertion.")
        return False  # Stop if the connection fails

    cursor = conn.cursor()

    query = """
    INSERT INTO restaurants (business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, categories)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (business_id) DO NOTHING;
    """

    try:
        for restaurant in data:
            # Ensure we're only inserting restaurants
            categories = restaurant.get("categories", "")
            
            # Skip if the business is not a restaurant
            if not categories or "Restaurant" not in categories:
                continue  # Skip non-restaurant businesses

            print(f"Inserting: {restaurant['name']}")  # Log what is being inserted

            # Ensure categories is a valid list or empty list if it's None or empty
            categories_list = categories.split(", ") if isinstance(categories, str) else []
            categories_array = "{" + ",".join(f'"{cat.strip()}"' for cat in categories_list) + "}" if categories_list else "{}"

            # Handle missing or None values in the restaurant data by providing default values
            business_id = restaurant.get("business_id", None)
            name = restaurant.get("name", None)
            address = restaurant.get("address", None)
            city = restaurant.get("city", None)
            state = restaurant.get("state", None)
            postal_code = restaurant.get("postal_code", None)
            latitude = restaurant.get("latitude", None)
            longitude = restaurant.get("longitude", None)
            stars = restaurant.get("stars", None)
            review_count = restaurant.get("review_count", None)
            is_open = True if restaurant.get("is_open") == 1 else False

            # Insert into the database (basic information)
            cursor.execute(query, (
                business_id,
                name,
                address,
                city,
                state,
                postal_code,
                latitude,
                longitude,
                stars,
                review_count,
                is_open,  # Ensure is_open is a boolean
                categories_array  # Insert the formatted categories array
            ))

        conn.commit()
        return True

    except Exception as e:
        print(f"Error inserting data: {e}")  # Log insertion errors
        return False

    finally:
        cursor.close()
        conn.close()



@app.route("/restaurants/search", methods=["GET"])
def search_restaurants():
    """Search for restaurants based only on state."""
    state = request.args.get("state", "").upper()  # Convert state to uppercase

    if not state:
        return jsonify({"error": "State is required"}), 400  # Return an error if state is not provided

    print(f"Searching for state: {state}")  # Debugging log to ensure we are receiving the state

    conn = database_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    cursor = conn.cursor()

    # Build search query with only the state filter
    search_query = """
    SELECT business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, categories
    FROM restaurants
    WHERE LOWER(state) = LOWER(%s)
    LIMIT 20;
    """

    cursor.execute(search_query, (state.lower(),))  # Only use the state as a filter
    results = cursor.fetchall()

    print(f"Found {len(results)} results for state: {state}")  # Debugging log to check how many results

    cursor.close()
    conn.close()

    if not results:
        return jsonify({"error": f"No restaurants found for the state: {state}"}), 404  # Enhanced error message

    # Convert query results into JSON format
    restaurants = []
    for row in results:
        restaurants.append({
            "business_id": row[0],
            "name": row[1],
            "address": row[2],
            "city": row[3],
            "state": row[4],
            "postal_code": row[5],
            "latitude": row[6],
            "longitude": row[7],
            "stars": row[8],
            "review_count": row[9],
            "categories": row[10]
        })

    return jsonify({"restaurants": restaurants})



@app.route("/upload", methods=["POST"])
def upload_data():
    data = request.get_json()
    file_path = data.get("file_path")  # Get the file path from the request body

    if not file_path:
        return jsonify({"error": "File path is required"}), 400

    if not os.path.exists(file_path):
        return jsonify({"error": f"File not found: {file_path}"}), 400

    json_data = load_json_data(file_path)
    if json_data is None:
        return jsonify({"error": "Failed to read JSON file"}), 500

    success = insert_restaurants(json_data)

    if success:
        return jsonify({"message": "Data inserted successfully"})
    else:
        return jsonify({"error": "Database connection failed"}), 500

@app.route('/')
def home():
    return "Welcome to CraveRank"

if __name__ == "__main__":
    app.run(debug=True)

@app.route("/signup", methods=["POST"])
def signup():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    conn = database_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    cursor = conn.cursor()

    # This will check if username already exists
    cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
    if cursor.fetchone():
        return jsonify({"error": "Username already exists"}), 409

    # This will hash the password and store the information within the database.
    hashed_password = generate_password_hash(password)

    try:
        cursor.execute(
            "INSERT INTO users (username, password) VALUES (%s, %s)",
            (username, hashed_password)
        )
        conn.commit()
        return jsonify({"message": "User registered successfully"}), 201
    except Exception as e:
        print(f"Error during signup: {e}")
        return jsonify({"error": "Error registering user"}), 500
    finally:
        cursor.close()
        conn.close()

@app.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    conn = database_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    cursor = conn.cursor()
    cursor.execute("SELECT password FROM users WHERE username = %s", (username,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result is None:
        return jsonify({"error": "Invalid username or password"}), 401

    stored_hashed_pw = result[0]
    if check_password_hash(stored_hashed_pw, password):
        return jsonify({"message": "Login successful"}), 200
    else:
        return jsonify({"error": "Invalid username or password"}), 401

#YELP_API_URL = 'https://api.yelp.com/v3/businesses/search'