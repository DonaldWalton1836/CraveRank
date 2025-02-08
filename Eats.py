from flask import Flask, request, jsonify
import requests
import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()
YELP_API_KEY = os.getenv("YELP_API_KEY")
YELP_API_URL = os.getenv("YELP_API_URL")
HEADERS = {
    "Authorization": f"Bearer {YELP_API_KEY}"
}

def database_connection():
    connect = mysql.connector.connect(
        host='sql5.freesqldatabase.com',
        user='sql5757119',
        password='1JclsHjhAz',
        database='sql5757119',
        connection_timeout=300
    )
    return connect

app=Flask(__name__)
 
#Function to get resutrants
def get_restaurants(location, categories=None, min_rating = 0, limit = 10, radius = 5000, price = None):
    params={
        "term":"restaurants",
        "location":location,
        "categories": categories,
        "limit":limit,
        "radius":radius,
        "sort_by":"rating", 
    }
    if price:
        params["price"] = price
    response = requests.get(YELP_API_URL, headers=HEADERS, params=params)

    if response.status_code == 200:
        businesses = response.json()['businesses']
        filtered_businesses = [business for business in businesses if business['rating'] >= min_rating]
        insert_yelp_data(filtered_businesses)
        return filtered_businesses
    else:
        return{"error": f"Error {response.status_code} from Yelp API"} 


def insert_yelp_data(businesses):
    connect = database_connection()
    cursor = connect.cursor()

    for business in businesses:
        cursor.execute('SELECT restaurant_id FROM Restaurants WHERE yelp_id =%s', (business['id'],))
        existing_restaurant = cursor.fetchone()

        if not existing_restaurant:
            cursor.execute('''
                INSERT INTO Restaurants (name, address, city, state, zip_code, latitude, longitude, rating, price_range, phone, website, yelp_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                business['name'],
                business['location'].get('address', ''),
                business['location'].get('city', ''),
                business['location'].get('state', ''),
                business['location'].get('zip_code', ''),
                business['coordinates'].get('latitude', None),
                business['coordinates'].get('longitude', None),
                business['rating'],
                business.get('price', None),
                business.get('phone', ''),
                business.get('url', ''),
                business['id']
            ))
            restaurant_id = cursor.lastrowid

            #Insert categories
            for category in business['categories']:
                cursor.execute('SELECT category_id FROM Categories WHERE name =%s' ,(category['title'],))
                category_id = cursor.fetchone()

                if not category_id:
                    (cursor.execute(''' INSERT INTO Categories (name) VALUES (%s) ''', (category['title'],)))
                    category_id = cursor.lastrowid
                else:
                    category_id = category_id[0]
                cursor.execute(''' INSERT INTO RestaurantCategories (restaurant_id, category_id) VALUES (%s, %s) ''',( restaurant_id, category_id))

            
            #Insert reviews
            if 'reviews' in business:
                for review in business['review']:
                    cursor.execute('SELECT user_id FROM Users WHERE username = %s', (review['user']['name'],))
                    user_id = cursor.fetchone()

                    if not user_id:
                        cursor.execute(''' INSERT INTO Users (username) VALUES (%s)''', (review['user']['name'],))
                        user_id[0]
                        cursor.execute('''INSERT INTO Reviews (user_id, restaurant_id, review_content, rating, created_at, updated_at) VALUES (%s, %s, %s, %s, NOW(), NOW()) ''',
                                   (user_id, restaurant_id,review['text'], review['rating']))

            connect.commit()
    connect.close()
#Handle Resurant Search
@app.route("/search_restaurants", methods=["GET"])
def search_restaurants():
    location = request.args.get("location")
    category = request.args.get("category")
    min_rating = request.args.get("min_rating", 0, type=float)
    limit = request.args.get("limit", 10, type=int)
    radius = request.args.get("radius", 5000, type=int)
    price = request.args.get("price")

    if not location:
        return jsonify({"error": "Location is required"}), 400
    
    result = get_restaurants(location, category, min_rating, limit, radius, price)

    if "error" in result:
        return jsonify(result), 500
    else:
        return jsonify(result)
    
if __name__ == "__main__":
    app.run(debug=True)



                                   
                        




    

#YELP_API_URL = 'https://api.yelp.com/v3/businesses/search'


