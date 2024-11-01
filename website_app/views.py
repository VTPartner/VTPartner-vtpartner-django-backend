from django.shortcuts import render,redirect, reverse
from django.db import connection, DatabaseError
from django.views.decorators.cache import never_cache
from django.contrib.auth import logout
from django.http import HttpResponseServerError,JsonResponse,HttpResponse,HttpResponseRedirect
from colorama import Fore, Style
from django.views.decorators.cache import never_cache
from django.utils import timezone
from datetime import datetime
import pytz
import base64
import os
import uuid
import mimetypes
import requests
import json
import time
import re
from django.middleware.csrf import get_token
from django.views.decorators.csrf import csrf_exempt
from PIL import Image  # Pillow library for image processing
from django.db import connection, DatabaseError

# Create your views here.
mapKey = "AIzaSyAAlmEtjJOpSaJ7YVkMKwdSuMTbTx39l_o"

# Utility function to check for missing fields
def check_missing_fields(fields):
    missing_fields = [field for field, value in fields.items() if not value]
    print("missing_fields::",missing_fields)
    return missing_fields if missing_fields else None

#Common Functions
def select_query(query, params=None):
    """
    Executes a parameterized SQL select query and returns the result.
    
    Args:
        query (str): The SQL query to execute.
        params (list or tuple): Parameters to substitute into the query.

    Returns:
        list: Rows from the query result.

    Raises:
        ValueError: If no data is found.
        DatabaseError: For database-specific errors.
    """
    try:
        print("Select_Query::=>", query)
        print("Params::", params)
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()

            if not result:
                raise ValueError("No Data Found")  # Custom error when no results are found

            return result

    except ValueError as e:
        print(f"Error: {e}")
        raise  # Re-raise to be handled by calling function
    
    except DatabaseError as e:
        print("DatabaseError executing query:", e)
        raise  # Re-raise to be handled by calling function

    except Exception as e:
        print("Unexpected error:", e)
        raise  # Re-raise for unexpected errors


def update_query(query, params):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.rowcount

def insert_query(query, params):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)

            # If the query has a RETURNING clause, fetch the returned rows
            if cursor.description:
                return cursor.fetchall()  # Fetch all returned rows if any
            else:
                return cursor.rowcount  # Return number of affected rows
    except IntegrityError as e:
        print("Error: Failed to insert data due to integrity error", e)
        raise
    except Exception as e:
        print("Error executing query:", e)
        raise
    

@csrf_exempt
def fare_result(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get("category_id")
            city_id = data.get("city_id")

            # Check for missing fields
            required_fields = {"category_id": category_id, "city_id": city_id}
            missing_fields = check_missing_fields(required_fields)

            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )

            # SQL query
            query = """
                SELECT vtpartner.vehiclestbl.vehicle_id, vehicle_name, weight, size_image, starting_price_per_km
                FROM vtpartner.vehicle_city_wise_price_tbl
                JOIN vtpartner.vehiclestbl ON vtpartner.vehicle_city_wise_price_tbl.vehicle_id = vtpartner.vehiclestbl.vehicle_id
                WHERE category_id = %s AND city_id = %s
                ORDER BY weight ASC
            """
            values = [category_id, city_id]

            # Execute query using the helper function
            result = select_query(query, values)

            # Prepare fare result response
            fare_result = [
                {
                    "vehicle_id": row[0],
                    "vehicle_name": row[1],
                    "weight": row[2],
                    "size_image": row[3],
                    "starting_price_per_km": row[4],
                }
                for row in result
            ]

            return JsonResponse({"fare_result": fare_result}, status=200)

        except ValueError as e:
            # No data found
            return JsonResponse({"message": str(e)}, status=404)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_services(request):
    if request.method == "POST":
        try:
            # SQL query
            query = """
                SELECT category_id, category_name, category_type_id, category_image, category_type, epoch, description
                FROM vtpartner.categorytbl
                JOIN vtpartner.category_type_tbl ON category_type_tbl.cat_type_id = categorytbl.category_type_id
                ORDER BY category_id ASC
            """
            # Execute query
            result = select_query(query)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare services details response
            services_details = [
                {
                    "category_id": row[0],
                    "category_name": row[1],
                    "category_type_id": row[2],
                    "category_image": row[3],
                    "category_type": row[4],
                    "epoch": row[5],
                    "description": row[6],
                }
                for row in result
            ]

            return JsonResponse({"services_details": services_details}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)
        
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_allowed_cities(request):
    if request.method == "POST":
        try:
            # SQL query to get allowed cities
            query = """
                SELECT city_id, city_name, pincode, bg_image, time, pincode_until, description, status, covered_distance
                FROM vtpartner.available_citys_tbl
                ORDER BY city_id
            """

            # Execute query
            result = select_query(query)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare cities response
            cities = [
                {
                    "city_id": row[0],
                    "city_name": row[1],
                    "pincode": row[2],
                    "bg_image": row[3],
                    "time": row[4],
                    "pincode_until": row[5],
                    "description": row[6],
                    "status": row[7],
                    "covered_distance": row[8],
                }
                for row in result
            ]

            return JsonResponse({"cities": cities}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)
        
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_vehicles(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get("category_id")

            # Check for missing fields
            required_fields = {"category_id": category_id}
            missing_fields = check_missing_fields(required_fields)

            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )

            # SQL query to get vehicle details
            query = """
                SELECT vehicle_id, vehicle_name, weight, vehicle_types_tbl.vehicle_type_id, 
                       vehicle_types_tbl.vehicle_type_name, description, image, size_image
                FROM vtpartner.vehiclestbl
                JOIN vtpartner.vehicle_types_tbl ON vehiclestbl.vehicle_type_id = vehicle_types_tbl.vehicle_type_id
                WHERE category_id = %s
                ORDER BY vehicle_id DESC
            """
            values = [category_id]

            # Execute query
            result = select_query(query, values)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare vehicle details response
            vehicle_details = [
                {
                    "vehicle_id": row[0],
                    "vehicle_name": row[1],
                    "weight": row[2],
                    "vehicle_type_id": row[3],
                    "vehicle_type_name": row[4],
                    "description": row[5],
                    "image": row[6],
                    "size_image": row[7],
                }
                for row in result
            ]

            return JsonResponse({"vehicle_details": vehicle_details}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)
        
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_vehicles_with_price(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get("category_id")
            city_id = data.get("city_id")

            # Check for missing fields
            required_fields = {"category_id": category_id, "city_id": city_id}
            missing_fields = check_missing_fields(required_fields)

            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )

            # SQL query to get vehicle details with price
            query = """
                SELECT 
                    v.vehicle_id,
                    v.vehicle_name,
                    v.weight,
                    vt.vehicle_type_id,
                    vt.vehicle_type_name,
                    v.description,
                    v.image,
                    v.size_image,
                    vc.starting_price_per_km
                FROM 
                    vtpartner.vehiclestbl v
                JOIN 
                    vtpartner.vehicle_types_tbl vt ON v.vehicle_type_id = vt.vehicle_type_id
                LEFT JOIN 
                    vtpartner.vehicle_city_wise_price_tbl vc ON v.vehicle_id = vc.vehicle_id 
                WHERE 
                    v.category_id = %s AND vc.city_id = %s
                ORDER BY 
                    v.vehicle_id DESC
            """
            values = [category_id, city_id]

            # Execute query
            result = select_query(query, values)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare vehicle details response
            vehicle_details = [
                {
                    "vehicle_id": row[0],
                    "vehicle_name": row[1],
                    "weight": row[2],
                    "vehicle_type_id": row[3],
                    "vehicle_type_name": row[4],
                    "description": row[5],
                    "image": row[6],
                    "size_image": row[7],
                    "starting_price_per_km": row[8],
                }
                for row in result
            ]

            return JsonResponse({"vehicle_details": vehicle_details}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)
        
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_sub_categories(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get("category_id")

            # Check for missing fields
            required_fields = {"category_id": category_id}
            missing_fields = check_missing_fields(required_fields)

            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )

            # SQL query to get subcategories
            query = """
                SELECT 
                    sub_cat_id,
                    sub_cat_name,
                    cat_id,
                    image,
                    epoch_time
                FROM 
                    vtpartner.sub_categorytbl 
                WHERE 
                    cat_id = %s 
                ORDER BY 
                    sub_cat_id DESC
            """
            values = [category_id]

            # Execute query
            result = select_query(query, values)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare subcategories response
            sub_categories_details = [
                {
                    "sub_cat_id": row[0],
                    "sub_cat_name": row[1],
                    "cat_id": row[2],
                    "image": row[3],
                    "epoch_time": row[4],
                }
                for row in result
            ]

            return JsonResponse({"sub_categories_details": sub_categories_details}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)
        
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_other_services(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            sub_cat_id = data.get("sub_cat_id")

            # Check for missing fields
            required_fields = {"sub_cat_id": sub_cat_id}
            missing_fields = check_missing_fields(required_fields)

            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )

            # SQL query to get other services
            query = """
                SELECT 
                    service_id,
                    service_name,
                    sub_cat_id,
                    service_image,
                    time_updated
                FROM 
                    vtpartner.other_servicestbl 
                WHERE 
                    sub_cat_id = %s 
                ORDER BY 
                    sub_cat_id DESC
            """
            values = [sub_cat_id]

            # Execute query
            result = select_query(query, values)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare other services response
            other_services_details = [
                {
                    "service_id": row[0],
                    "service_name": row[1],
                    "sub_cat_id": row[2],
                    "service_image": row[3],
                    "time_updated": row[4],
                }
                for row in result
            ]

            return JsonResponse({"other_services_details": other_services_details}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)
        
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_delivery_gallery_images(request):
    if request.method == "POST":
        try:
            # SQL query to get delivery gallery images
            query = """
                SELECT 
                    gallery_id,
                    image_url,
                    category_type,
                    epoch
                FROM 
                    vtpartner.service_gallerytbl,
                    vtpartner.category_type_tbl 
                WHERE 
                    service_gallerytbl.category_type_id = category_type_tbl.cat_type_id 
                    AND service_gallerytbl.category_type_id = '1' 
                ORDER BY 
                    gallery_id ASC
            """

            # Execute query
            result = select_query(query)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare gallery data response
            gallery_data_delivery = [
                {
                    "gallery_id": row[0],
                    "image_url": row[1],
                    "category_type": row[2],
                    "epoch": row[3],
                }
                for row in result
            ]

            return JsonResponse({"gallery_data_delivery": gallery_data_delivery}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)
        
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_services_gallery_images(request):
    if request.method == "POST":
        try:
            # SQL query to get service gallery images
            query = """
                SELECT 
                    gallery_id,
                    image_url,
                    category_type,
                    epoch
                FROM 
                    vtpartner.service_gallerytbl,
                    vtpartner.category_type_tbl 
                WHERE 
                    service_gallerytbl.category_type_id = category_type_tbl.cat_type_id 
                    AND service_gallerytbl.category_type_id = '2' 
                ORDER BY 
                    gallery_id ASC
            """

            # Execute query
            result = select_query(query)

            # Check if result is empty
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Prepare gallery data response
            gallery_data_services = [
                {
                    "gallery_id": row[0],
                    "image_url": row[1],
                    "category_type": row[2],
                    "epoch": row[3],
                }
                for row in result
            ]

            return JsonResponse({"gallery_data_services": gallery_data_services}, status=200)

        except ValueError as err:
            return JsonResponse({"message": str(err)}, status=404)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def distance(request):
    if request.method == "GET":
        origins = request.GET.get("origins")
        destinations = request.GET.get("destinations")
        api_key = mapKey  # Make sure to define mapKey somewhere in your settings or context

        try:
            response = requests.get(
                f"https://maps.googleapis.com/maps/api/distancematrix/json?origins=place_id:{origins}&destinations=place_id:{destinations}&units=metric&key={api_key}"
            )
            response_data = response.json()

            return JsonResponse(response_data, status=200)

        except Exception as error:
            print("Error fetching distance data:", error)
            return JsonResponse({"error": "Error fetching distance data"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_new_enquiry(request):
    try:
        data = json.loads(request.body)
        category_id = data.get("category_id")
        vehicle_id = data.get("vehicle_id")
        city_id = data.get("city_id")
        name = data.get("name")
        mobile_no = data.get("mobile_no")
        source_type = data.get("source_type")
        # List of required fields
        required_fields = {
            "category_id":category_id,
            "vehicle_id":vehicle_id,
            "city_id":city_id,
            "name":name,
            "mobile_no":mobile_no,
            "source_type":source_type
        }

        # Use the utility function to check for missing fields
        missing_fields = check_missing_fields(required_fields)

        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Validating to avoid duplication
        query_duplicate_check = """
            SELECT COUNT(*) FROM vtpartner.enquirytbl 
            WHERE name ILIKE %s AND category_id = %s
        """
        values_duplicate_check = [name, category_id]

        result = select_query(query_duplicate_check, values_duplicate_check)

        # Check if the result is greater than 0 to determine if the enquiry already exists
        if result[0][0] > 0:
            return JsonResponse({"message": "Enquiry Request already exists"}, status=409)

        # If enquiry is not duplicate, proceed to insert
        query = """
            INSERT INTO vtpartner.enquirytbl 
            (category_id, vehicle_id, city_id, name, mobile_no, source_type) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = [
            category_id,
            vehicle_id,
            city_id,
            name,
            mobile_no,
            source_type,
        ]
        row_count = insert_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

    except Exception as err:
        print("Error executing add new enquiry query", err)
        return JsonResponse({"message": "Error executing add new enquiry query"}, status=500)
    

