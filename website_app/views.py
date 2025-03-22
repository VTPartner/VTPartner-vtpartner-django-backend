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
mapKey = "AIzaSyD-vFDMqcEcgyeppWvGrAuhVymvF0Dxue0"

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
            result = None
            result = cursor.fetchall()
            
            # if result == []:
            #     raise ValueError("No Data Found")  # Custom error when no results are found

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
                WHERE category_id = %s AND city_id = %s AND price_type_id='1' 
                ORDER BY vtpartner.vehiclestbl.vehicle_id ASC
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
                SELECT category_id, category_name, category_type_id, category_image, category_type, epoch, description,website_background_image,attach_vehicle_background_image
                FROM vtpartner.categorytbl
                JOIN vtpartner.category_type_tbl ON category_type_tbl.cat_type_id = categorytbl.category_type_id
                ORDER BY category_id ASC
            """
            # Execute query
            result = select_query(query)

            # Check if result is empty
            if result == []:
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
                    "website_background_image": row[7],
                    "attach_vehicle_background_image": row[8],
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
            if result == []:
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
            if result == []:
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
            if result == []:
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
            if result == []:
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
            if result == []:
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
            if result == []:
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
            if result == []:
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
            print("response_data::",response_data)
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
            VALUES (%s, %s, %s, %s, %s, %s)
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

@csrf_exempt
def add_new_drivers_enquiry(request):
    try:
        data = json.loads(request.body)
        category_id = data.get("category_id")
        sub_cat_id = data.get("sub_cat_id")
        service_id = data.get("service_id")
        city_id = data.get("city_id")
        name = data.get("name")
        mobile_no = data.get("mobile_no")
        source_type = data.get("source_type")
        
        # Required fields dictionary
        required_fields = {
            "category_id": category_id,
            "sub_cat_id": sub_cat_id,
            "service_id": service_id,
            "city_id": city_id,
            "name": name,
            "mobile_no": mobile_no,
            "source_type": source_type
        }

        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Check for duplicates
        query_duplicate_check = """
            SELECT COUNT(*) FROM vtpartner.enquirytbl 
            WHERE name ILIKE %s AND category_id = %s
        """
        values_duplicate_check = [name, category_id]

        result = select_query(query_duplicate_check, values_duplicate_check)

        if result[0][0] > 0:
            return JsonResponse({"message": "Enquiry Request already exists"}, status=409)

        # Proceed with insertion if no duplicate
        query = """
            INSERT INTO vtpartner.enquirytbl 
            (category_id, city_id, name, mobile_no, source_type, sub_cat_id, service_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = [
            category_id,
            city_id,
            name,
            mobile_no,
            source_type,
            sub_cat_id,
            service_id
        ]
        
        row_count = insert_query(query, values)

        return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

    except Exception as err:
        print("Error executing add new enquiry query", err)
        return JsonResponse({"message": "Error executing add new enquiry query"}, status=500)

@csrf_exempt
def add_new_estimation_request(request):
    try:
        data = json.loads(request.body)
        # Extracting required fields from the request data
        category_id = data.get("category_id", -1)  
        sub_cat_id = data.get("sub_cat_id", -1)  
        service_id = data.get("service_id", -1)  
        start_address = data.get("start_address", "NA")
        end_address = data.get("end_address", "NA")
        work_description = data.get("work_description", "NA")
        name = data.get("name", "NA")
        mobile_no = data.get("mobile_no", "NA")
        purpose = data.get("purpose", "NA")
        hours = data.get("hours", 0.0)
        days = data.get("days", 0)
        city_id = data.get("city_id", -1)
        request_type = data.get("request_type")

        # Validating required fields (you can customize which fields are required)
        required_fields = {
            "category_id": category_id,
            "start_address": start_address,
            "name": name,
            "mobile_no": mobile_no,
            "city_id": city_id,
        }

        missing_fields = [field for field, value in required_fields.items() if value is None or value == ""]

        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Insert the new estimation request into the table
        query = """
            INSERT INTO vtpartner.estimation_request_tbl 
            (category_id, start_address, end_address, work_description, name, mobile_no, purpose, hours, days, city_id,request_type,sub_cat_id,service_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s)
        """
        values = [
            category_id,
            start_address,
            end_address,
            work_description,
            name,
            mobile_no,
            purpose,
            hours,
            days,
            city_id,
            request_type,
            sub_cat_id,
            service_id
        ]
        row_count = insert_query(query, values)
        print(f"{row_count} row(s) inserted")
        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

    except Exception as err:
        print("Error executing add new estimation request query", err)
        return JsonResponse({"message": "Error executing add new estimation request query"}, status=500)


@csrf_exempt
def check_allowed_pincode(request):
    if request.method == "POST":
        data = json.loads(request.body)
        pickup_place_id = data.get("pickupPlaceId")
        drop_place_id = data.get("dropPlaceId")
        city_id = data.get("city_id")  # Get city_id from the request payload

        # Check if both place IDs and city_id are provided
        if not pickup_place_id or not drop_place_id or not city_id:
            return JsonResponse({"error": "pickupPlaceId, dropPlaceId, and city_id are required."}, status=400)

        try:
            # Fetch details for both pickup and drop locations using Google Maps API
            pickup_response = requests.get(
                f"https://maps.googleapis.com/maps/api/place/details/json?place_id={pickup_place_id}&key={mapKey}"
            )
            drop_response = requests.get(
                f"https://maps.googleapis.com/maps/api/place/details/json?place_id={drop_place_id}&key={mapKey}"
            )
            
            print("pickup_response::",pickup_response)
            print("drop_response::",drop_response)

            pickup_data = pickup_response.json()
            drop_data = drop_response.json()

            print("pickup_data['status']::",pickup_data['status'])
            # Initialize pincodes
            pickup_pincode = None
            drop_pincode = None

            # Extract pincode from pickup location
            if pickup_data['status'] == 'OK':
                print("pickup_data['result']['address_components']::",pickup_data['result']['address_components'])
                for component in pickup_data['result']['address_components']:
                    if 'postal_code' in component['types']:
                        pickup_pincode = component['long_name']
                        print("pickup_pincode::",pickup_pincode)
                        break

            # Extract pincode from drop location
            if drop_data['status'] == 'OK':
                for component in drop_data['result']['address_components']:
                    if 'postal_code' in component['types']:
                        drop_pincode = component['long_name']
                        break
            print("pickup_pincode::",pickup_pincode)
            print("drop_pincode::",drop_pincode)
            # Check if both pincodes exist in the allowed_pincodes_tbl with status 1
            with connection.cursor() as cursor:
                query = """
                    SELECT COUNT(*) 
                    FROM vtpartner.allowed_pincodes_tbl 
                    WHERE pincode = %s AND city_id = %s AND status = 1
                """
                cursor.execute(query, [pickup_pincode, city_id])
                allowed_pickup_count = cursor.fetchone()[0]

                cursor.execute(query, [drop_pincode, city_id])
                allowed_drop_count = cursor.fetchone()[0]

            # Check if both pincodes are allowed
            if allowed_pickup_count > 0 and allowed_drop_count > 0:
                return JsonResponse({"message": "Both pincodes are allowed."}, status=200)
            else:
                return JsonResponse({"error": "One or both pincodes are not allowed."}, status=404)

        except Exception as e:
            print("Error checking allowed pincodes:", e)
            return JsonResponse({"error": "Error checking allowed pincodes"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def driver_form_print(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            # Extracting required fields from the request data
            driver_id = data.get("driver_id")  
            query = """
                SELECT goods_driver_id, driver_first_name, profile_pic, is_online, ratings,
                       mobile_no, goods_driverstbl.registration_date, goods_driverstbl.time,
                       r_lat, r_lng, current_lat, current_lng, status, recent_online_pic,
                       is_verified, goods_driverstbl.category_id, goods_driverstbl.vehicle_id,
                       city_id, aadhar_no, pan_card_no, goods_driverstbl.house_no,
                       goods_driverstbl.city_name, full_address, gender, goods_driverstbl.owner_id,
                       aadhar_card_front, aadhar_card_back, pan_card_front, pan_card_back,
                       license_front, license_back, insurance_image, noc_image,
                       pollution_certificate_image, rc_image, vehicle_image, vehicle_plate_image,
                       category_name, vehicle_name, category_type, driving_license_no,
                       vehicle_plate_no, rc_no, insurance_no, noc_no,
                       profile_photo AS owner_photo, owner_name, owner_mobile_no,
                       owner_tbl.house_no AS owner_house_no, owner_tbl.city_name AS owner_city_name,
                       owner_tbl.address AS owner_address, owner_tbl.profile_photo
                FROM vtpartner.goods_driverstbl
                LEFT JOIN vtpartner.categorytbl ON goods_driverstbl.category_id = categorytbl.category_id
                LEFT JOIN vtpartner.category_type_tbl ON categorytbl.category_type_id = category_type_tbl.cat_type_id
                LEFT JOIN vtpartner.vehiclestbl ON goods_driverstbl.vehicle_id = vehiclestbl.vehicle_id
                LEFT JOIN vtpartner.owner_tbl ON owner_tbl.owner_id = goods_driverstbl.owner_id
                WHERE goods_driver_id=%s ORDER BY goods_driver_id DESC
            """

            result = select_query(query,[driver_id])

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to the same column names from the database
            mapped_results = [
                {
                    "goods_driver_id": row[0],
                    "driver_first_name": row[1],
                    "profile_pic": row[2],
                    "is_online": row[3],
                    "ratings": row[4],
                    "mobile_no": row[5],
                    "registration_date": row[6],
                    "time": row[7],
                    "r_lat": row[8],
                    "r_lng": row[9],
                    "current_lat": row[10],
                    "current_lng": row[11],
                    "status": row[12],
                    "recent_online_pic": row[13],
                    "is_verified": row[14],
                    "category_id": row[15],
                    "vehicle_id": row[16],
                    "city_id": row[17],
                    "aadhar_no": row[18],
                    "pan_card_no": row[19],
                    "house_no": row[20],
                    "city_name": row[21],
                    "full_address": row[22],
                    "gender": row[23],
                    "owner_id": row[24],
                    "aadhar_card_front": row[25],
                    "aadhar_card_back": row[26],
                    "pan_card_front": row[27],
                    "pan_card_back": row[28],
                    "license_front": row[29],
                    "license_back": row[30],
                    "insurance_image": row[31],
                    "noc_image": row[32],
                    "pollution_certificate_image": row[33],
                    "rc_image": row[34],
                    "vehicle_image": row[35],
                    "vehicle_plate_image": row[36],
                    "category_name": row[37],
                    "vehicle_name": row[38],
                    "category_type": row[39],
                    "driving_license_no": row[40],
                    "vehicle_plate_no": row[41],
                    "rc_no": row[42],
                    "insurance_no": row[43],
                    "noc_no": row[44],
                    "owner_photo": row[45],
                    "owner_name": row[46],
                    "owner_mobile_no": row[47],
                    "owner_house_no": row[48],
                    "owner_city_name": row[49],
                    "owner_address": row[50],
                    "owner_profile_photo": row[51],
                }
                for row in result
            ]
            print("mapped_results::",mapped_results)
            return JsonResponse({"goods_drivers": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_faqs(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get("category_id")
            
            # List of required fields
            required_fields = {
                "category_id":category_id,
            }

            # Use the utility function to check for missing fields
            missing_fields = check_missing_fields(required_fields)

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )
            query = """
                select faqid,question,answer,time_at from vtpartner.faqtbl where category_id=%s order by faqid desc
            """

            result = select_query(query,[category_id])  # Assuming select_query returns a list of tuples
            print("result::",result)
            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                mapped_results.append({
                    "faq_id": row[0],
                    "question": row[1],
                    "answer": row[2],
                    "time_at": row[3],
                })

            return JsonResponse({"all_faqs_details": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)