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
from django.db.utils import IntegrityError
from django.conf import settings


from PIL import Image  # Pillow library for image processing

# Utility function to check for missing fields
def check_missing_fields(fields):
    missing_fields = [field for field, value in fields.items() if not value]
    return missing_fields if missing_fields else None


#Common Functions 
def execute_raw_query(query, params=None,):
    
    result = []
    try:
        print(f"{Fore.GREEN}Query Executed: {query}{Style.RESET_ALL}")
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
            print(f"Result: {result}, Result length: {len(result)}")
        return result
    except DatabaseError as e:
        print(f"{Fore.RED}DatabaseError Found: {e}{Style.RESET_ALL}")
        # Need To Handle the error appropriately, such as logging or raising a custom exception
        # roll back transactions if needed
        
        return 500
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Handle other unexpected errors
        return 500
    finally:
        # Ensure the cursor is closed to release resources
        cursor.close()  # Note: cursor might not be defined if an exception occurs earlier

def execute_raw_query_fetch_one(query, params=None,):
    
    result = []
    try:
        print(f"{Fore.GREEN}Query Executed: {query}{Style.RESET_ALL}")
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            print(f"Result: {result}")
        return result
    except DatabaseError as e:
        print(f"{Fore.RED}DatabaseError Found: {e}{Style.RESET_ALL}")
        # Need To Handle the error appropriately, such as logging or raising a custom exception
        # roll back transactions if needed
        
        return 500
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Handle other unexpected errors
        return 500
    finally:
        # Ensure the cursor is closed to release resources
        cursor.close()  # Note: cursor might not be defined if an exception occurs earlier

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

def upload_images(request):
    try:
        if request.method == "POST":
            data = json.loads(request.body)
            uploaded_image = data.get("vtPartnerImage")
            # Generate a unique identifier for the image
            unique_identifier = str(uuid.uuid4())

            # Extract the file extension from the uploaded image
            file_extension = mimetypes.guess_extension(uploaded_image.content_type)

            # Construct the custom image name with the unique identifier and original extension
            custom_image_name = f'img_{unique_identifier}{file_extension}'
            # Assuming you have a MEDIA_ROOT where the images will be stored
            file_path = os.path.join(settings.MEDIA_ROOT, custom_image_name)

            # Open the uploaded image using Pillow
            img = Image.open(uploaded_image)
            img_resized = img.resize((300, 300))
            # Save the resized image
            img_resized.save(file_path)

            # Assuming you have a MEDIA_URL configured
            image_url = os.path.join(settings.MEDIA_URL, custom_image_name)
            print(f'Uploaded Image URL: {image_url}')
    except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
    return JsonResponse({"image_url": image_url}, status=200)
    

def epochToDateTime(epoch):
    datetime_obj = datetime.utcfromtimestamp(epoch)
    gmt_plus_0530 = pytz.timezone('Asia/Kolkata')
    datetime_obj_gmt_plus_0530 = datetime_obj.replace(tzinfo=pytz.utc).astimezone(gmt_plus_0530)
    deliveryEpoch = datetime_obj_gmt_plus_0530.strftime('%Y-%m-%d %I:%M:%S %p')
    return deliveryEpoch

# Create your views here.
@csrf_exempt
def login_view(request):
    if request.method == "POST":
        data = json.loads(request.body)
        email = data.get('email')
        password = data.get('password')
        
        # Use parameterized query to prevent SQL injection
        query = "SELECT admin_id, admin_name, email, password,admin_role FROM vtpartner.admintbl WHERE email = '"+str(email)+"' AND password = '"+str(password)+"'"
        user_data = execute_raw_query_fetch_one(query)  # Pass parameters as a tuple
        
        if user_data:
            # User is authorized
            print('User is Authorized')
            
            # Create response data
            response_data = {
                'token': 'your_generated_jwt_token_here',  # You need to generate a JWT token
                'user': {
                    'id': user_data[0],
                    'role':user_data[3],
                    'name': user_data[1],
                    'email': user_data[2],
                },
            }
            return JsonResponse(response_data, status=200)
        else:
            return JsonResponse({'message': 'No Data Found'}, status=404)

    # If not a POST request, you can return a different response
    return JsonResponse({'message': 'Method not allowed'}, status=500)

@csrf_exempt
def all_branches(request):
    if request.method == "POST":
        data = json.loads(request.body)
        admin_id = data.get("admin_id")

        # Verify token
        # token_verified = verify_token(request)  # Adjust this function as per your implementation
        # if not token_verified:
        #     return JsonResponse({"message": "Unauthorized"}, status=401)

        try:
            query = """
            SELECT 
                branchtbl.branch_id, 
                branch_name, 
                location, 
                city_id, 
                branchtbl.reg_date, 
                creation_time, 
                branch_status 
            FROM 
                vtpartner.branchtbl, vtpartner.admintbl 
            WHERE 
                admintbl.branch_id = branchtbl.branch_id 
                AND admin_id = %s
            """
            params = [admin_id]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)
            
            # Map the results to a list of dictionaries with meaningful keys
            branches = [
                {
                    "branch_id": row[0],
                    "branch_name": row[1],
                    "location": row[2],
                    "city_id": row[3],
                    "reg_date": row[4],
                    "creation_time": row[5],
                    "branch_status": row[6],
                }
                for row in result
            ]

            # Return user branch data
            return JsonResponse({"branches": branches}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_allowed_cities(request):
    if request.method == "POST":
        try:
            query = """
            SELECT 
                city_id, 
                city_name, 
                pincode, 
                bg_image, 
                time, 
                pincode_until, 
                description, 
                status 
            FROM 
                vtpartner.available_citys_tbl 
            ORDER BY 
                city_id DESC
            """
            result = select_query(query)  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries with meaningful keys
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
                }
                for row in result
            ]

            return JsonResponse({"cities": cities}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_allowed_city(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            city_id = data.get("city_id")
            city_name = data.get("city_name")
            pincode = data.get("pincode")
            pincode_until = data.get("pincode_until")
            description = data.get("description")
            bg_image = data.get("bg_image")
            status = data.get("status")

            # List of required fields
            required_fields = {
                "city_id": city_id,
                "city_name": city_name,
                "pincode": pincode,
                "pincode_until": pincode_until,
                "description": description,
                "bg_image": bg_image,
                "status": status,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            query = """
                UPDATE vtpartner.available_citys_tbl 
                SET city_name = %s, pincode = %s, bg_image = %s, 
                    pincode_until = %s, description = %s, 
                    time = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), 
                    status = %s 
                WHERE city_id = %s
            """
            params = [
                city_name,
                pincode,
                bg_image,
                pincode_until,
                description,
                status,
                city_id,
            ]

            row_count = update_query(query, params)
            return JsonResponse({"message": f"{row_count} rows updated"}, status=200)

        except Exception as err:
            print("Error executing allowed city UPDATE query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def add_new_allowed_city(request):
    if request.method == "POST":
        try:
            # Extracting data from the request body
            data = json.loads(request.body)
            city_name = data.get('city_name')
            pincode = data.get('pincode')
            pincode_until = data.get('pincode_until')
            description = data.get('description')
            bg_image = data.get('bg_image')

            # List of required fields
            required_fields = {
                'city_name': city_name,
                'pincode': pincode,
                'pincode_until': pincode_until,
                'description': description,
                'bg_image': bg_image,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            # Prepare the insert query
            query = """
                INSERT INTO vtpartner.available_citys_tbl (city_name, pincode, bg_image, pincode_until, description)
                VALUES (%s, %s, %s, %s, %s)
            """
            params = [city_name, pincode, bg_image, pincode_until, description]
            row_count = insert_query(query, params)

            return JsonResponse({"message": f"{row_count} rows inserted"}, status=201)

        except Exception as err:
            print("Error executing add new allowed city query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_allowed_pincodes(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            city_id = data.get("city_id")

            query = """
                SELECT 
                    pincode_id, 
                    allowed_pincodes_tbl.pincode, 
                    creation_time, 
                    allowed_pincodes_tbl.status 
                FROM 
                    vtpartner.allowed_pincodes_tbl 
                WHERE 
                    allowed_pincodes_tbl.city_id = %s 
                ORDER BY 
                    pincode_id DESC
            """
            result = select_query(query, [city_id])

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            pincodes = [
                {
                    "pincode_id": row[0],
                    "pincode": row[1],
                    "creation_time": row[2],
                    "status": row[3],
                }
                for row in result
            ]

            return JsonResponse({"pincodes": pincodes}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def add_new_pincode(request):
    if request.method == "POST":
        try:
            # Extracting data from the request body
            data = json.loads(request.body)
            city_id = data.get('city_id')
            pincode = data.get('pincode')
            pincode_status = data.get('pincode_status')

            # List of required fields
            required_fields = {
                'city_id': city_id,
                'pincode': pincode,
                'pincode_status': pincode_status,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) FROM vtpartner.allowed_pincodes_tbl WHERE pincode ILIKE %s
            """
            values_duplicate_check = [pincode]
            result = select_query(query_duplicate_check, values_duplicate_check)

            # Check if the result is greater than 0 to determine if the pincode already exists
            if result[0]['count'] > 0:
                return JsonResponse({"message": "Pincode already exists"}, status=409)

            # If pincode is not duplicate, proceed to insert
            query = """
                INSERT INTO vtpartner.allowed_pincodes_tbl (pincode, city_id, status) VALUES (%s, %s, %s)
            """
            params = [pincode, city_id, pincode_status]
            row_count = insert_query(query, params)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error executing add new allowed pincodes query:", err)
            return JsonResponse({"message": "Error executing add new allowed pincodes query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def edit_pincode(request):
    if request.method == "POST":
        try:
            # Extracting data from the request body
            data = json.loads(request.body)
            city_id = data.get('city_id')
            pincode = data.get('pincode')
            pincode_status = data.get('pincode_status')
            pincode_id = data.get('pincode_id')

            # List of required fields
            required_fields = {
                'city_id': city_id,
                'pincode': pincode,
                'pincode_status': pincode_status,
                'pincode_id': pincode_id,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) FROM vtpartner.allowed_pincodes_tbl WHERE pincode ILIKE %s AND city_id = %s AND pincode_id != %s
            """
            values_duplicate_check = [pincode, city_id, pincode_id]
            result = select_query(query_duplicate_check, values_duplicate_check)

            # Check if the result is greater than 0 to determine if the pincode already exists
            if result[0]['count'] > 0:
                return JsonResponse({"message": "Pincode already exists"}, status=409)

            # Update the pincode
            query = """
                UPDATE vtpartner.allowed_pincodes_tbl SET pincode = %s, city_id = %s, status = %s, creation_time = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) WHERE pincode_id = %s
            """
            values = [pincode, city_id, pincode_status, pincode_id]
            row_count = update_query(query, values)

            # Check if any rows were updated
            if row_count > 0:
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
            else:
                return JsonResponse({"message": "Pincode not found"}, status=404)

        except Exception as err:
            print("Error executing updating allowed pincodes query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def service_types(request):
    if request.method == "POST":
        try:
            query = """
                SELECT cat_type_id, category_type FROM vtpartner.category_type_tbl
            """
            result = select_query(query)  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_type_details = [
                {
                    "cat_type_id": row['cat_type_id'],
                    "category_type": row['category_type']
                }
                for row in result
            ]

            return JsonResponse({"services_type_details": services_type_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_services(request):
    if request.method == "POST":
        try:
            query = """
                SELECT 
                    category_id, 
                    category_name, 
                    category_type_id, 
                    category_image, 
                    category_type, 
                    epoch, 
                    description 
                FROM 
                    vtpartner.categorytbl
                JOIN 
                    vtpartner.category_type_tbl 
                ON 
                    category_type_tbl.cat_type_id = categorytbl.category_type_id 
                ORDER BY 
                    category_id DESC
            """
            result = select_query(query)  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "category_id": row['category_id'],
                    "category_name": row['category_name'],
                    "category_type_id": row['category_type_id'],
                    "category_image": row['category_image'],
                    "category_type": row['category_type'],
                    "epoch": row['epoch'],
                    "description": row['description'],
                }
                for row in result
            ]

            return JsonResponse({"services_details": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def add_service(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)  # Parse JSON request body
            category_name = data.get("category_name")
            category_type_id = data.get("category_type_id")
            category_image = data.get("category_image")
            description = data.get("description")

            # List of required fields
            required_fields = {
                "category_name": category_name,
                "category_type_id": category_type_id,
                "category_image": category_image,
                "description": description,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            # Validate to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) FROM vtpartner.categorytbl WHERE category_name ILIKE %s
            """
            result = select_query(query_duplicate_check, [category_name])  # Assuming select_query is defined

            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Service Name already exists"}, status=409)

            # If not duplicate, proceed to insert
            query = """
                INSERT INTO vtpartner.categorytbl (category_name, category_type_id, category_image, description)
                VALUES (%s, %s, %s, %s)
            """
            row_count = insert_query(query, [category_name, category_type_id, category_image, description])  # Assuming insert_query is defined

            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error executing add new category query:", err)
            return JsonResponse({"message": "Error executing add new category query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def edit_service(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)  # Parse JSON request body
            category_id = data.get("category_id")
            category_name = data.get("category_name")
            category_type_id = data.get("category_type_id")
            category_image = data.get("category_image")
            description = data.get("description")

            # List of required fields
            required_fields = {
                "category_id": category_id,
                "category_name": category_name,
                "category_type_id": category_type_id,
                "category_image": category_image,
                "description": description,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            # Validate to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) FROM vtpartner.categorytbl WHERE category_name ILIKE %s AND category_id != %s
            """
            result = select_query(query_duplicate_check, [category_name, category_id])  # Assuming select_query is defined

            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Service Name already exists"}, status=409)

            # If not duplicate, proceed to update
            query = """
                UPDATE vtpartner.categorytbl 
                SET category_name = %s, category_type_id = %s, category_image = %s, 
                    epoch = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), description = %s 
                WHERE category_id = %s
            """
            row_count = update_query(query, [category_name, category_type_id, category_image, description, category_id])  # Assuming update_query is defined

            if row_count > 0:
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
            else:
                return JsonResponse({"message": "Category not found"}, status=404)

        except Exception as err:
            print("Error executing updating category query:", err)
            return JsonResponse({"message": "Error executing updating category query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def vehicle_types(request):
    if request.method == "POST":
        try:
            query = "SELECT vehicle_type_id, vehicle_type_name FROM vtpartner.vehicle_types_tbl"
            result = select_query(query)  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            vehicle_type_details = [
                {
                    "vehicle_type_id": row['vehicle_type_id'],
                    "vehicle_type_name": row['vehicle_type_name'],
                }
                for row in result
            ]

            return JsonResponse({"vehicle_type_details": vehicle_type_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_vehicles(request):
    if request.method == "POST":
        try:
            body = json.loads(request.body)
            category_id = body.get("category_id")

            # List of required fields
            required_fields = {
                "category_id": category_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming check_missing_fields is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            query = """
                SELECT vehicle_id, vehicle_name, weight, vehicle_types_tbl.vehicle_type_id,
                       vehicle_types_tbl.vehicle_type_name, description, image, size_image
                FROM vtpartner.vehiclestbl
                JOIN vtpartner.vehicle_types_tbl ON vehiclestbl.vehicle_type_id = vehicle_types_tbl.vehicle_type_id
                WHERE category_id = %s
                ORDER BY vehicle_id DESC
            """
            result = select_query(query, [category_id])  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            vehicle_details = [
                {
                    "vehicle_id": row['vehicle_id'],
                    "vehicle_name": row['vehicle_name'],
                    "weight": row['weight'],
                    "vehicle_type_id": row['vehicle_type_id'],
                    "vehicle_type_name": row['vehicle_type_name'],
                    "description": row['description'],
                    "image": row['image'],
                    "size_image": row['size_image'],
                }
                for row in result
            ]

            return JsonResponse({"vehicle_details": vehicle_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def add_vehicle(request):
    if request.method == "POST":
        try:
            body = json.loads(request.body)
            category_id = body.get("category_id")
            vehicle_name = body.get("vehicle_name")
            weight = body.get("weight")
            vehicle_type_id = body.get("vehicle_type_id")
            description = body.get("description")
            image = body.get("image")
            size_image = body.get("size_image")

            # List of required fields
            required_fields = {
                "category_id": category_id,
                "vehicle_name": vehicle_name,
                "weight": weight,
                "vehicle_type_id": vehicle_type_id,
                "description": description,
                "image": image,
                "size_image": size_image,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming check_missing_fields is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) FROM vtpartner.vehiclestbl WHERE vehicle_name ILIKE %s
            """
            result = select_query(query_duplicate_check, [vehicle_name])  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the vehicle name already exists
            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Vehicle Name already exists"}, status=409)

            # If vehicle name is not duplicate, proceed to insert
            query = """
                INSERT INTO vtpartner.vehiclestbl 
                (vehicle_name, weight, vehicle_type_id, description, image, size_image, category_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                vehicle_name,
                weight,
                vehicle_type_id,
                description,
                image,
                size_image,
                category_id,
            ]
            row_count = insert_query(query, values)  # Assuming insert_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error executing add new vehicle query:", err)
            return JsonResponse({"message": "Error executing add new vehicle query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def edit_vehicle(request):
    if request.method == "POST":
        try:
            body = json.loads(request.body)
            category_id = body.get("category_id")
            vehicle_id = body.get("vehicle_id")
            vehicle_name = body.get("vehicle_name")
            weight = body.get("weight")
            vehicle_type_id = body.get("vehicle_type_id")
            description = body.get("description")
            image = body.get("image")
            size_image = body.get("size_image")

            # List of required fields
            required_fields = {
                "category_id": category_id,
                "vehicle_id": vehicle_id,
                "vehicle_name": vehicle_name,
                "weight": weight,
                "vehicle_type_id": vehicle_type_id,
                "description": description,
                "image": image,
                "size_image": size_image,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming check_missing_fields is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) FROM vtpartner.vehiclestbl WHERE vehicle_name ILIKE %s AND vehicle_id != %s
            """
            result = select_query(query_duplicate_check, [vehicle_name, vehicle_id])  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the vehicle name already exists
            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Vehicle Name already exists"}, status=409)

            # If vehicle name is not duplicate, proceed to update
            query = """
                UPDATE vtpartner.vehiclestbl 
                SET vehicle_name = %s, weight = %s, vehicle_type_id = %s, 
                    description = %s, image = %s, size_image = %s, category_id = %s 
                WHERE vehicle_id = %s
            """
            values = [
                vehicle_name,
                weight,
                vehicle_type_id,
                description,
                image,
                size_image,
                category_id,
                vehicle_id,
            ]
            row_count = update_query(query, values)  # Assuming update_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing updating vehicle query:", err)
            return JsonResponse({"message": "Error executing updating vehicle query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def vehicle_prices(request):
    if request.method == "POST":
        try:
            body = json.loads(request.body)
            vehicle_id = body.get("vehicle_id")

            # List of required fields
            required_fields = {
                "vehicle_id": vehicle_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming check_missing_fields is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            query = """
                SELECT price_id, vehicle_city_wise_price_tbl.city_id, vehicle_city_wise_price_tbl.vehicle_id,
                       starting_price_per_km, minimum_time, vehicle_city_wise_price_tbl.price_type_id,
                       city_name, price_type, bg_image, time_created_at
                FROM vtpartner.available_citys_tbl
                JOIN vtpartner.vehicle_city_wise_price_tbl ON vehicle_city_wise_price_tbl.city_id = available_citys_tbl.city_id
                JOIN vtpartner.vehiclestbl ON vehicle_city_wise_price_tbl.vehicle_id = vehiclestbl.vehicle_id
                JOIN vtpartner.vehicle_price_type_tbl ON vehicle_price_type_tbl.price_type_id = vehicle_city_wise_price_tbl.price_type_id
                WHERE vehicle_city_wise_price_tbl.vehicle_id = %s
                ORDER BY city_name
            """
            result = select_query(query, [vehicle_id])  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for a clearer JSON response
            vehicle_price_details = [
                {
                    "price_id": row['price_id'],
                    "city_id": row['city_id'],
                    "vehicle_id": row['vehicle_id'],
                    "starting_price_per_km": row['starting_price_per_km'],
                    "minimum_time": row['minimum_time'],
                    "price_type_id": row['price_type_id'],
                    "city_name": row['city_name'],
                    "price_type": row['price_type'],
                    "bg_image": row['bg_image'],
                    "time_created_at": row['time_created_at'],
                }
                for row in result
            ]

            return JsonResponse({"vehicle_price_details": vehicle_price_details}, status=200)

        except Exception as err:
            print("Error executing vehicle_prices query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def vehicle_price_types(request):
    if request.method == "POST":
        try:
            query = """
                SELECT price_type_id, price_type 
                FROM vtpartner.vehicle_price_type_tbl
            """
            result = select_query(query)  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for a clearer JSON response
            vehicle_price_types = [
                {
                    "price_type_id": row['price_type_id'],
                    "price_type": row['price_type'],
                }
                for row in result
            ]

            return JsonResponse({"vehicle_price_types": vehicle_price_types}, status=200)

        except Exception as err:
            print("Error executing vehicle_price_types query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def add_vehicle_price(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            city_id = data.get('city_id')
            vehicle_id = data.get('vehicle_id')
            starting_price_km = data.get('starting_price_km')
            minimum_time = data.get('minimum_time')
            price_type_id = data.get('price_type_id')

            # List of required fields
            required_fields = {
                'city_id': city_id,
                'vehicle_id': vehicle_id,
                'starting_price_km': starting_price_km,
                'minimum_time': minimum_time,
                'price_type_id': price_type_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) 
                FROM vtpartner.available_citys_tbl,
                     vtpartner.vehicle_city_wise_price_tbl 
                WHERE available_citys_tbl.city_id = vehicle_city_wise_price_tbl.city_id 
                AND available_citys_tbl.city_id = %s 
                AND vehicle_id = %s 
                AND price_type_id = %s
            """
            values_duplicate_check = (city_id, vehicle_id, price_type_id)
            result = select_query(query_duplicate_check, values_duplicate_check)  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the entry already exists
            if result and result[0][0] > 0:
                print("City Name already exists")
                return JsonResponse({"message": "City Name already exists"}, status=409)

            # Proceed to insert the new price
            query = """
                INSERT INTO vtpartner.vehicle_city_wise_price_tbl 
                (city_id, vehicle_id, starting_price_per_km, minimum_time, price_type_id) 
                VALUES (%s, %s, %s, %s, %s)
            """
            values = (city_id, vehicle_id, starting_price_km, minimum_time, price_type_id)
            row_count = insert_query(query, values)  # Assuming insert_query is defined

            # Send success response for insertion
            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error executing add new price to vehicle query:", err)
            return JsonResponse({"message": "Error executing add new price to vehicle query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def edit_vehicle_price(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            price_id = data.get('price_id')
            city_id = data.get('city_id')
            vehicle_id = data.get('vehicle_id')
            starting_price_km = data.get('starting_price_km')
            minimum_time = data.get('minimum_time')
            price_type_id = data.get('price_type_id')

            # List of required fields
            required_fields = {
                'city_id': city_id,
                'vehicle_id': vehicle_id,
                'starting_price_km': starting_price_km,
                'minimum_time': minimum_time,
                'price_type_id': price_type_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) 
                FROM vtpartner.available_citys_tbl,
                     vtpartner.vehicle_city_wise_price_tbl 
                WHERE available_citys_tbl.city_id = vehicle_city_wise_price_tbl.city_id 
                AND available_citys_tbl.city_id = %s 
                AND vehicle_id = %s 
                AND price_id != %s 
                AND price_type_id = %s
            """
            values_duplicate_check = (city_id, vehicle_id, price_id, price_type_id)
            result = select_query(query_duplicate_check, values_duplicate_check)  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the entry already exists
            if result and result[0][0] > 0:
                return JsonResponse({"message": "City Name already exists"}, status=409)

            # Proceed to update the price
            query = """
                UPDATE vtpartner.vehicle_city_wise_price_tbl 
                SET city_id = %s, 
                    vehicle_id = %s, 
                    starting_price_per_km = %s, 
                    minimum_time = %s, 
                    price_type_id = %s,
                    time_created_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) 
                WHERE price_id = %s
            """
            values = (city_id, vehicle_id, starting_price_km, minimum_time, price_type_id, price_id)
            row_count = update_query(query, values)  # Assuming update_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing updating price to vehicle query:", err)
            return JsonResponse({"message": "Error executing updating price to vehicle query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_sub_categories(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get('category_id')

            # List of required fields
            required_fields = {
                'category_id': category_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Query to get subcategories
            query = """
                SELECT sub_cat_id, sub_cat_name, cat_id, image, epoch_time 
                FROM vtpartner.sub_categorytbl 
                WHERE cat_id = %s 
                ORDER BY sub_cat_id DESC
            """
            values = (category_id,)
            result = select_query(query, values)  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for clearer response
            sub_categories_details = [
                {
                    "sub_cat_id": row['sub_cat_id'],
                    "sub_cat_name": row['sub_cat_name'],
                    "cat_id": row['cat_id'],
                    "image": row['image'],
                    "epoch_time": row['epoch_time'],
                }
                for row in result
            ]

            return JsonResponse({"sub_categories_details": sub_categories_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def add_sub_category(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get('category_id')
            sub_cat_name = data.get('sub_cat_name')
            image = data.get('image')

            # List of required fields
            required_fields = {
                'category_id': category_id,
                'sub_cat_name': sub_cat_name,
                'image': image,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) 
                FROM vtpartner.sub_categorytbl 
                WHERE LOWER(sub_cat_name) = LOWER(%s) AND cat_id = %s
            """
            values_duplicate_check = (sub_cat_name, category_id)
            result = select_query(query_duplicate_check, values_duplicate_check)  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the sub-category already exists
            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Sub Category Name already exists"}, status=409)

            # Proceed to insert the new sub-category
            query = """
                INSERT INTO vtpartner.sub_categorytbl (sub_cat_name, cat_id, image) 
                VALUES (%s, %s, %s)
            """
            values = (sub_cat_name, category_id, image)
            row_count = insert_query(query, values)  # Assuming insert_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error executing add new sub category query:", err)
            return JsonResponse({"message": "Error executing add new sub category query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def edit_sub_category(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get('category_id')
            sub_cat_id = data.get('sub_cat_id')
            sub_cat_name = data.get('sub_cat_name')
            image = data.get('image')

            # List of required fields
            required_fields = {
                'category_id': category_id,
                'sub_cat_id': sub_cat_id,
                'sub_cat_name': sub_cat_name,
                'image': image,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) 
                FROM vtpartner.sub_categorytbl 
                WHERE LOWER(sub_cat_name) = LOWER(%s) AND sub_cat_id != %s
            """
            values_duplicate_check = (sub_cat_name, sub_cat_id)
            result = select_query(query_duplicate_check, values_duplicate_check)  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the sub-category already exists
            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Sub Category Name already exists"}, status=409)

            # Proceed to update the sub-category
            query = """
                UPDATE vtpartner.sub_categorytbl 
                SET sub_cat_name = %s, cat_id = %s, image = %s, epoch_time = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
                WHERE sub_cat_id = %s
            """
            values = (sub_cat_name, category_id, image, sub_cat_id)
            row_count = update_query(query, values)  # Assuming update_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing updating sub category query:", err)
            return JsonResponse({"message": "Error executing updating sub category query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_other_services(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            sub_cat_id = data.get('sub_cat_id')

            # List of required fields
            required_fields = {
                'sub_cat_id': sub_cat_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            query = """
                SELECT service_id, service_name, sub_cat_id, service_image, time_updated 
                FROM vtpartner.other_servicestbl 
                WHERE sub_cat_id = %s 
                ORDER BY service_id DESC  -- Changed to order by service_id for better clarity
            """
            values = (sub_cat_id,)
            result = select_query(query, values)  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for clearer response
            other_services_details = [
                {
                    "service_id": row['service_id'],
                    "service_name": row['service_name'],
                    "sub_cat_id": row['sub_cat_id'],
                    "service_image": row['service_image'],
                    "time_updated": row['time_updated'],
                }
                for row in result
            ]

            return JsonResponse({"other_services_details": other_services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def add_other_service(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            service_name = data.get('service_name')
            sub_cat_id = data.get('sub_cat_id')
            service_image = data.get('service_image')

            # List of required fields
            required_fields = {
                'service_name': service_name,
                'sub_cat_id': sub_cat_id,
                'service_image': service_image,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) 
                FROM vtpartner.other_servicestbl 
                WHERE service_name ILIKE %s AND sub_cat_id = %s
            """
            values_duplicate_check = (service_name, sub_cat_id)

            result = select_query(query_duplicate_check, values_duplicate_check)  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the service name already exists
            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Service Name already exists"}, status=409)

            # If service name is not duplicate, proceed to insert
            query = """
                INSERT INTO vtpartner.other_servicestbl (service_name, sub_cat_id, service_image) 
                VALUES (%s, %s, %s)
            """
            values = (service_name, sub_cat_id, service_image)
            row_count = insert_query(query, values)  # Assuming insert_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error executing add new other Service query:", err)
            return JsonResponse({"message": "Error executing add new other Service query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def edit_other_service(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            service_id = data.get('service_id')
            service_name = data.get('service_name')
            sub_cat_id = data.get('sub_cat_id')
            service_image = data.get('service_image')

            # List of required fields
            required_fields = {
                'service_id': service_id,
                'service_name': service_name,
                'sub_cat_id': sub_cat_id,
                'service_image': service_image,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Validating to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) 
                FROM vtpartner.other_servicestbl 
                WHERE service_name ILIKE %s AND service_id != %s
            """
            values_duplicate_check = (service_name, service_id)

            result = select_query(query_duplicate_check, values_duplicate_check)  # Assuming select_query is defined

            # Check if the result is greater than 0 to determine if the service name already exists
            if result and result[0]['count'] > 0:
                return JsonResponse({"message": "Service Name already exists"}, status=409)

            # If service name is not duplicate, proceed to update
            query = """
                UPDATE vtpartner.other_servicestbl 
                SET service_name = %s, sub_cat_id = %s, service_image = %s, time_updated = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) 
                WHERE service_id = %s
            """
            values = (service_name, sub_cat_id, service_image, service_id)
            row_count = update_query(query, values)  # Assuming update_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing updating other service query:", err)
            return JsonResponse({"message": "Error executing updating other service query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_enquiries(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get('category_id')

            # List of required fields
            required_fields = {
                'category_id': category_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            query = """
                SELECT enquiry_id, enquirytbl.category_id, enquirytbl.sub_cat_id, 
                       enquirytbl.service_id, enquirytbl.vehicle_id, enquirytbl.city_id, 
                       name, mobile_no, time_at, source_type, enquirytbl.status, 
                       category_name, sub_cat_name, service_name, city_name, vehicle_name
                FROM vtpartner.enquirytbl 
                LEFT JOIN vtpartner.categorytbl ON enquirytbl.category_id = categorytbl.category_id 
                LEFT JOIN vtpartner.sub_categorytbl ON enquirytbl.sub_cat_id = sub_categorytbl.sub_cat_id 
                LEFT JOIN vtpartner.other_servicestbl ON enquirytbl.service_id = other_servicestbl.service_id 
                LEFT JOIN vtpartner.vehiclestbl ON enquirytbl.vehicle_id = vehiclestbl.vehicle_id 
                LEFT JOIN vtpartner.available_citys_tbl ON enquirytbl.city_id = available_citys_tbl.city_id 
                WHERE enquirytbl.category_id = %s 
                ORDER BY enquiry_id DESC
            """
            values = (category_id,)

            result = select_query(query, values)  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a structured format for clarity
            all_enquiries_details = [
                {
                    "enquiry_id": row['enquiry_id'],
                    "category_id": row['category_id'],
                    "sub_cat_id": row['sub_cat_id'],
                    "service_id": row['service_id'],
                    "vehicle_id": row['vehicle_id'],
                    "city_id": row['city_id'],
                    "name": row['name'],
                    "mobile_no": row['mobile_no'],
                    "time_at": row['time_at'],
                    "source_type": row['source_type'],
                    "status": row['status'],
                    "category_name": row['category_name'],
                    "sub_cat_name": row['sub_cat_name'],
                    "service_name": row['service_name'],
                    "city_name": row['city_name'],
                    "vehicle_name": row['vehicle_name'],
                }
                for row in result
            ]

            return JsonResponse({"all_enquiries_details": all_enquiries_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_gallery_images(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_type_id = data.get('category_type_id')

            # List of required fields
            required_fields = {
                'category_type_id': category_type_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            query = """
                SELECT gallery_id, image_url, category_type, epoch 
                FROM vtpartner.service_gallerytbl
                JOIN vtpartner.category_type_tbl 
                ON service_gallerytbl.category_type_id = category_type_tbl.cat_type_id 
                WHERE service_gallerytbl.category_type_id = %s
            """
            values = (category_type_id,)

            result = select_query(query, values)  # Assuming select_query is defined

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a structured format for clarity
            gallery_images_data = [
                {
                    "gallery_id": row['gallery_id'],
                    "image_url": row['image_url'],
                    "category_type": row['category_type'],
                    "epoch": row['epoch'],
                }
                for row in result
            ]

            return JsonResponse({"gallery_images_data": gallery_images_data}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def add_gallery_image(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            image_url = data.get('image_url')
            category_type_id = data.get('category_type_id')

            # List of required fields
            required_fields = {
                'image_url': image_url,
                'category_type_id': category_type_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Insert the new gallery image
            query = """
                INSERT INTO vtpartner.service_gallerytbl (image_url, category_type_id) 
                VALUES (%s, %s)
            """
            values = (image_url, category_type_id)

            row_count = insert_query(query, values)  # Assuming insert_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error executing add new gallery image query:", err)
            return JsonResponse({"message": "Error executing add new gallery image query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def edit_gallery_image(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            image_url = data.get('image_url')
            gallery_id = data.get('gallery_id')

            # List of required fields
            required_fields = {
                'image_url': image_url,
                'gallery_id': gallery_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming this function is defined

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Update the gallery image
            query = """
                UPDATE vtpartner.service_gallerytbl 
                SET image_url = %s, epoch = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) 
                WHERE gallery_id = %s
            """
            values = (image_url, gallery_id)
            row_count = update_query(query, values)  # Assuming update_query is defined

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing updating gallery image query:", err)
            return JsonResponse({"message": "Error executing updating gallery image query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def enquiries_all(request):
    if request.method == "POST":
        try:
            query = """
                SELECT enquiry_id, enquirytbl.category_id, enquirytbl.sub_cat_id,
                       enquirytbl.service_id, enquirytbl.vehicle_id, enquirytbl.city_id,
                       name, mobile_no, enquirytbl.time_at, source_type, enquirytbl.status,
                       category_name, sub_cat_name, service_name, city_name, vehicle_name,
                       category_type
                FROM vtpartner.enquirytbl
                LEFT JOIN vtpartner.categorytbl ON enquirytbl.category_id = categorytbl.category_id
                LEFT JOIN vtpartner.sub_categorytbl ON enquirytbl.sub_cat_id = sub_categorytbl.sub_cat_id
                LEFT JOIN vtpartner.other_servicestbl ON enquirytbl.service_id = other_servicestbl.service_id
                LEFT JOIN vtpartner.vehiclestbl ON enquirytbl.vehicle_id = vehiclestbl.vehicle_id
                LEFT JOIN vtpartner.available_citys_tbl ON enquirytbl.city_id = available_citys_tbl.city_id
                LEFT JOIN vtpartner.category_type_tbl ON categorytbl.category_type_id = category_type_tbl.cat_type_id
                WHERE enquirytbl.status = 0
                ORDER BY enquiry_id DESC
            """
            values = []

            result = select_query(query)  # Assuming select_query is defined
            
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Structure the response data for better clarity
            enquiries_data = [
                {
                    "enquiry_id": row[0],
                    "category_id": row[1],
                    "sub_cat_id": row[2],
                    "service_id": row[3],
                    "vehicle_id": row[4],
                    "city_id": row[5],
                    "name": row[6],
                    "mobile_no": row[7],
                    "time_at": row[8],
                    "source_type": row[9],
                    "status": row[10],
                    "category_name": row[11],
                    "sub_cat_name": row[12],
                    "service_name": row[13],
                    "city_name": row[14],
                    "vehicle_name": row[15],
                    "category_type": row[16],
                }
                for row in result
            ]

            return JsonResponse({"all_enquiries_details": enquiries_data}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def register_agent(request):
    if request.method == "POST":
        try:
            # Destructure fields from request body
            body = json.loads(request.body)
            print("req.body::", body)

            # Extract fields from request body
            enquiry_id = body.get("enquiry_id")
            agent_name = body.get("agent_name")
            mobile_no = body.get("mobile_no")
            gender = body.get("gender")
            aadhar_no = body.get("aadhar_no")
            pan_no = body.get("pan_no")
            city_name = body.get("city_name")
            house_no = body.get("house_no")
            address = body.get("address")
            agent_photo_url = body.get("agent_photo_url")
            aadhar_card_front_url = body.get("aadhar_card_front_url")
            aadhar_card_back_url = body.get("aadhar_card_back_url")
            pan_card_front_url = body.get("pan_card_front_url")
            pan_card_back_url = body.get("pan_card_back_url")
            license_front_url = body.get("license_front_url")
            license_back_url = body.get("license_back_url")
            insurance_image_url = body.get("insurance_image_url")
            noc_image_url = body.get("noc_image_url")
            pollution_certificate_image_url = body.get("pollution_certificate_image_url")
            rc_image_url = body.get("rc_image_url")
            vehicle_image_url = body.get("vehicle_image_url")
            category_id = body.get("category_id")
            sub_cat_id = body.get("sub_cat_id")
            service_id = body.get("service_id")
            vehicle_id = body.get("vehicle_id")
            city_id = body.get("city_id")
            optional_documents = body.get("optionalDocuments", [])
            owner_name = body.get("owner_name")
            owner_mobile_no = body.get("owner_mobile_no")
            owner_house_no = body.get("owner_house_no")
            owner_city_name = body.get("owner_city_name")
            owner_address = body.get("owner_address")
            owner_photo_url = body.get("owner_photo_url")
            vehicle_plate_image = body.get("vehicle_plate_image")
            driving_license_no = body.get("driving_license_no")
            vehicle_plate_no = body.get("vehicle_plate_no")
            rc_no = body.get("rc_no")
            insurance_no = body.get("insurance_no")
            noc_no = body.get("noc_no")

            # Required fields check
            required_fields = {
                "enquiry_id": enquiry_id,
                "agent_name": agent_name,
                "mobile_no": mobile_no,
                "gender": gender,
                "aadhar_no": aadhar_no,
                "pan_no": pan_no,
                "category_id": category_id,
                "city_id": city_id,
            }

            missing_fields = [field for field, value in required_fields.items() if value is None]

            # If there are missing fields, return an error response
            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Check if owner exists by mobile number
            owner_id = None
            if owner_name and owner_mobile_no:
                try:
                    check_owner_query = "SELECT owner_id FROM vtpartner.owner_tbl WHERE owner_mobile_no = %s"
                    owner_result = select_query(check_owner_query, [owner_mobile_no])

                    if owner_result:
                        # Owner exists, get the existing owner ID
                        owner_id = owner_result[0]['owner_id']
                except Exception as error:
                    print("Owner error::", error)
                    # Insert owner data into owner_tbl if it does not exist
                    insert_owner_query = """
                        INSERT INTO vtpartner.owner_tbl (
                            owner_name, owner_mobile_no, house_no, city_name, address, profile_photo
                        ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING owner_id
                    """
                    owner_values = [owner_name, owner_mobile_no, owner_house_no, owner_city_name, owner_address, owner_photo_url]
                    new_owner_result = insert_query(insert_owner_query, owner_values)

                    if new_owner_result:
                        owner_id = new_owner_result[0]['owner_id']
                    else:
                        raise Exception("Failed to retrieve owner ID from insert operation")

            # Determine driver table and related fields based on category_id
            driver_table, name_column, driver_id_field = None, None, None

            if category_id == "1":
                driver_table = "vtpartner.goods_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "goods_driver_id"
            elif category_id == "2":
                driver_table = "vtpartner.cab_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "cab_driver_id"
            elif category_id == "3":
                driver_table = "vtpartner.jcb_crane_driverstbl"
                name_column = "driver_name"
                driver_id_field = "jcb_crane_driver_id"
            elif category_id == "4":
                driver_table = "vtpartner.other_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "other_driver_id"
            elif category_id == "5":
                driver_table = "vtpartner.handyman_servicestbl"
                name_column = "name"
                driver_id_field = "handyman_id"
            else:
                return JsonResponse({"message": "Invalid category_id"}, status=400)

            # Prepare common values for driver insert
            common_values = [
                agent_name,
                mobile_no,
                gender,
                aadhar_no,
                pan_no,
                city_name,
                house_no,
                address,
                agent_photo_url,
                aadhar_card_front_url,
                aadhar_card_back_url,
                pan_card_front_url,
                pan_card_back_url,
            ]

            if category_id in ["4", "5"]:  # Handyman Service specific columns
                insert_driver_query = f"""
                    INSERT INTO {driver_table} (
                        {name_column}, mobile_no, gender, aadhar_no, pan_card_no,
                        city_name, house_no, full_address, profile_pic,
                        aadhar_card_front, aadhar_card_back, pan_card_front,
                        pan_card_back, category_id, city_id, sub_cat_id, service_id, status
                    ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, 1)
                    RETURNING {driver_id_field}
                """
                driver_values = [
                    *common_values,
                    category_id,
                    city_id,
                    sub_cat_id,
                    service_id,
                ]
            else:  # Other categories (Goods, Cab, JCB/Crane)
                insert_driver_query = f"""
                    INSERT INTO {driver_table} (
                        {name_column}, mobile_no, gender, aadhar_no, pan_card_no,
                        city_name, house_no, full_address, profile_pic,
                        aadhar_card_front, aadhar_card_back, pan_card_front,
                        pan_card_back, license_front, license_back,
                        insurance_image, noc_image, pollution_certificate_image,
                        rc_image, vehicle_image, category_id, vehicle_id, city_id,
                        owner_id, vehicle_plate_image, status,
                        driving_license_no, vehicle_plate_no, rc_no,
                        insurance_no, noc_no
                    ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s, %s)
                    RETURNING {driver_id_field}
                """
                driver_values = [
                    *common_values,
                    license_front_url,
                    license_back_url,
                    insurance_image_url,
                    noc_image_url,
                    pollution_certificate_image_url,
                    rc_image_url,
                    vehicle_image_url,
                    category_id,
                    vehicle_id,
                    city_id,
                    owner_id,
                    vehicle_plate_image,
                    driving_license_no,
                    vehicle_plate_no,
                    rc_no,
                    insurance_no,
                    noc_no,
                ]

            driver_result = insert_query(insert_driver_query, driver_values)

            if driver_result:
                driver_id = driver_result[0][driver_id_field]
            else:
                raise Exception("Failed to retrieve driver ID from insert operation")

            # Insert optional documents into documents_vehicle_verified_tbl if any
            if optional_documents:
                insert_document_query = """
                    INSERT INTO vtpartner.documents_vehicle_verified_tbl (
                        document_name, document_image_url, {}
                    ) VALUES (%s, %s, %s)
                """.format(driver_id_field)

                for doc in optional_documents:
                    document_values = [doc["other"], doc["imageUrl"], driver_id]
                    insert_query(insert_document_query, document_values)

            # Update enquiry status
            update_query("UPDATE vtpartner.enquirytbl SET status = 2 WHERE enquiry_id = %s", [enquiry_id])

            # Respond with success
            return JsonResponse({"message": "Agent registered successfully."}, status=201)

        except Exception as e:
            print("Error during registration:", str(e))
            return JsonResponse({"message": "Error during registration", "error": str(e)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def check_driver_existence(request):
    if request.method == "POST":
        try:
            # Get the request body and parse JSON
            body = json.loads(request.body)
            mobile_no = body.get("mobile_no")
            enquiry_id = body.get("enquiry_id")

            # Required fields check
            required_fields = {
                "mobile_no": mobile_no,
                "enquiry_id": enquiry_id,
            }

            missing_fields = [field for field, value in required_fields.items() if value is None]

            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Query to check if the driver exists with the given mobile number
            check_driver_query = """
                SELECT goods_driver_id 
                FROM vtpartner.goods_driverstbl 
                WHERE mobile_no = %s
            """

            result = select_query(check_driver_query, [mobile_no])

            if result:
                # Driver exists, return a message with their ID
                driver_id = result[0]['goods_driver_id']
                
                # Update enquiry status
                try:
                    update_query("UPDATE vtpartner.enquirytbl SET status = 2 WHERE enquiry_id = %s", [enquiry_id])
                except Exception as error:
                    print("Update error:", error)
                    return JsonResponse({"message": "Failed to update enquiry status."}, status=500)

                return JsonResponse({
                    "message": f"Driver already exists with ID: {driver_id}",
                    "exists": True,
                    "driverId": driver_id,
                }, status=200)
            else:
                # Driver does not exist
                return JsonResponse({
                    "message": "Driver does not exist. Mobile number is available for registration.",
                    "exists": False,
                }, status=200)

        except Exception as error:
            print("Error checking driver existence:", error)
            return JsonResponse({
                "message": "An error occurred while checking driver existence.",
            }, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def check_handyman_existence(request):
    if request.method == "POST":
        try:
            # Get the request body and parse JSON
            body = json.loads(request.body)
            mobile_no = body.get("mobile_no")
            category_id = body.get("category_id")
            enquiry_id = body.get("enquiry_id")

            # Required fields check
            required_fields = {
                "mobile_no": mobile_no,
                "category_id": category_id,
                "enquiry_id": enquiry_id,
            }

            missing_fields = [field for field, value in required_fields.items() if value is None]

            if missing_fields:
                print(f"Missing required fields: {', '.join(missing_fields)}")
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Query to check if the handyman exists with the given mobile number
            if category_id == "5":
                check_driver_query = """
                    SELECT handyman_id 
                    FROM vtpartner.handyman_servicestbl 
                    WHERE mobile_no = %s AND category_id = %s
                """
            else:
                check_driver_query = """
                    SELECT other_driver_id 
                    FROM vtpartner.other_driverstbl 
                    WHERE mobile_no = %s AND category_id = %s
                """

            result = select_query(check_driver_query, [mobile_no, category_id])

            if result:
                # Handy man exists, return a message with their ID
                handyman_id = result[0]['handyman_id'] if category_id == "5" else result[0]['other_driver_id']
                
                # Update enquiry status
                try:
                    update_query("UPDATE vtpartner.enquirytbl SET status = 2 WHERE enquiry_id = %s", [enquiry_id])
                except Exception as error:
                    print("Update error:", error)
                    return JsonResponse({"message": "Failed to update enquiry status."}, status=500)

                return JsonResponse({
                    "message": f"Handy man already exists with ID: {handyman_id}",
                    "exists": True,
                    "driverId": handyman_id,
                }, status=200)
            else:
                # Handy man does not exist
                return JsonResponse({
                    "message": "Handy man does not exist. Mobile number is available for registration.",
                    "exists": False,
                }, status=200)

        except Exception as error:
            print("Error checking handyman existence:", error)
            return JsonResponse({
                "message": "An error occurred while checking handyman existence.",
                "exists": False,
            }, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_goods_drivers(request):
    if request.method == "POST":
        try:
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
                ORDER BY goods_driver_id DESC
            """

            result = select_query(query)

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to the same column names from the database
            mapped_results = [
                {
                    "goods_driver_id": row["goods_driver_id"],
                    "driver_first_name": row["driver_first_name"],
                    "profile_pic": row["profile_pic"],
                    "is_online": row["is_online"],
                    "ratings": row["ratings"],
                    "mobile_no": row["mobile_no"],
                    "registration_date": row["registration_date"],
                    "time": row["time"],
                    "r_lat": row["r_lat"],
                    "r_lng": row["r_lng"],
                    "current_lat": row["current_lat"],
                    "current_lng": row["current_lng"],
                    "status": row["status"],
                    "recent_online_pic": row["recent_online_pic"],
                    "is_verified": row["is_verified"],
                    "category_id": row["category_id"],
                    "vehicle_id": row["vehicle_id"],
                    "city_id": row["city_id"],
                    "aadhar_no": row["aadhar_no"],
                    "pan_card_no": row["pan_card_no"],
                    "house_no": row["house_no"],
                    "city_name": row["city_name"],
                    "full_address": row["full_address"],
                    "gender": row["gender"],
                    "owner_id": row["owner_id"],
                    "aadhar_card_front": row["aadhar_card_front"],
                    "aadhar_card_back": row["aadhar_card_back"],
                    "pan_card_front": row["pan_card_front"],
                    "pan_card_back": row["pan_card_back"],
                    "license_front": row["license_front"],
                    "license_back": row["license_back"],
                    "insurance_image": row["insurance_image"],
                    "noc_image": row["noc_image"],
                    "pollution_certificate_image": row["pollution_certificate_image"],
                    "rc_image": row["rc_image"],
                    "vehicle_image": row["vehicle_image"],
                    "vehicle_plate_image": row["vehicle_plate_image"],
                    "category_name": row["category_name"],
                    "vehicle_name": row["vehicle_name"],
                    "category_type": row["category_type"],
                    "driving_license_no": row["driving_license_no"],
                    "vehicle_plate_no": row["vehicle_plate_no"],
                    "rc_no": row["rc_no"],
                    "insurance_no": row["insurance_no"],
                    "noc_no": row["noc_no"],
                    "owner_photo": row["owner_photo"],
                    "owner_name": row["owner_name"],
                    "owner_mobile_no": row["owner_mobile_no"],
                    "owner_house_no": row["owner_house_no"],
                    "owner_city_name": row["owner_city_name"],
                    "owner_address": row["owner_address"],
                    "owner_profile_photo": row["owner_profile_photo"],
                }
                for row in result
            ]

            return JsonResponse({"all_goods_drivers": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_cab_drivers(request):
    if request.method == "POST":
        try:
            query = """
                SELECT cab_driver_id, driver_first_name, profile_pic, is_online, ratings,
                       mobile_no, cab_driverstbl.registration_date, cab_driverstbl.time,
                       r_lat, r_lng, current_lat, current_lng, status, recent_online_pic,
                       is_verified, cab_driverstbl.category_id, cab_driverstbl.vehicle_id,
                       city_id, aadhar_no, pan_card_no, cab_driverstbl.house_no,
                       cab_driverstbl.city_name, full_address, gender, cab_driverstbl.owner_id,
                       aadhar_card_front, aadhar_card_back, pan_card_front, pan_card_back,
                       license_front, license_back, insurance_image, noc_image,
                       pollution_certificate_image, rc_image, vehicle_image, vehicle_plate_image,
                       category_name, vehicle_name, category_type, driving_license_no,
                       vehicle_plate_no, rc_no, insurance_no, noc_no,
                       profile_photo AS owner_photo, owner_name, owner_mobile_no,
                       owner_tbl.house_no AS owner_house_no, owner_tbl.city_name AS owner_city_name,
                       owner_tbl.address AS owner_address, owner_tbl.profile_photo
                FROM vtpartner.cab_driverstbl
                LEFT JOIN vtpartner.categorytbl ON cab_driverstbl.category_id = categorytbl.category_id
                LEFT JOIN vtpartner.category_type_tbl ON categorytbl.category_type_id = category_type_tbl.cat_type_id
                LEFT JOIN vtpartner.vehiclestbl ON cab_driverstbl.vehicle_id = vehiclestbl.vehicle_id
                LEFT JOIN vtpartner.owner_tbl ON owner_tbl.owner_id = cab_driverstbl.owner_id
                ORDER BY cab_driver_id DESC
            """

            result = select_query(query)

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to keep the original column names
            mapped_results = [
                {
                    "cab_driver_id": row["cab_driver_id"],
                    "driver_first_name": row["driver_first_name"],
                    "profile_pic": row["profile_pic"],
                    "is_online": row["is_online"],
                    "ratings": row["ratings"],
                    "mobile_no": row["mobile_no"],
                    "registration_date": row["registration_date"],
                    "time": row["time"],
                    "r_lat": row["r_lat"],
                    "r_lng": row["r_lng"],
                    "current_lat": row["current_lat"],
                    "current_lng": row["current_lng"],
                    "status": row["status"],
                    "recent_online_pic": row["recent_online_pic"],
                    "is_verified": row["is_verified"],
                    "category_id": row["category_id"],
                    "vehicle_id": row["vehicle_id"],
                    "city_id": row["city_id"],
                    "aadhar_no": row["aadhar_no"],
                    "pan_card_no": row["pan_card_no"],
                    "house_no": row["house_no"],
                    "city_name": row["city_name"],
                    "full_address": row["full_address"],
                    "gender": row["gender"],
                    "owner_id": row["owner_id"],
                    "aadhar_card_front": row["aadhar_card_front"],
                    "aadhar_card_back": row["aadhar_card_back"],
                    "pan_card_front": row["pan_card_front"],
                    "pan_card_back": row["pan_card_back"],
                    "license_front": row["license_front"],
                    "license_back": row["license_back"],
                    "insurance_image": row["insurance_image"],
                    "noc_image": row["noc_image"],
                    "pollution_certificate_image": row["pollution_certificate_image"],
                    "rc_image": row["rc_image"],
                    "vehicle_image": row["vehicle_image"],
                    "vehicle_plate_image": row["vehicle_plate_image"],
                    "category_name": row["category_name"],
                    "vehicle_name": row["vehicle_name"],
                    "category_type": row["category_type"],
                    "driving_license_no": row["driving_license_no"],
                    "vehicle_plate_no": row["vehicle_plate_no"],
                    "rc_no": row["rc_no"],
                    "insurance_no": row["insurance_no"],
                    "noc_no": row["noc_no"],
                    "owner_photo": row["owner_photo"],
                    "owner_name": row["owner_name"],
                    "owner_mobile_no": row["owner_mobile_no"],
                    "owner_house_no": row["owner_house_no"],
                    "owner_city_name": row["owner_city_name"],
                    "owner_address": row["owner_address"],
                    "owner_profile_photo": row["owner_profile_photo"],
                }
                for row in result
            ]

            return JsonResponse({"all_cab_drivers": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt  # Disable CSRF protection for this view
def all_jcb_crane_drivers(request):
    if request.method == "POST":
        try:
            query = """
                SELECT jcb_crane_driver_id, driver_name, profile_pic, is_online, ratings,
                       mobile_no, jcb_crane_driverstbl.registration_date, jcb_crane_driverstbl.time,
                       r_lat, r_lng, current_lat, current_lng, status, recent_online_pic,
                       is_verified, jcb_crane_driverstbl.category_id, jcb_crane_driverstbl.vehicle_id,
                       city_id, aadhar_no, pan_card_no, jcb_crane_driverstbl.house_no,
                       jcb_crane_driverstbl.city_name, full_address, gender, jcb_crane_driverstbl.owner_id,
                       aadhar_card_front, aadhar_card_back, pan_card_front, pan_card_back,
                       license_front, license_back, insurance_image, noc_image,
                       pollution_certificate_image, rc_image, vehicle_image, vehicle_plate_image,
                       category_name, vehicle_name, category_type, driving_license_no,
                       vehicle_plate_no, rc_no, insurance_no, noc_no,
                       profile_photo AS owner_photo, owner_name, owner_mobile_no,
                       owner_tbl.house_no AS owner_house_no, owner_tbl.city_name AS owner_city_name,
                       owner_tbl.address AS owner_address, owner_tbl.profile_photo
                FROM vtpartner.jcb_crane_driverstbl
                LEFT JOIN vtpartner.categorytbl ON jcb_crane_driverstbl.category_id = categorytbl.category_id
                LEFT JOIN vtpartner.category_type_tbl ON categorytbl.category_type_id = category_type_tbl.cat_type_id
                LEFT JOIN vtpartner.vehiclestbl ON jcb_crane_driverstbl.vehicle_id = vehiclestbl.vehicle_id
                LEFT JOIN vtpartner.owner_tbl ON owner_tbl.owner_id = jcb_crane_driverstbl.owner_id
                ORDER BY jcb_crane_driver_id DESC
            """

            result = select_query(query)

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to keep the original column names
            mapped_results = [
                {
                    "jcb_crane_driver_id": row["jcb_crane_driver_id"],
                    "driver_name": row["driver_name"],
                    "profile_pic": row["profile_pic"],
                    "is_online": row["is_online"],
                    "ratings": row["ratings"],
                    "mobile_no": row["mobile_no"],
                    "registration_date": row["registration_date"],
                    "time": row["time"],
                    "r_lat": row["r_lat"],
                    "r_lng": row["r_lng"],
                    "current_lat": row["current_lat"],
                    "current_lng": row["current_lng"],
                    "status": row["status"],
                    "recent_online_pic": row["recent_online_pic"],
                    "is_verified": row["is_verified"],
                    "category_id": row["category_id"],
                    "vehicle_id": row["vehicle_id"],
                    "city_id": row["city_id"],
                    "aadhar_no": row["aadhar_no"],
                    "pan_card_no": row["pan_card_no"],
                    "house_no": row["house_no"],
                    "city_name": row["city_name"],
                    "full_address": row["full_address"],
                    "gender": row["gender"],
                    "owner_id": row["owner_id"],
                    "aadhar_card_front": row["aadhar_card_front"],
                    "aadhar_card_back": row["aadhar_card_back"],
                    "pan_card_front": row["pan_card_front"],
                    "pan_card_back": row["pan_card_back"],
                    "license_front": row["license_front"],
                    "license_back": row["license_back"],
                    "insurance_image": row["insurance_image"],
                    "noc_image": row["noc_image"],
                    "pollution_certificate_image": row["pollution_certificate_image"],
                    "rc_image": row["rc_image"],
                    "vehicle_image": row["vehicle_image"],
                    "vehicle_plate_image": row["vehicle_plate_image"],
                    "category_name": row["category_name"],
                    "vehicle_name": row["vehicle_name"],
                    "category_type": row["category_type"],
                    "driving_license_no": row["driving_license_no"],
                    "vehicle_plate_no": row["vehicle_plate_no"],
                    "rc_no": row["rc_no"],
                    "insurance_no": row["insurance_no"],
                    "noc_no": row["noc_no"],
                    "owner_photo": row["owner_photo"],
                    "owner_name": row["owner_name"],
                    "owner_mobile_no": row["owner_mobile_no"],
                    "owner_house_no": row["owner_house_no"],
                    "owner_city_name": row["owner_city_name"],
                    "owner_address": row["owner_address"],
                    "owner_profile_photo": row["owner_profile_photo"],
                }
                for row in result
            ]

            return JsonResponse({"all_jcb_crane_drivers": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_handy_man(request):
    if request.method == "POST":
        try:
            query = """
                SELECT handyman_id, name, profile_pic, is_online, ratings, mobile_no,
                       handyman_servicestbl.registration_date, handyman_servicestbl.time,
                       r_lat, r_lng, current_lat, current_lng, status, recent_online_pic,
                       is_verified, handyman_servicestbl.category_id, handyman_servicestbl.sub_cat_id,
                       handyman_servicestbl.service_id, city_id, aadhar_no, pan_card_no,
                       handyman_servicestbl.house_no, handyman_servicestbl.city_name,
                       full_address, gender, aadhar_card_front, aadhar_card_back,
                       pan_card_front, pan_card_back, sub_cat_name, service_name, category_name
                FROM vtpartner.handyman_servicestbl
                LEFT JOIN vtpartner.sub_categorytbl ON handyman_servicestbl.sub_cat_id = sub_categorytbl.sub_cat_id
                LEFT JOIN vtpartner.other_servicestbl ON handyman_servicestbl.service_id = other_servicestbl.service_id
                LEFT JOIN vtpartner.categorytbl ON categorytbl.category_id = handyman_servicestbl.category_id
                ORDER BY handyman_id DESC
            """

            result = select_query(query)

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            return JsonResponse({"all_handy_man_details": result}, status=200)

        except Exception as err:
            print("Error executing query", err)
            if str(err) == "No Data Found":
                return JsonResponse({"message": "No Data Found"}, status=404)
            else:
                return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def edit_driver_details(request):
    if request.method == "POST":
        try:
            data = request.POST
            required_fields = {
                "driver_id": data.get("driver_id"),
                "agent_name": data.get("agent_name"),
                "mobile_no": data.get("mobile_no"),
                "gender": data.get("gender"),
                "aadhar_no": data.get("aadhar_no"),
                "pan_no": data.get("pan_no"),
                "city_name": data.get("city_name"),
                "house_no": data.get("house_no"),
                "address": data.get("address"),
                "agent_photo_url": data.get("agent_photo_url"),
                "aadhar_card_front_url": data.get("aadhar_card_front_url"),
                "aadhar_card_back_url": data.get("aadhar_card_back_url"),
                "pan_card_front_url": data.get("pan_card_front_url"),
                "pan_card_back_url": data.get("pan_card_back_url"),
                "license_front_url": data.get("license_front_url"),
                "license_back_url": data.get("license_back_url"),
                "insurance_image_url": data.get("insurance_image_url"),
                "noc_image_url": data.get("noc_image_url"),
                "pollution_certificate_image_url": data.get("pollution_certificate_image_url"),
                "rc_image_url": data.get("rc_image_url"),
                "vehicle_image_url": data.get("vehicle_image_url"),
                "category_id": data.get("category_id"),
                "vehicle_id": data.get("vehicle_id"),
                "city_id": data.get("city_id"),
                "owner_name": data.get("owner_name"),
                "owner_mobile_no": data.get("owner_mobile_no"),
                "owner_house_no": data.get("owner_house_no"),
                "owner_city_name": data.get("owner_city_name"),
                "owner_address": data.get("owner_address"),
                "owner_photo_url": data.get("owner_photo_url"),
                "vehicle_plate_image": data.get("vehicle_plate_image"),
                "driving_license_no": data.get("driving_license_no"),
                "vehicle_plate_no": data.get("vehicle_plate_no"),
                "rc_no": data.get("rc_no"),
                "insurance_no": data.get("insurance_no"),
                "noc_no": data.get("noc_no"),
                "owner_id": data.get("owner_id"),
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            driver_id = required_fields["driver_id"]
            owner_name = required_fields["owner_name"]
            owner_mobile_no = required_fields["owner_mobile_no"]
            owner_id = None

            # Handle Owner details
            if owner_name and owner_mobile_no:
                check_owner_query = """
                    SELECT owner_id FROM vtpartner.owner_tbl 
                    WHERE owner_mobile_no = %s
                """
                owner_result = select_query(check_owner_query, [owner_mobile_no])

                if owner_result:
                    owner_id = owner_result[0]['owner_id']
                    update_owner_query = """
                        UPDATE vtpartner.owner_tbl SET 
                        house_no = %s, city_name = %s, address = %s, profile_photo = %s, owner_name = %s 
                        WHERE owner_id = %s
                    """
                    owner_values = [
                        required_fields["owner_house_no"], 
                        required_fields["owner_city_name"],
                        required_fields["owner_address"], 
                        required_fields["owner_photo_url"],
                        owner_name, 
                        owner_id
                    ]
                    update_query(update_owner_query, owner_values)
                else:
                    insert_owner_query = """
                        INSERT INTO vtpartner.owner_tbl 
                        (owner_name, owner_mobile_no, house_no, city_name, address, profile_photo) 
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING owner_id
                    """
                    owner_values = [
                        owner_name, 
                        owner_mobile_no, 
                        required_fields["owner_house_no"], 
                        required_fields["owner_city_name"],
                        required_fields["owner_address"], 
                        required_fields["owner_photo_url"]
                    ]
                    new_owner_result = insert_query(insert_owner_query, owner_values)
                    owner_id = new_owner_result[0]['owner_id']

            # Determine the driver table and ID field based on category_id
            category_id = required_fields["category_id"]
            if category_id == "1":
                driver_table = "vtpartner.goods_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "goods_driver_id"
            elif category_id == "2":
                driver_table = "vtpartner.cab_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "cab_driver_id"
            elif category_id == "3":
                driver_table = "vtpartner.jcb_crane_driverstbl"
                name_column = "driver_name"
                driver_id_field = "jcb_crane_driver_id"
            else:
                return JsonResponse({"message": "Invalid category_id"}, status=400)

            update_driver_query = f"""
                UPDATE {driver_table} SET
                {name_column} = %s, mobile_no = %s, gender = %s, aadhar_no = %s, pan_card_no = %s,
                city_name = %s, house_no = %s, full_address = %s, profile_pic = %s,
                aadhar_card_front = %s, aadhar_card_back = %s, pan_card_front = %s,
                pan_card_back = %s, license_front = %s, license_back = %s,
                insurance_image = %s, noc_image = %s, pollution_certificate_image = %s,
                rc_image = %s, vehicle_image = %s, category_id = %s, vehicle_id = %s,
                city_id = %s, owner_id = %s, vehicle_plate_image = %s,
                driving_license_no = %s, vehicle_plate_no = %s, rc_no = %s,
                insurance_no = %s, noc_no = %s, time = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
                WHERE {driver_id_field} = %s
            """

            driver_values = [
                required_fields["agent_name"], required_fields["mobile_no"], required_fields["gender"], 
                required_fields["aadhar_no"], required_fields["pan_no"], required_fields["city_name"],
                required_fields["house_no"], required_fields["address"], required_fields["agent_photo_url"],
                required_fields["aadhar_card_front_url"], required_fields["aadhar_card_back_url"],
                required_fields["pan_card_front_url"], required_fields["pan_card_back_url"],
                required_fields["license_front_url"], required_fields["license_back_url"],
                required_fields["insurance_image_url"], required_fields["noc_image_url"],
                required_fields["pollution_certificate_image_url"], required_fields["rc_image_url"],
                required_fields["vehicle_image_url"], category_id, required_fields["vehicle_id"],
                required_fields["city_id"], owner_id, required_fields["vehicle_plate_image"],
                required_fields["driving_license_no"], required_fields["vehicle_plate_no"],
                required_fields["rc_no"], required_fields["insurance_no"], required_fields["noc_no"],
                driver_id
            ]

            row_count = update_query(update_driver_query, driver_values)

            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing updating driver query", err)
            return JsonResponse({"message": "Error executing updating driver query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_driver_details(request):
    if request.method == "POST":
        try:
            data = request.POST
            required_fields = {
                "driver_id": data.get("driver_id"),
                "agent_name": data.get("agent_name"),
                "mobile_no": data.get("mobile_no"),
                "gender": data.get("gender"),
                "aadhar_no": data.get("aadhar_no"),
                "pan_no": data.get("pan_no"),
                "city_name": data.get("city_name"),
                "house_no": data.get("house_no"),
                "address": data.get("address"),
                "agent_photo_url": data.get("agent_photo_url"),
                "aadhar_card_front_url": data.get("aadhar_card_front_url"),
                "aadhar_card_back_url": data.get("aadhar_card_back_url"),
                "pan_card_front_url": data.get("pan_card_front_url"),
                "pan_card_back_url": data.get("pan_card_back_url"),
                "license_front_url": data.get("license_front_url"),
                "license_back_url": data.get("license_back_url"),
                "insurance_image_url": data.get("insurance_image_url"),
                "noc_image_url": data.get("noc_image_url"),
                "pollution_certificate_image_url": data.get("pollution_certificate_image_url"),
                "rc_image_url": data.get("rc_image_url"),
                "vehicle_image_url": data.get("vehicle_image_url"),
                "category_id": data.get("category_id"),
                "vehicle_id": data.get("vehicle_id"),
                "city_id": data.get("city_id"),
                "owner_name": data.get("owner_name"),
                "owner_mobile_no": data.get("owner_mobile_no"),
                "owner_house_no": data.get("owner_house_no"),
                "owner_city_name": data.get("owner_city_name"),
                "owner_address": data.get("owner_address"),
                "owner_photo_url": data.get("owner_photo_url"),
                "vehicle_plate_image": data.get("vehicle_plate_image"),
                "driving_license_no": data.get("driving_license_no"),
                "vehicle_plate_no": data.get("vehicle_plate_no"),
                "rc_no": data.get("rc_no"),
                "insurance_no": data.get("insurance_no"),
                "noc_no": data.get("noc_no"),
                "owner_id": data.get("owner_id"),
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            owner_id = None
            owner_name = required_fields["owner_name"]
            owner_mobile_no = required_fields["owner_mobile_no"]

            if owner_name and owner_mobile_no:
                check_owner_query = """
                    SELECT owner_id FROM vtpartner.owner_tbl 
                    WHERE owner_mobile_no = %s
                """
                owner_result = select_query(check_owner_query, [owner_mobile_no])

                if owner_result:
                    owner_id = owner_result[0]['owner_id']
                else:
                    insert_owner_query = """
                        INSERT INTO vtpartner.owner_tbl 
                        (owner_name, owner_mobile_no, house_no, city_name, address, profile_photo) 
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING owner_id
                    """
                    owner_values = [
                        owner_name,
                        owner_mobile_no,
                        required_fields["owner_house_no"],
                        required_fields["owner_city_name"],
                        required_fields["owner_address"],
                        required_fields["owner_photo_url"],
                    ]
                    new_owner_result = insert_query(insert_owner_query, owner_values)
                    owner_id = new_owner_result[0]['owner_id']

            driver_table, name_column, driver_id_field = None, None, None

            # Determine the driver table and ID field based on category_id
            category_id = required_fields["category_id"]
            if category_id == "1":
                driver_table = "vtpartner.goods_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "goods_driver_id"
            elif category_id == "2":
                driver_table = "vtpartner.cab_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "cab_driver_id"
            elif category_id == "3":
                driver_table = "vtpartner.jcb_crane_driverstbl"
                name_column = "driver_name"
                driver_id_field = "jcb_crane_driver_id"
            else:
                return JsonResponse({"message": "Invalid category_id"}, status=400)

            insert_driver_query = f"""
                INSERT INTO {driver_table} (
                    {name_column}, mobile_no, gender, aadhar_no, pan_card_no, 
                    city_name, house_no, full_address, profile_pic, 
                    aadhar_card_front, aadhar_card_back, pan_card_front, 
                    pan_card_back, license_front, license_back, 
                    insurance_image, noc_image, pollution_certificate_image, 
                    rc_image, vehicle_image, category_id, vehicle_id, city_id, 
                    owner_id, vehicle_plate_image, driving_license_no, 
                    vehicle_plate_no, rc_no, insurance_no, noc_no, status
                ) 
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, '1'
                ) 
                RETURNING {driver_id_field}
            """

            driver_values = [
                required_fields["agent_name"],
                required_fields["mobile_no"],
                required_fields["gender"],
                required_fields["aadhar_no"],
                required_fields["pan_no"],
                required_fields["city_name"],
                required_fields["house_no"],
                required_fields["address"],
                required_fields["agent_photo_url"],
                required_fields["aadhar_card_front_url"],
                required_fields["aadhar_card_back_url"],
                required_fields["pan_card_front_url"],
                required_fields["pan_card_back_url"],
                required_fields["license_front_url"],
                required_fields["license_back_url"],
                required_fields["insurance_image_url"],
                required_fields["noc_image_url"],
                required_fields["pollution_certificate_image_url"],
                required_fields["rc_image_url"],
                required_fields["vehicle_image_url"],
                category_id,
                required_fields["vehicle_id"],
                required_fields["city_id"],
                owner_id,
                required_fields["vehicle_plate_image"],
                required_fields["driving_license_no"],
                required_fields["vehicle_plate_no"],
                required_fields["rc_no"],
                required_fields["insurance_no"],
                required_fields["noc_no"],
            ]

            new_driver_result = insert_query(insert_driver_query, driver_values)
            return JsonResponse({"message": f"{new_driver_result[0][driver_id_field]} row(s) inserted"}, status=201)

        except Exception as err:
            print("Error executing add new driver query", err)
            return JsonResponse({"message": "Error executing add new driver query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def edit_handyman_details(request):
    try:
        data = request.data

        required_fields = {
            "handyman_id": data.get("handyman_id"),
            "agent_name": data.get("agent_name"),
            "mobile_no": data.get("mobile_no"),
            "gender": data.get("gender"),
            "aadhar_no": data.get("aadhar_no"),
            "pan_no": data.get("pan_no"),
            "city_id": data.get("city_id"),
            "city_name": data.get("city_name"),
            "house_no": data.get("house_no"),
            "address": data.get("address"),
            "category_id": data.get("category_id"),
            "sub_cat_id": data.get("sub_cat_id"),
            "service_id": data.get("service_id"),
            "agent_photo_url": data.get("agent_photo_url"),
            "aadhar_card_front_url": data.get("aadhar_card_front_url"),
            "aadhar_card_back_url": data.get("aadhar_card_back_url"),
            "pan_card_front_url": data.get("pan_card_front_url"),
            "pan_card_back_url": data.get("pan_card_back_url"),
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        update_driver_query = """
            UPDATE vtpartner.handyman_servicestbl
            SET 
                name = %s,
                mobile_no = %s,
                gender = %s,
                aadhar_no = %s,
                pan_card_no = %s,
                city_name = %s,
                house_no = %s,
                full_address = %s,
                profile_pic = %s,
                aadhar_card_front = %s,
                aadhar_card_back = %s,
                pan_card_front = %s,
                pan_card_back = %s,
                category_id = %s,
                city_id = %s,
                sub_cat_id = %s,
                service_id = %s,
                status = 1
            WHERE handyman_id = %s
        """

        driver_values = [
            data['agent_name'], data['mobile_no'], data['gender'], 
            data['aadhar_no'], data['pan_no'], data['city_name'], 
            data['house_no'], data['address'], data['agent_photo_url'], 
            data['aadhar_card_front_url'], data['aadhar_card_back_url'], 
            data['pan_card_front_url'], data['pan_card_back_url'], 
            data['category_id'], data['city_id'], 
            data['sub_cat_id'], data['service_id'], 
            data['handyman_id']
        ]

        row_count = update_query(update_driver_query, driver_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating driver query", err)
        return JsonResponse({"message": "Error executing updating driver query"}, status=500)