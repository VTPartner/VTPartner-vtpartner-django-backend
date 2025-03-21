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
from django.core.files.storage import FileSystemStorage
import math
from django.db import transaction
from storages.backends.s3boto3 import S3Boto3Storage
from django.conf import settings
import boto3
from botocore.exceptions import ClientError

from PIL import Image  # Pillow library for image processing

# Utility function to check for missing fields
def check_missing_fields(fields):
    missing_fields = [field for field, value in fields.items() if not value]
    print("missing_fields::",missing_fields)
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
            result = None
            result = cursor.fetchall()

            # if result == []:
            #     raise ValueError("No Data Found")  # Custom error when no results are found
            # print("query result::",result)
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

def delete_query(query, params):
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

def upload_images2(uploaded_image):
    print("uploaded_image::::",uploaded_image)
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
    return image_url

# @csrf_exempt
# def upload_image(request):
#     if request.method == 'POST':
#         file = request.FILES['image']  # Assuming the file is named 'image' in the form data
#         fs = FileSystemStorage()
#         filename = fs.save(file.name, file)
#         file_url = fs.url(filename)
#         print("file_url::",file_url)
#         return JsonResponse({'image_url': file_url})
#     else:
#         return JsonResponse({'error': 'Invalid request'})
@csrf_exempt
def upload_image(request):
    if request.method == 'POST':
        try:
            if 'image' not in request.FILES:
                return JsonResponse({
                    'success': False,
                    'error': 'No file provided'
                }, status=400)

            file = request.FILES['image']

            # Initialize S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_S3_REGION_NAME
            )

            # Generate unique filename
            file_extension = os.path.splitext(file.name)[1]
            unique_filename = f"uploads/{datetime.now().strftime('%Y%m%d_%H%M%S')}{file_extension}"

            # Upload file to S3 without ACL
            s3_client.upload_fileobj(
                file,
                settings.AWS_STORAGE_BUCKET_NAME,
                unique_filename,
                ExtraArgs={
                    'ContentType': file.content_type
                }
            )

            # Generate file URL
            file_url = f"https://{settings.AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com/{unique_filename}"
            
            return JsonResponse({
                'success': True,
                'image_url': file_url,
                'message': 'File uploaded successfully'
            })

        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)
    
    return JsonResponse({
        'success': False,
        'error': 'Invalid request method'
    }, status=400)
       
@csrf_exempt
def upload_images(request):
    if request.method == "POST":
        # Debugging: Log the contents of request.FILES
        print("Files in request:", request.FILES)

        # Access the uploaded image file from request.FILES
        uploaded_images = request.FILES.getlist("vtPartnerImage")  # Get a list of files

        if not uploaded_images:
            return JsonResponse({"message": "No image provided"}, status=400)

        uploaded_image = uploaded_images[0]  # Get the first image file

        try:
            # Check if uploaded_image has a size
            if uploaded_image.size == 0:
                return JsonResponse({"message": "Uploaded file is empty"}, status=400)

            # Open and verify the uploaded image
            img = Image.open(uploaded_image)
            img.verify()  # Verify that it is an image
            img_resized = img.resize((300, 300))

            # Construct the file path for saving the image
            unique_identifier = str(uuid.uuid4())
            file_extension = mimetypes.guess_extension(uploaded_image.content_type) or ".jpg"
            custom_image_name = f'img_{unique_identifier}{file_extension}'
            file_path = os.path.join(settings.MEDIA_ROOT, custom_image_name)

            # Save the resized image
            img_resized.save(file_path)

            # Assuming you have MEDIA_URL configured
            image_url = os.path.join(settings.MEDIA_URL, custom_image_name)
            print(f'Uploaded Image URL: {image_url}')

            return JsonResponse({"image_url": image_url}, status=200)

        except Exception as img_err:
            print("Error processing image:", img_err)
            return JsonResponse({"message": "Invalid image file"}, status=400)

    return JsonResponse({"message": "Method not allowed"}, status=405)


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

            if result == []:
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
                status,
                base_price,
                outstation_distance 
            FROM 
                vtpartner.available_citys_tbl 
            ORDER BY 
                city_id DESC
            """
            result = select_query(query)  # Assuming select_query is defined elsewhere

            if result == []:
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
                    "base_price": row[8],
                    "outstation_distance": row[9]
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
            base_price = data.get("base_price")
            outstation_distance = data.get("outstation_distance")

            # List of required fields
            required_fields = {
                "city_id": city_id,
                "city_name": city_name,
                "pincode": pincode,
                "pincode_until": pincode_until,
                "description": description,
                "bg_image": bg_image,
                "status": status,
                "base_price": base_price,
                "outstation_distance": outstation_distance,
            }

            # Check for missing fields
             # Use the utility function to check for missing fields
            missing_fields = check_missing_fields(required_fields)

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

            query = """
                UPDATE vtpartner.available_citys_tbl 
                SET city_name = %s, pincode = %s, bg_image = %s, 
                    pincode_until = %s, description = %s, 
                    time = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), 
                    status = %s,base_price = %s, outstation_distance = %s  
                WHERE city_id = %s
            """
            params = [
                city_name,
                pincode,
                bg_image,
                pincode_until,
                description,
                status,
                base_price,
                outstation_distance,
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
            base_price = data.get('base_price')
            outstation_distance = data.get('outstation_distance')

            # List of required fields
            required_fields = {
                'city_name': city_name,
                'pincode': pincode,
                'pincode_until': pincode_until,
                'description': description,
                'bg_image': bg_image,
                'base_price': base_price,
                'outstation_distance': outstation_distance,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            # Prepare the insert query
            query = """
                INSERT INTO vtpartner.available_citys_tbl (city_name, pincode, bg_image, pincode_until, description, base_price, outstation_distance)
                VALUES (%s, %s, %s, %s, %s,%s, %s)
            """
            params = [city_name, pincode, bg_image, pincode_until, description,base_price, outstation_distance]
            row_count = insert_query(query, params)

            return JsonResponse({"message": f"{row_count} rows inserted"}, status=200)

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

            if result == []:
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
            if result[0][0] > 0:
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
            if result[0][0] > 0:
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_type_details = [
                {
                    "cat_type_id": row[0],
                    "category_type": row[1]
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
                    description,
                    website_background_image,
                    attach_vehicle_background_image
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
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
            website_background_image = data.get("website_background_image")
            attach_vehicle_background_image = data.get("attach_vehicle_background_image")

            # List of required fields
            required_fields = {
                "category_name": category_name,
                "category_type_id": category_type_id,
                "category_image": category_image,
                "description": description,
                "website_background_image": website_background_image,
                "attach_vehicle_background_image": attach_vehicle_background_image,
            }

            # Check for missing fields
            missing_fields = [field for field, value in required_fields.items() if value is None]
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            # Validate to avoid duplication
            query_duplicate_check = """
                SELECT COUNT(*) FROM vtpartner.categorytbl WHERE category_name ILIKE %s
            """
            result = select_query(query_duplicate_check, [category_name]) 

            if result and result[0][0] > 0:
                return JsonResponse({"message": "Service Name already exists"}, status=409)

            # If not duplicate, proceed to insert
            query = """
                INSERT INTO vtpartner.categorytbl (category_name, category_type_id, category_image, description,website_background_image,attach_vehicle_background_image)
                VALUES (%s, %s, %s, %s,%s,%s)
            """
            row_count = insert_query(query, [category_name, category_type_id, category_image, description,website_background_image,attach_vehicle_background_image])  

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
            print("data::",data)
            category_id = data.get("category_id")
            category_name = data.get("category_name")
            category_type_id = data.get("category_type_id")
            category_image = data.get("category_image")
            description = data.get("description")
            website_background_image = data.get("website_background_image")
            attach_vehicle_background_image = data.get("attach_vehicle_background_image")
            
            

            # List of required fields
            required_fields = {
                "category_id": category_id,
                "category_name": category_name,
                "category_type_id": category_type_id,
                "category_image": category_image,
                "description": description,
                "website_background_image": website_background_image,
                "attach_vehicle_background_image": attach_vehicle_background_image,
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

            if result and result[0][0] > 0:
                return JsonResponse({"message": "Service Name already exists"}, status=409)

            # If not duplicate, proceed to update
            query = """
                UPDATE vtpartner.categorytbl 
                SET category_name = %s, category_type_id = %s, category_image = %s, 
                    epoch = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), description = %s, website_background_image = %s,attach_vehicle_background_image = %s 
                WHERE category_id = %s
            """
            row_count = update_query(query, [category_name, category_type_id, category_image, description,website_background_image,attach_vehicle_background_image, category_id])  # Assuming update_query is defined

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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            vehicle_type_details = [
                {
                    "vehicle_type_id": row[0],
                    "vehicle_type_name": row[1],
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
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
            if result and result[0][0] > 0:
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
            if result and result[0][0] > 0:
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for a clearer JSON response
            vehicle_price_details = [
                {
                    "price_id": row[0],
                    "city_id": row[1],
                    "vehicle_id": row[2],
                    "starting_price_per_km": row[3],
                    "minimum_time": row[4],
                    "price_type_id": row[5],
                    "city_name": row[6],
                    "price_type": row[7],
                    "bg_image": row[8],
                    "time_created_at": row[9],
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for a clearer JSON response
            vehicle_price_types = [
                {
                    "price_type_id": row[0],
                    "price_type": row[1],
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


@csrf_exempt
def add_peak_hour_price(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            city_id = data.get('city_id')
            vehicle_id = data.get('vehicle_id')
            price_per_km = data.get('price_per_km')
            start_time = data.get('start_time')
            end_time = data.get('end_time')

            required_fields = {
                'city_id': city_id,
                'vehicle_id': vehicle_id,
                'price_per_km': price_per_km,
                'start_time': start_time,
                'end_time': end_time
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Check for overlapping time slots using text comparison
            query_overlap_check = """
                SELECT COUNT(*) 
                FROM vtpartner.vehicle_peak_hours_price_tbl 
                WHERE city_id = %s 
                AND vehicle_id = %s 
                AND status = 1
                AND (
                    (start_time <= %s AND end_time > %s)
                    OR (start_time < %s AND end_time >= %s)
                    OR (%s <= start_time AND %s > start_time)
                )
            """
            values_overlap_check = (
                city_id, vehicle_id, 
                end_time, start_time,  # First condition
                end_time, end_time,    # Second condition
                start_time, end_time   # Third condition
            )
            result = select_query(query_overlap_check, values_overlap_check)

            if result and result[0][0] > 0:
                return JsonResponse({"message": "Time slot overlaps with existing peak hours"}, status=409)

            query = """
                INSERT INTO vtpartner.vehicle_peak_hours_price_tbl 
                (city_id, vehicle_id, price_per_km, start_time, end_time) 
                VALUES (%s, %s, %s, %s, %s)
            """
            values = (city_id, vehicle_id, price_per_km, start_time, end_time)
            row_count = insert_query(query, values)

            return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

        except Exception as err:
            print("Error adding peak hour price:", err)
            return JsonResponse({"message": "Error adding peak hour price"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def edit_peak_hour_price(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            peak_price_id = data.get('peak_price_id')
            city_id = data.get('city_id')
            vehicle_id = data.get('vehicle_id')
            price_per_km = data.get('price_per_km')
            start_time = data.get('start_time')
            end_time = data.get('end_time')
            status = data.get('status', 1)

            required_fields = {
                'peak_price_id': peak_price_id,
                'city_id': city_id,
                'vehicle_id': vehicle_id,
                'price_per_km': price_per_km,
                'start_time': start_time,
                'end_time': end_time
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Check for overlapping time slots excluding current record
            query_overlap_check = """
                SELECT COUNT(*) 
                FROM vtpartner.vehicle_peak_hours_price_tbl 
                WHERE city_id = %s 
                AND vehicle_id = %s 
                AND peak_price_id != %s
                AND status = 1
                AND (
                    (start_time <= %s AND end_time > %s)
                    OR (start_time < %s AND end_time >= %s)
                    OR (%s <= start_time AND %s > start_time)
                )
            """
            values_overlap_check = (
                city_id, vehicle_id, peak_price_id,
                end_time, start_time,  # First condition
                end_time, end_time,    # Second condition
                start_time, end_time   # Third condition
            )
            result = select_query(query_overlap_check, values_overlap_check)

            if result and result[0][0] > 0:
                return JsonResponse({"message": "Time slot overlaps with existing peak hours"}, status=409)

            query = """
                UPDATE vtpartner.vehicle_peak_hours_price_tbl 
                SET city_id = %s,
                    vehicle_id = %s,
                    price_per_km = %s,
                    start_time = %s,
                    end_time = %s,
                    status = %s,
                    time_created_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
                WHERE peak_price_id = %s
            """
            values = (city_id, vehicle_id, price_per_km, start_time, end_time, status, peak_price_id)
            row_count = update_query(query, values)

            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error updating peak hour price:", err)
            return JsonResponse({"message": "Error updating peak hour price"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_peak_hour_prices(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            vehicle_id = data.get('vehicle_id')
            city_id = data.get('city_id')

            query = """
                SELECT p.*, c.city_name, c.bg_image
                FROM vtpartner.vehicle_peak_hours_price_tbl p
                JOIN vtpartner.available_citys_tbl c ON p.city_id = c.city_id
                WHERE p.vehicle_id = %s
                AND (p.city_id = %s OR %s IS NULL)
                ORDER BY p.start_time::time
            """
            values = (vehicle_id, city_id, city_id)
            result = select_query(query, values)

            peak_hours = []
            for row in result:
                peak_hours.append({
                    'peak_price_id': row[0],
                    'city_id': row[1],
                    'vehicle_id': row[2],
                    'price_per_km': float(row[3]),
                    'start_time': row[6],  # Changed index to match table structure
                    'end_time': row[7],    # Changed index to match table structure
                    'status': row[4],
                    'time_created_at': float(row[5]),
                    'city_name': row[8],
                    'bg_image': row[9]
                })

            return JsonResponse({"peak_hours": peak_hours}, status=200)

        except Exception as err:
            print("Error fetching peak hour prices:", err)
            return JsonResponse({"message": "Error fetching peak hour prices"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_all_customers(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            page = int(data.get('page', 1))
            limit = int(data.get('limit', 10))
            search = data.get('search', '').lower()
            offset = (page - 1) * limit

            # Get total count for pagination with search
            count_query = """
                SELECT COUNT(*) 
                FROM vtpartner.customers_tbl
                WHERE status = 1
                AND (
                    LOWER(customer_name) LIKE %s OR 
                    LOWER(mobile_no) LIKE %s OR
                    LOWER(email) LIKE %s OR
                    LOWER(gst_no) LIKE %s
                )
            """
            search_pattern = f'%{search}%'
            count_result = select_query(count_query, 
                (search_pattern, search_pattern, search_pattern, search_pattern))
            total_count = count_result[0][0]

            # Get paginated results
            query = """
                SELECT 
                    customer_id,
                    customer_name,
                    profile_pic,
                    mobile_no,
                    email,
                    registration_date,
                    time,
                    status,
                    full_address,
                    gst_no,
                    gst_address,
                    purpose,
                    pincode
                FROM vtpartner.customers_tbl
                WHERE status = 1
                AND customer_name !='NA'
                AND (
                    LOWER(customer_name) LIKE %s OR 
                    LOWER(mobile_no) LIKE %s OR
                    LOWER(email) LIKE %s OR
                    LOWER(gst_no) LIKE %s
                )
                ORDER BY time DESC
                LIMIT %s OFFSET %s
            """
            values = (search_pattern, search_pattern, search_pattern, 
                     search_pattern, limit, offset)
            result = select_query(query, values)

            customers = []
            for row in result:
                customers.append({
                    'customer_id': row[0],
                    'customer_name': row[1],
                    'profile_pic': row[2],
                    'mobile_no': row[3],
                    'email': row[4],
                    'registration_date': str(row[5]),
                    'time_created_at': float(row[6]),
                    'status': row[7],
                    'full_address': row[8],
                    'gst_no': row[9],
                    'gst_address': row[10],
                    'purpose': row[11],
                    'pincode': row[12]
                })

            return JsonResponse({
                "customers": customers,
                "total": total_count,
                "page": page,
                "total_pages": math.ceil(total_count / limit)
            }, status=200)

        except Exception as err:
            print("Error fetching customers:", err)
            return JsonResponse({"message": str(err)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_banner(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            banner_title = data.get('banner_title')
            banner_description = data.get('banner_description')
            banner_image = data.get('banner_image')
            banner_type = data.get('banner_type')
            start_date = data.get('start_date')
            end_date = data.get('end_date')

            required_fields = {
                'banner_title': banner_title,
                'banner_image': banner_image,
                'banner_type': banner_type,
                'start_date': start_date,
                'end_date': end_date
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            query = """
                INSERT INTO vtpartner.banners_tbl 
                (banner_title, banner_description, banner_image, banner_type, start_date, end_date) 
                VALUES (%s, %s, %s, %s, %s::timestamp, %s::timestamp)
            """
            values = (banner_title, banner_description, banner_image, banner_type, start_date, end_date)
            row_count = insert_query(query, values)

            return JsonResponse({"message": "Banner added successfully"}, status=200)

        except Exception as err:
            print("Error adding banner:", err)
            return JsonResponse({"message": "Error adding banner"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def edit_banner(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            banner_id = data.get('banner_id')
            banner_title = data.get('banner_title')
            banner_description = data.get('banner_description')
            banner_image = data.get('banner_image')
            banner_type = data.get('banner_type')
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            status = data.get('status', 1)

            required_fields = {
                'banner_id': banner_id,
                'banner_title': banner_title,
                'banner_image': banner_image,
                'banner_type': banner_type,
                'start_date': start_date,
                'end_date': end_date
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            query = """
                UPDATE vtpartner.banners_tbl 
                SET banner_title = %s,
                    banner_description = %s,
                    banner_image = %s,
                    banner_type = %s,
                    start_date = %s::timestamp,
                    end_date = %s::timestamp,
                    status = %s,
                    time_created_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
                WHERE banner_id = %s
            """
            values = (banner_title, banner_description, banner_image, banner_type, 
                     start_date, end_date, status, banner_id)
            row_count = update_query(query, values)

            return JsonResponse({"message": "Banner updated successfully"}, status=200)

        except Exception as err:
            print("Error updating banner:", err)
            return JsonResponse({"message": "Error updating banner"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_banner(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            banner_id = data.get('banner_id')

            if not banner_id:
                return JsonResponse({"message": "Banner ID is required"}, status=400)

            query = """
                UPDATE vtpartner.banners_tbl 
                SET status = 0,
                    time_created_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
                WHERE banner_id = %s
            """
            values = (banner_id,)
            row_count = update_query(query, values)

            return JsonResponse({"message": "Banner deleted successfully"}, status=200)

        except Exception as err:
            print("Error deleting banner:", err)
            return JsonResponse({"message": "Error deleting banner"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_all_banners(request):
    if request.method == "POST":
        try:
            query = """
                SELECT banner_id, banner_title, banner_description, banner_image, 
                       banner_type, start_date, end_date, status, time_created_at
                FROM vtpartner.banners_tbl
                
                ORDER BY time_created_at DESC
            """
            result = select_query(query)

            banners = []
            for row in result:
                banners.append({
                    'banner_id': row[0],
                    'banner_title': row[1],
                    'banner_description': row[2],
                    'banner_image': row[3],
                    'banner_type': row[4],
                    'start_date': str(row[5]),
                    'end_date': str(row[6]),
                    'status': row[7],
                    'time_created_at': float(row[8])
                })

            return JsonResponse({"banners": banners}, status=200)

        except Exception as err:
            print("Error fetching banners:", err)
            return JsonResponse({"message": "Error fetching banners"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_orders_report(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            start_date = data.get('start_date')
            end_date = data.get('end_date')

            if not start_date or not end_date:
                return JsonResponse({"message": "Start date and end date are required"}, status=400)

            query = """
                SELECT 
                    booking_id, orders_tbl.customer_id, orders_tbl.driver_id,
                    pickup_lat, pickup_lng, destination_lat, destination_lng,
                    distance, orders_tbl.time, total_price, base_price,
                    booking_timing, booking_date, booking_status,
                    driver_arrival_time, otp, gst_amount, igst_amount,
                    goods_type_id, payment_method, orders_tbl.city_id,
                    order_id, sender_name, sender_number, receiver_name,
                    receiver_number, driver_first_name, goods_driverstbl.authtoken,
                    customer_name, customers_tbl.authtoken, pickup_address,
                    drop_address, customers_tbl.mobile_no, goods_driverstbl.mobile_no,
                    vehiclestbl.vehicle_id, vehiclestbl.vehicle_name, vehiclestbl.image 
                FROM vtpartner.vehiclestbl, vtpartner.orders_tbl,
                     vtpartner.goods_driverstbl, vtpartner.customers_tbl 
                WHERE goods_driverstbl.goods_driver_id = orders_tbl.driver_id 
                AND customers_tbl.customer_id = orders_tbl.customer_id 
                AND vehiclestbl.vehicle_id = goods_driverstbl.vehicle_id
                AND booking_date BETWEEN %s AND %s
                ORDER BY order_id DESC
            """
            
            result = select_query(query, (start_date, end_date))

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Calculate totals
            total_amount = 0
            total_orders = len(result)

            mapped_results = []
            for row in result:
                total_amount += float(row[9] or 0)  # total_price
                mapped_results.append({
                    "booking_id": str(row[0]),
                    "customer_id": str(row[1]),
                    "driver_id": str(row[2]),
                    "pickup_lat": str(row[3]),
                    "pickup_lng": str(row[4]),
                    "destination_lat": str(row[5]),
                    "destination_lng": str(row[6]),
                    "distance": str(row[7]),
                    "total_time": str(row[8]),
                    "total_price": str(row[9]),
                    "base_price": str(row[10]),
                    "booking_timing": str(row[11]),
                    "booking_date": str(row[12]),
                    "booking_status": str(row[13]),
                    "driver_arrival_time": str(row[14]),
                    "otp": str(row[15]),
                    "gst_amount": str(row[16]),
                    "igst_amount": str(row[17]),
                    "goods_type_id": str(row[18]),
                    "payment_method": str(row[19]),
                    "city_id": str(row[20]),
                    "order_id": str(row[21]),
                    "sender_name": str(row[22]),
                    "sender_number": str(row[23]),
                    "receiver_name": str(row[24]),
                    "receiver_number": str(row[25]),
                    "driver_first_name": str(row[26]),
                    "customer_name": str(row[28]),
                    "pickup_address": str(row[30]),
                    "drop_address": str(row[31]),
                    "customer_mobile_no": str(row[32]),
                    "driver_mobile_no": str(row[33]),
                    "vehicle_name": str(row[35]),
                })

            return JsonResponse({
                "results": mapped_results,
                "total_amount": total_amount,
                "total_orders": total_orders
            }, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": str(err)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_coupon(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            required_fields = {
                'coupon_code': data.get('coupon_code'),
                'coupon_title': data.get('coupon_title'),
                'category_id': data.get('category_id'),
                'discount_type': data.get('discount_type'),
                'discount_value': data.get('discount_value'),
                'start_date': data.get('start_date'),
                'end_date': data.get('end_date')
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Check if coupon code already exists
            check_query = "SELECT COUNT(*) FROM vtpartner.coupons_tbl WHERE coupon_code = %s"
            result = select_query(check_query, (data.get('coupon_code'),))
            if result[0][0] > 0:
                return JsonResponse({"message": "Coupon code already exists"}, status=409)

            query = """
                INSERT INTO vtpartner.coupons_tbl 
                (coupon_code, coupon_title, coupon_description, category_id, 
                discount_type, discount_value, min_order_value, max_discount, 
                usage_limit, start_date, end_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::timestamp, %s::timestamp)
            """
            values = (
                data.get('coupon_code'),
                data.get('coupon_title'),
                data.get('coupon_description'),
                data.get('category_id'),
                data.get('discount_type'),
                data.get('discount_value'),
                data.get('min_order_value', 0),
                data.get('max_discount', 0),
                data.get('usage_limit', 0),
                data.get('start_date'),
                data.get('end_date')
            )
            insert_query(query, values)
            return JsonResponse({"message": "Coupon added successfully"}, status=200)

        except Exception as err:
            print("Error adding coupon:", err)
            return JsonResponse({"message": str(err)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def edit_coupon(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            required_fields = {
                'coupon_id': data.get('coupon_id'),
                'coupon_code': data.get('coupon_code'),
                'coupon_title': data.get('coupon_title'),
                'category_id': data.get('category_id'),
                'discount_type': data.get('discount_type'),
                'discount_value': data.get('discount_value'),
                'start_date': data.get('start_date'),
                'end_date': data.get('end_date')
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

            # Check if coupon code exists for other coupons
            check_query = """
                SELECT COUNT(*) FROM vtpartner.coupons_tbl 
                WHERE coupon_code = %s AND coupon_id != %s
            """
            result = select_query(check_query, (data.get('coupon_code'), data.get('coupon_id')))
            if result[0][0] > 0:
                return JsonResponse({"message": "Coupon code already exists"}, status=409)

            query = """
                UPDATE vtpartner.coupons_tbl 
                SET coupon_code = %s,
                    coupon_title = %s,
                    coupon_description = %s,
                    category_id = %s,
                    discount_type = %s,
                    discount_value = %s,
                    min_order_value = %s,
                    max_discount = %s,
                    usage_limit = %s,
                    start_date = %s::timestamp,
                    end_date = %s::timestamp,
                    status = %s
                WHERE coupon_id = %s
            """
            values = (
                data.get('coupon_code'),
                data.get('coupon_title'),
                data.get('coupon_description'),
                data.get('category_id'),
                data.get('discount_type'),
                data.get('discount_value'),
                data.get('min_order_value', 0),
                data.get('max_discount', 0),
                data.get('usage_limit', 0),
                data.get('start_date'),
                data.get('end_date'),
                data.get('status', 1),
                data.get('coupon_id')
            )
            update_query(query, values)
            return JsonResponse({"message": "Coupon updated successfully"}, status=200)

        except Exception as err:
            print("Error updating coupon:", err)
            return JsonResponse({"message": str(err)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_coupon(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            coupon_id = data.get('coupon_id')

            if not coupon_id:
                return JsonResponse({"message": "Coupon ID is required"}, status=400)

            query = """
                UPDATE vtpartner.coupons_tbl 
                SET status = 0 
                WHERE coupon_id = %s
            """
            update_query(query, (coupon_id,))
            return JsonResponse({"message": "Coupon deleted successfully"}, status=200)

        except Exception as err:
            print("Error deleting coupon:", err)
            return JsonResponse({"message": str(err)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_all_coupons(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            page = int(data.get('page', 1))
            limit = int(data.get('limit', 10))
            search = data.get('search', '')
            offset = (page - 1) * limit

            # Get total count for pagination
            count_query = """
                SELECT COUNT(*) 
                FROM vtpartner.coupons_tbl c
                JOIN vtpartner.categorytbl cat ON c.category_id = cat.category_id
                WHERE c.status = 1
                AND (
                    c.coupon_code ILIKE %s OR 
                    c.coupon_title ILIKE %s OR
                    cat.category_name ILIKE %s
                )
            """
            search_pattern = f'%{search}%'
            count_result = select_query(count_query, (search_pattern, search_pattern, search_pattern))
            total_count = count_result[0][0]

            # Get paginated results
            query = """
                SELECT c.*, cat.category_name
                FROM vtpartner.coupons_tbl c
                JOIN vtpartner.categorytbl cat ON c.category_id = cat.category_id
                WHERE c.status = 1
                AND (
                    c.coupon_code ILIKE %s OR 
                    c.coupon_title ILIKE %s OR
                    cat.category_name ILIKE %s
                )
                ORDER BY c.time_created_at DESC
                LIMIT %s OFFSET %s
            """
            values = (search_pattern, search_pattern, search_pattern, limit, offset)
            result = select_query(query, values)

            coupons = []
            for row in result:
                coupons.append({
                    'coupon_id': row[0],
                    'coupon_code': row[1],
                    'coupon_title': row[2],
                    'coupon_description': row[3],
                    'category_id': row[4],
                    'discount_type': row[5],
                    'discount_value': float(row[6]),
                    'min_order_value': float(row[7]),
                    'max_discount': float(row[8]),
                    'usage_limit': row[9],
                    'used_count': row[10],
                    'start_date': str(row[11]),
                    'end_date': str(row[12]),
                    'status': row[13],
                    'time_created_at': float(row[14]),
                    'category_name': row[15]
                })

            return JsonResponse({
                "coupons": coupons,
                "total": total_count,
                "page": page,
                "total_pages": math.ceil(total_count / limit)
            }, status=200)

        except Exception as err:
            print("Error fetching coupons:", err)
            return JsonResponse({"message": str(err)}, status=500)

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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for clearer response
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
            if result and result[0][0] > 0:
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
            if result and result[0][0] > 0:
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary for clearer response
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
            if result and result[0][0] > 0:
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
            if result and result[0][0] > 0:
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a structured format for clarity
            all_enquiries_details = [
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a structured format for clarity
            gallery_images_data = [
                {
                    "gallery_id": row[0],
                    "image_url": row[1],
                    "category_type": row[2],
                    "epoch": row[3],
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
            
            if result == []:
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
            category_id = int(body.get("category_id", 0))  # Cast to int
            sub_cat_id = int(body.get("sub_cat_id", 0))  # Cast to int
            service_id = int(body.get("service_id", 0))  # Cast to int
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
                    # Check if owner already exists based on mobile number
                    check_owner_query = "SELECT owner_id FROM vtpartner.owner_tbl WHERE owner_mobile_no = %s"
                    owner_result = select_query(check_owner_query, [owner_mobile_no])

                    if owner_result:
                        # Owner exists, get the existing owner ID
                        owner_id = owner_result[0][0]
                    else:
                        # Insert new owner if not found
                        insert_owner_query = """
                            INSERT INTO vtpartner.owner_tbl (
                                owner_name, owner_mobile_no, house_no, city_name, address, profile_photo
                            ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING owner_id
                        """
                        owner_values = [owner_name, owner_mobile_no, owner_house_no, owner_city_name, owner_address, owner_photo_url]
                        new_owner_result = insert_query(insert_owner_query, owner_values)

                        if new_owner_result:
                            owner_id = new_owner_result[0][0]
                        else:
                            raise Exception("Failed to retrieve owner ID from insert operation")

                except Exception as error:
                    print("Owner error::", error)
            print("owner_id::",owner_id)
            # if owner_id == None:
            #     return JsonResponse({"message": "Invalid Owner Id"}, status=500)
            # Determine driver table and related fields based on category_id
            driver_table, name_column, driver_id_field = None, None, None

            if category_id == 1:
                driver_table = "vtpartner.goods_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "goods_driver_id"
            elif category_id == 2:
                driver_table = "vtpartner.cab_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "cab_driver_id"
            elif category_id == 3:
                driver_table = "vtpartner.jcb_crane_driverstbl"
                name_column = "driver_name"
                driver_id_field = "jcb_crane_driver_id"
            elif category_id == 4:
                driver_table = "vtpartner.other_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "other_driver_id"
            elif category_id == 5:
                driver_table = "vtpartner.handymans_tbl"
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

            if category_id in [4, 5]:  # Handyman Service specific columns
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
                        owner_id, vehicle_plate_image,
                        driving_license_no, vehicle_plate_no, rc_no,
                        insurance_no, noc_no,status
                    ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,'1')
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
            print("insert_driver_query:",insert_driver_query)
            print("driver_values:",driver_values)
            driver_result = insert_query(insert_driver_query, driver_values)
            
            if not driver_result:
                raise Exception("Failed to insert driver data")

            driver_id = driver_result[0][0]
            print("driver_id::",driver_id)

            # Insert optional documents if they exist
            if optional_documents:
                insert_document_query = f"""
                    INSERT INTO vtpartner.documents_vehicle_verified_tbl (
                         {driver_id_field}, document_name, document_image_url
                    ) VALUES (%s, %s, %s)
                """

                for doc in optional_documents:
                    document_values = (driver_id, doc.get("other"), doc.get("imageUrl"))
                    insert_query(insert_document_query, document_values)

            return JsonResponse({"message": "Registration successful"}, status=201)

        except Exception as e:
            print("Error in register_agent:", str(e))
            return JsonResponse({"message": "Error occurred during registration"}, status=500)
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
                driver_id = result[0][0]
                
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
                "exists": False,
            }, status=200)

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
                    FROM vtpartner.handymans_tbl 
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
                handyman_id = result[0][0] if category_id == "5" else result[0][0]
                
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
            }, status=200)

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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to keep the original column names
            mapped_results = [
                {
                    "cab_driver_id":  row[0],
                    "driver_first_name":  row[1],
                    "profile_pic":  row[2],
                    "is_online":  row[3],
                    "ratings":  row[4],
                    "mobile_no":  row[5],
                    "registration_date":  row[6],
                    "time":  row[7],
                    "r_lat":  row[8],
                    "r_lng":  row[9],
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

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to keep the original column names
            mapped_results = [
                {
                    "jcb_crane_driver_id":  row[0],
                    "driver_name":  row[1],
                    "profile_pic":  row[2],
                    "is_online":  row[3],
                    "ratings":  row[4],
                    "mobile_no":  row[5],
                    "registration_date":  row[6],
                    "time":  row[7],
                    "r_lat":  row[8],
                    "r_lng":  row[9],
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
                       handymans_tbl.registration_date, handymans_tbl.time,
                       r_lat, r_lng, current_lat, current_lng, status, recent_online_pic,
                       is_verified, handymans_tbl.category_id, handymans_tbl.sub_cat_id,
                       handymans_tbl.service_id, city_id, aadhar_no, pan_card_no,
                       handymans_tbl.house_no, handymans_tbl.city_name,
                       full_address, gender, aadhar_card_front, aadhar_card_back,
                       pan_card_front, pan_card_back, sub_cat_name, service_name, category_name
                FROM vtpartner.handymans_tbl
                LEFT JOIN vtpartner.sub_categorytbl ON handymans_tbl.sub_cat_id = sub_categorytbl.sub_cat_id
                LEFT JOIN vtpartner.other_servicestbl ON handymans_tbl.service_id = other_servicestbl.service_id
                LEFT JOIN vtpartner.categorytbl ON categorytbl.category_id = handymans_tbl.category_id
                ORDER BY handyman_id DESC
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                mapped_results.append({
                    "handyman_id": row[0],
                    "name": row[1],
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
                    "sub_cat_id": row[16],
                    "service_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "aadhar_card_front": row[25],
                    "aadhar_card_back": row[26],
                    "pan_card_front": row[27],
                    "pan_card_back": row[28],
                    "sub_cat_name": row[29],
                    "service_name": row[30],
                    "category_name": row[31]
                })

            return JsonResponse({"all_handy_man_details": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def all_drivers(request):
    if request.method == "POST":
        try:
            query = """
                SELECT 
                    other_driverstbl.other_driver_id,
                    other_driverstbl.driver_first_name,
                    other_driverstbl.profile_pic,
                    other_driverstbl.is_online,
                    other_driverstbl.ratings,
                    other_driverstbl.mobile_no,
                    other_driverstbl.registration_date,
                    other_driverstbl.time,
                    other_driverstbl.r_lat,
                    other_driverstbl.r_lng,
                    other_driverstbl.current_lat,
                    other_driverstbl.current_lng,
                    other_driverstbl.status,
                    other_driverstbl.recent_online_pic,
                    other_driverstbl.is_verified,
                    other_driverstbl.category_id,
                    other_driverstbl.sub_cat_id,
                    other_driverstbl.service_id,
                    other_driverstbl.city_id,
                    other_driverstbl.aadhar_no,
                    other_driverstbl.pan_card_no,
                    other_driverstbl.house_no,
                    other_driverstbl.city_name,
                    other_driverstbl.full_address,
                    other_driverstbl.gender,
                    other_driverstbl.aadhar_card_front,
                    other_driverstbl.aadhar_card_back,
                    other_driverstbl.pan_card_front,
                    other_driverstbl.pan_card_back,
                    sub_categorytbl.sub_cat_name,
                    other_servicestbl.service_name,
                    categorytbl.category_name
                FROM 
                    vtpartner.other_driverstbl
                LEFT JOIN 
                    vtpartner.sub_categorytbl 
ON 
                    other_driverstbl.sub_cat_id = sub_categorytbl.sub_cat_id
                LEFT JOIN 
                    vtpartner.other_servicestbl 
                ON 
                    other_driverstbl.service_id = other_servicestbl.service_id
                LEFT JOIN 
                    vtpartner.categorytbl 
                ON 
                    other_driverstbl.category_id = categorytbl.category_id
                ORDER BY 
                    other_driverstbl.other_driver_id DESC;

            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                mapped_results.append({
                    "other_driver_id": row[0],
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
                    "sub_cat_id": row[16],
                    "service_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "aadhar_card_front": row[25],
                    "aadhar_card_back": row[26],
                    "pan_card_front": row[27],
                    "pan_card_back": row[28],
                    "sub_cat_name": row[29],
                    "service_name": row[30],
                    "category_name": row[31]
                })

            return JsonResponse({"all_drivers_details": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def edit_driver_details(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            print("req.body::", data)

            # Extract fields from request body
            driver_id= data.get("driver_id")
            agent_name= data.get("agent_name")
            mobile_no= data.get("mobile_no")
            gender= data.get("gender")
            aadhar_no= data.get("aadhar_no")
            pan_no= data.get("pan_no")
            city_name= data.get("city_name")
            house_no= data.get("house_no")
            address= data.get("address")
            agent_photo_url= data.get("agent_photo_url")
            aadhar_card_front_url= data.get("aadhar_card_front_url")
            aadhar_card_back_url= data.get("aadhar_card_back_url")
            pan_card_front_url= data.get("pan_card_front_url")
            pan_card_back_url= data.get("pan_card_back_url")
            license_front_url= data.get("license_front_url")
            license_back_url= data.get("license_back_url")
            insurance_image_url= data.get("insurance_image_url")
            noc_image_url= data.get("noc_image_url")
            pollution_certificate_image_url= data.get("pollution_certificate_image_url")
            rc_image_url= data.get("rc_image_url")
            vehicle_image_url= data.get("vehicle_image_url")
            category_id= int(data.get("category_id"))
            vehicle_id= data.get("vehicle_id")
            city_id= data.get("city_id")
            owner_name= data.get("owner_name")
            owner_mobile_no= data.get("owner_mobile_no")
            owner_house_no= data.get("owner_house_no")
            owner_city_name= data.get("owner_city_name")
            owner_address= data.get("owner_address")
            owner_photo_url= data.get("owner_photo_url")
            vehicle_plate_image= data.get("vehicle_plate_image")
            driving_license_no= data.get("driving_license_no")
            vehicle_plate_no= data.get("vehicle_plate_no")
            rc_no= data.get("rc_no")
            insurance_no= data.get("insurance_no")
            noc_no= data.get("noc_no")
            owner_id= data.get("owner_id")             # Required fields chec
            
            
            required_fields = {
                "driver_id": driver_id,
                "agent_name": agent_name,
                "mobile_no": mobile_no,
                "gender": gender,
                "aadhar_no": aadhar_no,
                "pan_no": pan_no,
                "city_name": city_name,
                "house_no": house_no,
                "address": address,
                "agent_photo_url": agent_photo_url,
                "aadhar_card_front_url": aadhar_card_front_url,
                "aadhar_card_back_url": aadhar_card_back_url,
                "pan_card_front_url": pan_card_front_url,
                "pan_card_back_url": pan_card_back_url,
                "license_front_url": license_front_url,
                "license_back_url": license_back_url,
                "insurance_image_url": insurance_image_url,
                "noc_image_url": noc_image_url,
                "pollution_certificate_image_url": pollution_certificate_image_url,
                "rc_image_url": rc_image_url,
                "vehicle_image_url": vehicle_image_url,
                "category_id": category_id,
                "vehicle_id": vehicle_id,
                "city_id": city_id,
                "owner_name": owner_name,
                "owner_mobile_no": owner_mobile_no,
                "owner_house_no": owner_house_no,
                "owner_city_name": owner_city_name,
                "owner_address": owner_address,
                "owner_photo_url": owner_photo_url,
                "vehicle_plate_image": vehicle_plate_image,
                "driving_license_no": driving_license_no,
                "vehicle_plate_no": vehicle_plate_no,
                "rc_no": rc_no,
                "insurance_no": insurance_no,
                "noc_no": noc_no,
                "owner_id": owner_id,
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            

            # Handle Owner details
            if owner_name and owner_mobile_no:
                check_owner_query = """
                    SELECT owner_id FROM vtpartner.owner_tbl 
                    WHERE owner_mobile_no = %s
                """
                owner_result = select_query(check_owner_query, [owner_mobile_no])

                if owner_result:
                    owner_id = owner_result[0][0]
                    update_owner_query = """
                        UPDATE vtpartner.owner_tbl SET 
                        house_no = %s, city_name = %s, address = %s, profile_photo = %s, owner_name = %s 
                        WHERE owner_id = %s
                    """
                    owner_values = [
                        owner_house_no,
                        owner_city_name,
                        owner_address,
                        owner_photo_url,
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
                        owner_house_no,
                        owner_city_name,
                        owner_address,
                        owner_photo_url,
                    ]
                    new_owner_result = insert_query(insert_owner_query, owner_values)
                    owner_id = new_owner_result[0][0]

            # Determine the driver table and ID field based on category_id
            
            if category_id == 1:
                driver_table = "vtpartner.goods_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "goods_driver_id"
            elif category_id == 2:
                driver_table = "vtpartner.cab_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "cab_driver_id"
            elif category_id == 3:
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
            data = json.loads(request.body)
            print("req.body::", data)

            # Extract fields from request body
            
            agent_name= data.get("agent_name")
            mobile_no= data.get("mobile_no")
            gender= data.get("gender")
            aadhar_no= data.get("aadhar_no")
            pan_no= data.get("pan_no")
            city_name= data.get("city_name")
            house_no= data.get("house_no")
            address= data.get("address")
            agent_photo_url= data.get("agent_photo_url")
            aadhar_card_front_url= data.get("aadhar_card_front_url")
            aadhar_card_back_url= data.get("aadhar_card_back_url")
            pan_card_front_url= data.get("pan_card_front_url")
            pan_card_back_url= data.get("pan_card_back_url")
            license_front_url= data.get("license_front_url")
            license_back_url= data.get("license_back_url")
            insurance_image_url= data.get("insurance_image_url")
            noc_image_url= data.get("noc_image_url")
            pollution_certificate_image_url= data.get("pollution_certificate_image_url")
            rc_image_url= data.get("rc_image_url")
            vehicle_image_url= data.get("vehicle_image_url")
            category_id= int(data.get("category_id"))
            vehicle_id= data.get("vehicle_id")
            city_id= data.get("city_id")
            owner_name= data.get("owner_name")
            owner_mobile_no= data.get("owner_mobile_no")
            owner_house_no= data.get("owner_house_no")
            owner_city_name= data.get("owner_city_name")
            owner_address= data.get("owner_address")
            owner_photo_url= data.get("owner_photo_url")
            vehicle_plate_image= data.get("vehicle_plate_image")
            driving_license_no= data.get("driving_license_no")
            vehicle_plate_no= data.get("vehicle_plate_no")
            rc_no= data.get("rc_no")
            insurance_no= data.get("insurance_no")
            noc_no= data.get("noc_no")
            owner_id= data.get("owner_id")             # Required fields chec
            
            
            required_fields = {
                
                "agent_name": agent_name,
                "mobile_no": mobile_no,
                "gender": gender,
                "aadhar_no": aadhar_no,
                "pan_no": pan_no,
                "city_name": city_name,
                "house_no": house_no,
                "address": address,
                "agent_photo_url": agent_photo_url,
                "aadhar_card_front_url": aadhar_card_front_url,
                "aadhar_card_back_url": aadhar_card_back_url,
                "pan_card_front_url": pan_card_front_url,
                "pan_card_back_url": pan_card_back_url,
                "license_front_url": license_front_url,
                "license_back_url": license_back_url,
                "insurance_image_url": insurance_image_url,
                "noc_image_url": noc_image_url,
                "pollution_certificate_image_url": pollution_certificate_image_url,
                "rc_image_url": rc_image_url,
                "vehicle_image_url": vehicle_image_url,
                "category_id": category_id,
                "vehicle_id": vehicle_id,
                "city_id": city_id,
                "owner_name": owner_name,
                "owner_mobile_no": owner_mobile_no,
                "owner_house_no": owner_house_no,
                "owner_city_name": owner_city_name,
                "owner_address": owner_address,
                "owner_photo_url": owner_photo_url,
                "vehicle_plate_image": vehicle_plate_image,
                "driving_license_no": driving_license_no,
                "vehicle_plate_no": vehicle_plate_no,
                "rc_no": rc_no,
                "insurance_no": insurance_no,
                "noc_no": noc_no,
                "owner_id": owner_id,
            }

            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400)

            owner_id = None
            

            if owner_name and owner_mobile_no:
                check_owner_query = """
                    SELECT owner_id FROM vtpartner.owner_tbl 
                    WHERE owner_mobile_no = %s
                """
                owner_result = select_query(check_owner_query, [owner_mobile_no])

                if owner_result:
                    owner_id = owner_result[0][0]
                else:
                    insert_owner_query = """
                        INSERT INTO vtpartner.owner_tbl 
                        (owner_name, owner_mobile_no, house_no, city_name, address, profile_photo) 
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING owner_id
                    """
                    owner_values = [
                        owner_name,
                        owner_mobile_no,
                        owner_house_no,
                        owner_city_name,
                        owner_address,
                        owner_photo_url,
                    ]
                    new_owner_result = insert_query(insert_owner_query, owner_values)
                    owner_id = new_owner_result[0][0]
            print("owner_id::",owner_id)
            driver_table, name_column, driver_id_field = None, None, None

            # Determine the driver table and ID field based on category_id
            
            if category_id == 1:
                driver_table = "vtpartner.goods_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "goods_driver_id"
            elif category_id == 2:
                driver_table = "vtpartner.cab_driverstbl"
                name_column = "driver_first_name"
                driver_id_field = "cab_driver_id"
            elif category_id == 3:
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

            new_driver_result = insert_query(insert_driver_query, driver_values)
            return JsonResponse({"message": f"{new_driver_result[0][0]} row(s) inserted"}, status=201)

        except Exception as err:
            print("Error executing add new driver query", err)
            return JsonResponse({"message": "Error executing add new driver query"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def edit_handyman_details(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        handyman_id= data.get("handyman_id"),
        agent_name= data.get("agent_name"),
        mobile_no= data.get("mobile_no"),
        gender= data.get("gender"),
        aadhar_no= data.get("aadhar_no"),
        pan_no= data.get("pan_no"),
        city_id= data.get("city_id"),
        city_name= data.get("city_name"),
        house_no= data.get("house_no"),
        address= data.get("address"),
        category_id= data.get("category_id"),
        sub_cat_id= data.get("sub_cat_id"),
        service_id= data.get("service_id"),
        agent_photo_url= data.get("agent_photo_url"),
        aadhar_card_front_url= data.get("aadhar_card_front_url"),
        aadhar_card_back_url= data.get("aadhar_card_back_url"),
        pan_card_front_url= data.get("pan_card_front_url"),
        pan_card_back_url= data.get("pan_card_back_url"),
        
        required_fields = {
            "handyman_id": handyman_id,
            "agent_name": agent_name,
            "mobile_no": mobile_no,
            "gender": gender,
            "aadhar_no": aadhar_no,
            "pan_no": pan_no,
            "city_id": city_id,
            "city_name": city_name,
            "house_no": house_no,
            "address": address,
            "category_id": category_id,
            "sub_cat_id": sub_cat_id,
            "service_id": service_id,
            "agent_photo_url": agent_photo_url,
            "aadhar_card_front_url": aadhar_card_front_url,
            "aadhar_card_back_url": aadhar_card_back_url,
            "pan_card_front_url": pan_card_front_url,
            "pan_card_back_url": pan_card_back_url,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        update_driver_query = """
            UPDATE vtpartner.handymans_tbl
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
            category_id,
            city_id,
            sub_cat_id,
            service_id,
            handyman_id,
        ]

        row_count = update_query(update_driver_query, driver_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating driver query", err)
        return JsonResponse({"message": "Error executing updating driver query"}, status=500)

@csrf_exempt
def update_handyman_status(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        handyman_id= data.get("handyman_id"),
        status= data.get("status"),
        
        
        required_fields = {
            "handyman_id": handyman_id,
            "status": status,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        query = """
            UPDATE vtpartner.handymans_tbl
            SET 
                status = %s
            WHERE handyman_id = %s
        """

        update_values = [
            status,
            handyman_id,
        ]

        row_count = update_query(query, update_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating  query", err)
        return JsonResponse({"message": "Error executing updating  query"}, status=500)


@csrf_exempt
def update_other_driver_status(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        other_driver_id= data.get("other_driver_id"),
        status= data.get("status"),
        
        
        required_fields = {
            "other_driver_id": other_driver_id,
            "status": status,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        query = """
            UPDATE vtpartner.other_driverstbl
            SET status = %s
            WHERE other_driver_id = %s
        """

        update_values = [
            status,
            other_driver_id,
        ]

        row_count = update_query(query, update_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating  query", err)
        return JsonResponse({"message": "Error executing updating  query"}, status=500)

@csrf_exempt
def update_jcb_crane_driver_status(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        jcb_crane_driver_id= data.get("jcb_crane_driver_id"),
        status= data.get("status"),
        
        
        required_fields = {
            "jcb_crane_driver_id": jcb_crane_driver_id,
            "status": status,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        query = """
            UPDATE vtpartner.jcb_crane_driverstbl
            SET 
                status = %s
               
            WHERE jcb_crane_driver_id = %s
        """

        update_values = [
            status,
            jcb_crane_driver_id,
        ]

        row_count = update_query(query, update_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating  query", err)
        return JsonResponse({"message": "Error executing updating  query"}, status=500)

@csrf_exempt
def update_cab_driver_status(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        cab_driver_id= data.get("cab_driver_id"),
        status= data.get("status"),
        
        
        required_fields = {
            "cab_driver_id": cab_driver_id,
            "status": status,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        query = """
            UPDATE vtpartner.cab_driverstbl
            SET 
                status = %s
               
            WHERE cab_driver_id = %s
        """

        update_values = [
            status,
            cab_driver_id,
        ]

        row_count = update_query(query, update_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating  query", err)
        return JsonResponse({"message": "Error executing updating  query"}, status=500)

@csrf_exempt
def update_goods_driver_status(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        goods_driver_id= data.get("goods_driver_id"),
        status= data.get("status"),
        reason= data.get("reason"),
        
        
        required_fields = {
            "goods_driver_id": goods_driver_id,
            "status": status,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        update_values = [
            status,
            goods_driver_id,
        ]
        
        # Add reason to the query if it is not empty
        if reason:
            query = """
                UPDATE vtpartner.goods_driverstbl
                SET 
                    status = %s,
                    reason = %s
                WHERE goods_driver_id = %s
            """
            update_values = [status, reason, goods_driver_id]
        else:
            query = """
                UPDATE vtpartner.goods_driverstbl
                SET 
                    status = %s
                WHERE goods_driver_id = %s
            """


        row_count = update_query(query, update_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating  query", err)
        return JsonResponse({"message": "Error executing updating  query"}, status=500)


@csrf_exempt
def add_new_handyman_details(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        # handyman_id= data.get("handyman_id"),
        agent_name= data.get("agent_name"),
        mobile_no= data.get("mobile_no"),
        gender= data.get("gender"),
        aadhar_no= data.get("aadhar_no"),
        pan_no= data.get("pan_no"),
        city_id= data.get("city_id"),
        city_name= data.get("city_name"),
        house_no= data.get("house_no"),
        address= data.get("address"),
        category_id= data.get("category_id"),
        sub_cat_id= data.get("sub_cat_id"),
        service_id= data.get("service_id"),
        agent_photo_url= data.get("agent_photo_url"),
        aadhar_card_front_url= data.get("aadhar_card_front_url"),
        aadhar_card_back_url= data.get("aadhar_card_back_url"),
        pan_card_front_url= data.get("pan_card_front_url"),
        pan_card_back_url= data.get("pan_card_back_url"),
        
        required_fields = {
            # "handyman_id": handyman_id,
            "agent_name": agent_name,
            "mobile_no": mobile_no,
            "gender": gender,
            "aadhar_no": aadhar_no,
            "pan_no": pan_no,
            "city_id": city_id,
            "city_name": city_name,
            "house_no": house_no,
            "address": address,
            "category_id": category_id,
            "sub_cat_id": sub_cat_id,
            "service_id": service_id,
            "agent_photo_url": agent_photo_url,
            "aadhar_card_front_url": aadhar_card_front_url,
            "aadhar_card_back_url": aadhar_card_back_url,
            "pan_card_front_url": pan_card_front_url,
            "pan_card_back_url": pan_card_back_url,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        add_query = """
            INSERT INTO vtpartner.handymans_tbl
            (
                name ,
                mobile_no ,
                gender ,
                aadhar_no ,
                pan_card_no ,
                city_name ,
                house_no ,
                full_address ,
                profile_pic ,
                aadhar_card_front ,
                aadhar_card_back ,
                pan_card_front ,
                pan_card_back ,
                category_id ,
                city_id ,
                sub_cat_id ,
                service_id ,
                status ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'1')
            
        """

        add_values = [
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
            category_id,
            city_id,
            sub_cat_id,
            service_id,
            
        ]

        row_count = insert_query(add_query, add_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating driver query", err)
        return JsonResponse({"message": "Error executing updating driver query"}, status=500)

@csrf_exempt
def edit_other_driver_details(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        other_driver_id= data.get("other_driver_id"),
        agent_name= data.get("agent_name"),
        mobile_no= data.get("mobile_no"),
        gender= data.get("gender"),
        aadhar_no= data.get("aadhar_no"),
        pan_no= data.get("pan_no"),
        city_id= data.get("city_id"),
        city_name= data.get("city_name"),
        house_no= data.get("house_no"),
        address= data.get("address"),
        category_id= data.get("category_id"),
        sub_cat_id= data.get("sub_cat_id"),
        service_id= data.get("service_id"),
        agent_photo_url= data.get("agent_photo_url"),
        aadhar_card_front_url= data.get("aadhar_card_front_url"),
        aadhar_card_back_url= data.get("aadhar_card_back_url"),
        pan_card_front_url= data.get("pan_card_front_url"),
        pan_card_back_url= data.get("pan_card_back_url"),
        
        required_fields = {
            "other_driver_id": other_driver_id,
            "agent_name": agent_name,
            "mobile_no": mobile_no,
            "gender": gender,
            "aadhar_no": aadhar_no,
            "pan_no": pan_no,
            "city_id": city_id,
            "city_name": city_name,
            "house_no": house_no,
            "address": address,
            "category_id": category_id,
            "sub_cat_id": sub_cat_id,
            "service_id": service_id,
            "agent_photo_url": agent_photo_url,
            "aadhar_card_front_url": aadhar_card_front_url,
            "aadhar_card_back_url": aadhar_card_back_url,
            "pan_card_front_url": pan_card_front_url,
            "pan_card_back_url": pan_card_back_url,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        update_driver_query = """
            UPDATE vtpartner.other_driverstbl
            SET 
                driver_first_name = %s,
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
            WHERE other_driver_id = %s
        """

        driver_values = [
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
            category_id,
            city_id,
            sub_cat_id,
            service_id,
            other_driver_id,
        ]

        row_count = update_query(update_driver_query, driver_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing updating driver query", err)
        return JsonResponse({"message": "Error executing updating driver query"}, status=500)

@csrf_exempt
def add_other_driver_details(request):
    try:
        data = json.loads(request.body)
        print("body::",data)
        # other_driver_id= data.get("other_driver_id"),
        agent_name= data.get("agent_name"),
        mobile_no= data.get("mobile_no"),
        gender= data.get("gender"),
        aadhar_no= data.get("aadhar_no"),
        pan_no= data.get("pan_no"),
        city_id= data.get("city_id"),
        city_name= data.get("city_name"),
        house_no= data.get("house_no"),
        address= data.get("address"),
        category_id= data.get("category_id"),
        sub_cat_id= data.get("sub_cat_id"),
        service_id= data.get("service_id"),
        agent_photo_url= data.get("agent_photo_url"),
        aadhar_card_front_url= data.get("aadhar_card_front_url"),
        aadhar_card_back_url= data.get("aadhar_card_back_url"),
        pan_card_front_url= data.get("pan_card_front_url"),
        pan_card_back_url= data.get("pan_card_back_url"),
        
        required_fields = {
            # "other_driver_id": other_driver_id,
            "agent_name": agent_name,
            "mobile_no": mobile_no,
            "gender": gender,
            "aadhar_no": aadhar_no,
            "pan_no": pan_no,
            "city_id": city_id,
            "city_name": city_name,
            "house_no": house_no,
            "address": address,
            "category_id": category_id,
            "sub_cat_id": sub_cat_id,
            "service_id": service_id,
            "agent_photo_url": agent_photo_url,
            "aadhar_card_front_url": aadhar_card_front_url,
            "aadhar_card_back_url": aadhar_card_back_url,
            "pan_card_front_url": pan_card_front_url,
            "pan_card_back_url": pan_card_back_url,
        }

        missing_fields = check_missing_fields(required_fields)

        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )

        # Prepare the update query and values
        add_driver_query = """
            INSERT INTO vtpartner.other_driverstbl
            (
                driver_first_name,
                mobile_no ,
                gender ,
                aadhar_no ,
                pan_card_no ,
                city_name ,
                house_no ,
                full_address ,
                profile_pic ,
                aadhar_card_front ,
                aadhar_card_back ,
                pan_card_front ,
                pan_card_back ,
                category_id ,
                city_id ,
                sub_cat_id ,
                service_id ,
                status ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'1')
        """

        driver_values = [
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
            category_id,
            city_id,
            sub_cat_id,
            service_id,
            
        ]

        row_count = insert_query(add_driver_query, driver_values)

        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing adding driver query", err)
        return JsonResponse({"message": "Error executing adding driver query"}, status=500)

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

    
@csrf_exempt
def add_new_faq(request):
    try:
        data = json.loads(request.body)
        category_id = data.get("category_id")
        question = data.get("question")
        answer = data.get("answer")
        
        # List of required fields
        required_fields = {
            "category_id":category_id,
            "question":question,
            "answer":answer,
            
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
            INSERT INTO vtpartner.faqtbl 
            (question, answer, category_id) 
            VALUES (%s, %s, %s)
        """
        values = [
            question,
            answer,
            category_id,
            
        ]
        row_count = insert_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

    except Exception as err:
        print("Error executing add new faq query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)
    
@csrf_exempt
def edit_new_faq(request):
    try:
        data = json.loads(request.body)
        category_id = data.get("category_id")
        faq_id = data.get("faq_id")
        question = data.get("question")
        answer = data.get("answer")
        
        # List of required fields
        required_fields = {
            "category_id":category_id,
            "question":question,
            "answer":answer,
            "faq_id":faq_id,
            
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
            UPDATE vtpartner.faqtbl 
            SET question=%s, answer=%s, category_id=%s
            WHERE faqid=%s
        """
        values = [
            question,
            answer,
            category_id,
            faq_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

    except Exception as err:
        print("Error executing add new faq query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)

@csrf_exempt
def all_estimations(request):
    if request.method == "POST":
        try:
            query = """
                SELECT 
                    er.request_id,
                    er.category_id,
                    cat.category_name,
                    cat.category_image,
                    cat.description AS category_description,
                    er.start_address,
                    er.end_address,
                    er.work_description,
                    er.name,
                    er.mobile_no,
                    er.purpose,
                    er.hours,
                    er.days,
                    er.request_date,
                    er.request_time,
                    er.city_id,
                    er.request_type,
                    er.sub_cat_id,
                    sub.sub_cat_name,
                    sub.image AS sub_category_image,
                    er.service_id,
                    serv.service_name,
                    serv.service_image AS service_image
                FROM 
                    vtpartner.estimation_request_tbl er
                LEFT JOIN 
                    vtpartner.categorytbl cat ON er.category_id = cat.category_id
                LEFT JOIN 
                    vtpartner.sub_categorytbl sub ON er.sub_cat_id = sub.sub_cat_id
                LEFT JOIN 
                    vtpartner.other_servicestbl serv ON er.service_id = serv.service_id ORDER BY er.request_id DESC
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

                     # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                mapped_results.append({
                    "request_id": row[0],
                    "category_id": row[1],
                    "category_name": row[2],
                    "category_image": row[3],
                    "category_description": row[4],
                    "start_address": row[5],
                    "end_address": row[6],
                    "work_description": row[7],
                    "name": row[8],
                    "mobile_no": row[9],
                    "purpose": row[10],
                    "hours": row[11],
                    "days": row[12],
                    "request_date": row[13],
                    "request_time": row[14],
                    "city_id": row[15],
                    "request_type": row[16],
                    "sub_cat_id": row[17],
                    "sub_cat_name": row[18],
                    "sub_category_image": row[19],
                    "service_id": row[20],
                    "service_name": row[21],
                    "service_image": row[22]
                })

            return JsonResponse({"all_estimations_details": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)



@csrf_exempt
def get_total_goods_drivers_with_count(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")
            status = data.get("status")  # Status: 1 = Verified, 0 = Unverified, 2 = Blocked, 3 = Rejected

            if status not in [0, 1, 2, 3]:
                return JsonResponse({"message": "Invalid status provided"}, status=400)

            # Fetch total count separately
            count_query = f"SELECT COUNT(*) FROM vtpartner.goods_driverstbl WHERE status = {status};"
            total_count_result = select_query(count_query)
            total_count = total_count_result[0][0] if total_count_result else 0

            # Fetch driver details with explicit column selection
            query = f"""
                SELECT gd.goods_driver_id, gd.driver_first_name, gd.driver_last_name, 
                    gd.profile_pic, gd.is_online, gd.ratings, gd.mobile_no, 
                    gd.registration_date, gd.time, gd.r_lat, gd.r_lng, gd.current_lat, gd.current_lng, 
                    gd.status, gd.recent_online_pic, gd.is_verified, gd.category_id, 
                    gd.vehicle_id, gd.city_id, gd.aadhar_no, gd.pan_card_no, gd.house_no, 
                    gd.city_name, gd.full_address, gd.gender, gd.owner_id, gd.aadhar_card_front, 
                    gd.aadhar_card_back, gd.pan_card_front, gd.pan_card_back, gd.license_front, 
                    gd.license_back, gd.insurance_image, gd.noc_image, gd.pollution_certificate_image, 
                    gd.rc_image, gd.vehicle_image, gd.vehicle_plate_image, gd.driving_license_no, 
                    gd.vehicle_plate_no, gd.rc_no, gd.insurance_no, gd.noc_no, gd.vehicle_fuel_type, 
                    v.vehicle_name, 
                    v.image
                FROM vtpartner.goods_driverstbl gd
                LEFT JOIN vtpartner.vehiclestbl v ON gd.vehicle_id = v.vehicle_id AND gd.category_id = 1
                WHERE gd.status = {status}
                ORDER BY gd.goods_driver_id DESC
                {'LIMIT 10' if key is not None else ''};
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map results to a list of dictionaries
            mapped_results = []
            for row in result:
                mapped_results.append({
                    "goods_driver_id": row[0],
                    "driver_first_name": row[1],
                    "driver_last_name": row[2],
                    "profile_pic": row[3],
                    "is_online": row[4],
                    "ratings": row[5],
                    "mobile_no": row[6],
                    "registration_date": row[7],
                    "time": row[8],
                    "r_lat": row[9],
                    "r_lng": row[10],
                    "current_lat": row[11],
                    "current_lng": row[12],
                    "status": row[13],
                    "recent_online_pic": row[14],
                    "is_verified": row[15],
                    "category_id": row[16],
                    "vehicle_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "owner_id": row[25],
                    "aadhar_card_front": row[26],
                    "aadhar_card_back": row[27],
                    "pan_card_front": row[28],
                    "pan_card_back": row[29],
                    "license_front": row[30],
                    "license_back": row[31],
                    "insurance_image": row[32],
                    "noc_image": row[33],
                    "pollution_certificate_image": row[34],
                    "rc_image": row[35],
                    "driver_vehicle_image": row[36],
                    "vehicle_plate_image": row[37],
                    "driving_license_no": row[38],
                    "vehicle_plate_no": row[39],
                    "rc_no": row[40],
                    "insurance_no": row[41],
                    "noc_no": row[42],
                    "vehicle_fuel_type": row[43],
                    "vehicle_name": row[44],
                    "vehicle_image": row[45]
                })

            return JsonResponse({"drivers": mapped_results, "total_count": total_count}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

 

@csrf_exempt
def get_total_goods_drivers_verified_with_count(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")

            # Get the total count separately
            count_query = "SELECT COUNT(*) FROM vtpartner.goods_driverstbl WHERE status = 1;"
            total_count_result = select_query(count_query)
            total_count = total_count_result[0][0] if total_count_result else 0

            # Now fetch the driver data
            query = f"""
            SELECT gd.*, 
                v.vehicle_name, v.weight, v.description, v.image, v.size_image
            FROM vtpartner.goods_driverstbl gd
            LEFT JOIN vtpartner.vehiclestbl v ON gd.vehicle_id = v.vehicle_id AND gd.category_id = 1
            WHERE gd.status = 1 
            ORDER BY gd.goods_driver_id DESC
            {'LIMIT 10' if key is not None else ''};
            """

            result = select_query(query)

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results
            mapped_results = []
            for row in result:
                mapped_results.append({
                    "goods_driver_id": row[0],
                    "driver_first_name": row[1],
                    "driver_last_name": row[2],
                    "profile_pic": row[3],
                    "is_online": row[4],
                    "ratings": row[5],
                    "mobile_no": row[6],
                    "registration_date": row[7],
                    "time": row[8],
                    "r_lat": row[9],
                    "r_lng": row[10],
                    "current_lat": row[11],
                    "current_lng": row[12],
                    "status": row[13],
                    "recent_online_pic": row[14],
                    "is_verified": row[15],
                    "category_id": row[16],
                    "vehicle_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "owner_id": row[25],
                    "aadhar_card_front": row[26],
                    "aadhar_card_back": row[27],
                    "pan_card_front": row[28],
                    "pan_card_back": row[29],
                    "license_front": row[30],
                    "license_back": row[31],
                    "insurance_image": row[32],
                    "noc_image": row[33],
                    "pollution_certificate_image": row[34],
                    "rc_image": row[35],
                    "driver_vehicle_image": row[36],
                    "vehicle_plate_image": row[37],
                    "driving_license_no": row[38],
                    "vehicle_plate_no": row[39],
                    "rc_no": row[40],
                    "insurance_no": row[41],
                    "noc_no": row[42],
                    "vehicle_fuel_type": row[43],
                    "authtoken": row[44],
                    "otp_no": row[45],
                    "vehicle_name": row[46] if row[46] else "NA",
                    "vehicle_weight": row[47] if row[47] else "NA",
                    "vehicle_description": row[48] if row[48] else "NA",
                    "vehicle_image": row[49] if row[49] else "NA",
                    "vehicle_size_image": row[50] if row[50] else "NA",
                })

            # Return the result with total count
            return JsonResponse({"drivers": mapped_results, "total_count": total_count}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt
def get_total_goods_drivers_un_verified_with_count(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")
            if key !=None:
                query = """
                    SELECT *, 
                        (SELECT COUNT(*) 
                            FROM vtpartner.goods_driverstbl 
                            WHERE status = 0) AS total_count
                    FROM vtpartner.goods_driverstbl
                    WHERE status = 0 ORDER BY goods_driver_id DESC LIMIT 10;
                """
            else:
                query = """
                    SELECT *, 
                        (SELECT COUNT(*) 
                            FROM vtpartner.goods_driverstbl 
                            WHERE status = 0) AS total_count
                    FROM vtpartner.goods_driverstbl
                    WHERE status = 0 ORDER BY goods_driver_id DESC;
                """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "goods_driver_id": row[0],
                    "driver_first_name": row[1],
                    "driver_last_name": row[2],
                    "profile_pic": row[3],
                    "is_online": row[4],
                    "ratings": row[5],
                    "mobile_no": row[6],
                    "registration_date": row[7],
                    "time": row[8],
                    "r_lat": row[9],
                    "r_lng": row[10],
                    "current_lat": row[11],
                    "current_lng": row[12],
                    "status": row[13],
                    "recent_online_pic": row[14],
                    "is_verified": row[15],
                    "category_id": row[16],
                    "vehicle_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "owner_id": row[25],
                    "aadhar_card_front": row[26],
                    "aadhar_card_back": row[27],
                    "pan_card_front": row[28],
                    "pan_card_back": row[29],
                    "license_front": row[30],
                    "license_back": row[31],
                    "insurance_image": row[32],
                    "noc_image": row[33],
                    "pollution_certificate_image": row[34],
                    "rc_image": row[35],
                    "vehicle_image": row[36],
                    "vehicle_plate_image": row[37],
                    "driving_license_no": row[38],
                    "vehicle_plate_no": row[39],
                    "rc_no": row[40],
                    "insurance_no": row[41],
                    "noc_no": row[42],
                    "vehicle_fuel_type": row[43],
                    "authtoken": row[44],
                    "otp_no": row[45],
                    "total_count": row[-1],  # The total count is the last column
                })

            return JsonResponse({"drivers": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_total_goods_drivers_blocked_with_count(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")
            if key !=None:
                query = """
                    SELECT *, 
                        (SELECT COUNT(*) 
                            FROM vtpartner.goods_driverstbl 
                            WHERE status = 2) AS total_count
                    FROM vtpartner.goods_driverstbl
                    WHERE status = 2 ORDER BY goods_driver_id DESC LIMIT 10;
                """
            else:
                query = """
                SELECT *, 
                       (SELECT COUNT(*) 
                        FROM vtpartner.goods_driverstbl 
                        WHERE status = 2) AS total_count
                FROM vtpartner.goods_driverstbl
                WHERE status = 2 ORDER BY goods_driver_id DESC;
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "goods_driver_id": row[0],
                    "driver_first_name": row[1],
                    "driver_last_name": row[2],
                    "profile_pic": row[3],
                    "is_online": row[4],
                    "ratings": row[5],
                    "mobile_no": row[6],
                    "registration_date": row[7],
                    "time": row[8],
                    "r_lat": row[9],
                    "r_lng": row[10],
                    "current_lat": row[11],
                    "current_lng": row[12],
                    "status": row[13],
                    "recent_online_pic": row[14],
                    "is_verified": row[15],
                    "category_id": row[16],
                    "vehicle_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "owner_id": row[25],
                    "aadhar_card_front": row[26],
                    "aadhar_card_back": row[27],
                    "pan_card_front": row[28],
                    "pan_card_back": row[29],
                    "license_front": row[30],
                    "license_back": row[31],
                    "insurance_image": row[32],
                    "noc_image": row[33],
                    "pollution_certificate_image": row[34],
                    "rc_image": row[35],
                    "vehicle_image": row[36],
                    "vehicle_plate_image": row[37],
                    "driving_license_no": row[38],
                    "vehicle_plate_no": row[39],
                    "rc_no": row[40],
                    "insurance_no": row[41],
                    "noc_no": row[42],
                    "vehicle_fuel_type": row[43],
                    "authtoken": row[44],
                    "otp_no": row[45],
                    "total_count": row[-1],  # The total count is the last column
                })

            return JsonResponse({"drivers": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_total_goods_drivers_rejected_with_count(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")
            if key !=None:
                query = """
                    SELECT *, 
                        (SELECT COUNT(*) 
                            FROM vtpartner.goods_driverstbl 
                            WHERE status = 3) AS total_count
                    FROM vtpartner.goods_driverstbl
                    WHERE status = 3 ORDER BY goods_driver_id DESC LIMIT 10;
                """
            else:
                query = """
                SELECT *, 
                       (SELECT COUNT(*) 
                        FROM vtpartner.goods_driverstbl 
                        WHERE status = 3) AS total_count
                FROM vtpartner.goods_driverstbl
                WHERE status = 3 ORDER BY goods_driver_id DESC;
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "goods_driver_id": row[0],
                    "driver_first_name": row[1],
                    "driver_last_name": row[2],
                    "profile_pic": row[3],
                    "is_online": row[4],
                    "ratings": row[5],
                    "mobile_no": row[6],
                    "registration_date": row[7],
                    "time": row[8],
                    "r_lat": row[9],
                    "r_lng": row[10],
                    "current_lat": row[11],
                    "current_lng": row[12],
                    "status": row[13],
                    "recent_online_pic": row[14],
                    "is_verified": row[15],
                    "category_id": row[16],
                    "vehicle_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "owner_id": row[25],
                    "aadhar_card_front": row[26],
                    "aadhar_card_back": row[27],
                    "pan_card_front": row[28],
                    "pan_card_back": row[29],
                    "license_front": row[30],
                    "license_back": row[31],
                    "insurance_image": row[32],
                    "noc_image": row[33],
                    "pollution_certificate_image": row[34],
                    "rc_image": row[35],
                    "vehicle_image": row[36],
                    "vehicle_plate_image": row[37],
                    "driving_license_no": row[38],
                    "vehicle_plate_no": row[39],
                    "rc_no": row[40],
                    "insurance_no": row[41],
                    "noc_no": row[42],
                    "vehicle_fuel_type": row[43],
                    "authtoken": row[44],
                    "otp_no": row[45],
                    "total_count": row[-1],  # The total count is the last column
                })

            return JsonResponse({"drivers": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

# 1. Get total orders count and total earnings
@csrf_exempt
def get_total_goods_drivers_orders_and_earnings(request):
    if request.method == "POST":
        try:
            query = """
                SELECT COUNT(*) AS total_orders, 
                       SUM(total_price) AS total_earnings
                FROM vtpartner.orders_tbl;
            """
            result = select_query(query)
            if result:
                total_orders, total_earnings = result[0]
                return JsonResponse({
                    "total_orders": total_orders,
                    "total_earnings": total_earnings
                }, status=200)
            else:
                return JsonResponse({"message": "No Data Found"}, status=404)
        except Exception as e:
            print("Error:", e)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    return JsonResponse({"message": "Method not allowed"}, status=405)

# 2. Get today's earnings
@csrf_exempt
def get_goods_drivers_today_earnings(request):
    if request.method == "POST":
        try:
            query = """
                SELECT SUM(total_price) AS today_earnings
                FROM vtpartner.orders_tbl
                WHERE booking_date = CURRENT_DATE;
            """
            result = select_query(query)
            if result:
                today_earnings = result[0][0] or 0
                return JsonResponse({"today_earnings": today_earnings}, status=200)
            else:
                return JsonResponse({"message": "No Data Found"}, status=404)
        except Exception as e:
            print("Error:", e)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    return JsonResponse({"message": "Method not allowed"}, status=405)

# 3. Get current month's earnings
@csrf_exempt
def get_goods_drivers_current_month_earnings(request):
    if request.method == "POST":
        try:
            query = """
                SELECT SUM(total_price) AS current_month_earnings
                FROM vtpartner.orders_tbl
                WHERE EXTRACT(MONTH FROM booking_date) = EXTRACT(MONTH FROM CURRENT_DATE)
                  AND EXTRACT(YEAR FROM booking_date) = EXTRACT(YEAR FROM CURRENT_DATE);
            """
            result = select_query(query)
            if result:
                current_month_earnings = result[0][0] or 0
                return JsonResponse({"current_month_earnings": current_month_earnings}, status=200)
            else:
                return JsonResponse({"message": "No Data Found"}, status=404)
        except Exception as e:
            print("Error:", e)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

@csrf_exempt
def get_goods_all_ongoing_bookings_details(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")
            allBookings = data.get("allBookings")
            if key !=None:
                query = """
                    SELECT 
                    bookings_tbl.booking_id,
                    bookings_tbl.customer_id,
                    bookings_tbl.driver_id,
                    bookings_tbl.pickup_lat,
                    bookings_tbl.pickup_lng,
                    bookings_tbl.destination_lat,
                    bookings_tbl.destination_lng,
                    bookings_tbl.distance,
                    bookings_tbl.time,
                    bookings_tbl.total_price,
                    bookings_tbl.base_price,
                    bookings_tbl.booking_timing,
                    bookings_tbl.booking_date,
                    bookings_tbl.booking_status,
                    bookings_tbl.driver_arrival_time,
                    bookings_tbl.otp,
                    bookings_tbl.gst_amount,
                    bookings_tbl.igst_amount,
                    bookings_tbl.goods_type_id,
                    bookings_tbl.payment_method,
                    bookings_tbl.city_id,
                    bookings_tbl.cancelled_reason,
                    bookings_tbl.cancel_time,
                    bookings_tbl.order_id,
                    bookings_tbl.sender_name,
                    bookings_tbl.sender_number,
                    bookings_tbl.receiver_name,
                    bookings_tbl.receiver_number,
                    goods_driverstbl.driver_first_name,
                    goods_driverstbl.authtoken AS driver_authtoken,
                    customers_tbl.customer_name,
                    customers_tbl.authtoken AS customer_authtoken,
                    bookings_tbl.pickup_address,
                    bookings_tbl.drop_address,
                    customers_tbl.mobile_no AS customer_mobile_no,
                    goods_driverstbl.mobile_no AS driver_mobile_no,
                    vehiclestbl.vehicle_id,
                    vehiclestbl.vehicle_name,
                    vehiclestbl.image
                FROM 
                    vtpartner.bookings_tbl
                INNER JOIN 
                    vtpartner.goods_driverstbl 
                    ON goods_driverstbl.goods_driver_id = bookings_tbl.driver_id
                INNER JOIN 
                    vtpartner.customers_tbl 
                    ON customers_tbl.customer_id = bookings_tbl.customer_id
                INNER JOIN 
                    vtpartner.vehiclestbl 
                    ON vehiclestbl.vehicle_id = goods_driverstbl.vehicle_id
                WHERE bookings_tbl.booking_status!='Cancelled' AND bookings_tbl.booking_status!='End Trip'
                ORDER BY 
                    bookings_tbl.booking_id DESC LIMIT 10;
                """
            elif allBookings!=None:
                query = """
                SELECT 
                    bookings_tbl.booking_id,
                    bookings_tbl.customer_id,
                    bookings_tbl.driver_id,
                    bookings_tbl.pickup_lat,
                    bookings_tbl.pickup_lng,
                    bookings_tbl.destination_lat,
                    bookings_tbl.destination_lng,
                    bookings_tbl.distance,
                    bookings_tbl.time,
                    bookings_tbl.total_price,
                    bookings_tbl.base_price,
                    bookings_tbl.booking_timing,
                    bookings_tbl.booking_date,
                    bookings_tbl.booking_status,
                    bookings_tbl.driver_arrival_time,
                    bookings_tbl.otp,
                    bookings_tbl.gst_amount,
                    bookings_tbl.igst_amount,
                    bookings_tbl.goods_type_id,
                    bookings_tbl.payment_method,
                    bookings_tbl.city_id,
                    bookings_tbl.cancelled_reason,
                    bookings_tbl.cancel_time,
                    bookings_tbl.order_id,
                    bookings_tbl.sender_name,
                    bookings_tbl.sender_number,
                    bookings_tbl.receiver_name,
                    bookings_tbl.receiver_number,
                    goods_driverstbl.driver_first_name,
                    goods_driverstbl.authtoken AS driver_authtoken,
                    customers_tbl.customer_name,
                    customers_tbl.authtoken AS customer_authtoken,
                    bookings_tbl.pickup_address,
                    bookings_tbl.drop_address,
                    customers_tbl.mobile_no AS customer_mobile_no,
                    goods_driverstbl.mobile_no AS driver_mobile_no,
                    vehiclestbl.vehicle_id,
                    vehiclestbl.vehicle_name,
                    vehiclestbl.image
                FROM 
                    vtpartner.bookings_tbl
                INNER JOIN 
                    vtpartner.goods_driverstbl 
                    ON goods_driverstbl.goods_driver_id = bookings_tbl.driver_id
                INNER JOIN 
                    vtpartner.customers_tbl 
                    ON customers_tbl.customer_id = bookings_tbl.customer_id
                INNER JOIN 
                    vtpartner.vehiclestbl 
                    ON vehiclestbl.vehicle_id = goods_driverstbl.vehicle_id
                
                ORDER BY 
                    bookings_tbl.booking_id DESC;
            """
            else:
                query = """
                SELECT 
                    bookings_tbl.booking_id,
                    bookings_tbl.customer_id,
                    bookings_tbl.driver_id,
                    bookings_tbl.pickup_lat,
                    bookings_tbl.pickup_lng,
                    bookings_tbl.destination_lat,
                    bookings_tbl.destination_lng,
                    bookings_tbl.distance,
                    bookings_tbl.time,
                    bookings_tbl.total_price,
                    bookings_tbl.base_price,
                    bookings_tbl.booking_timing,
                    bookings_tbl.booking_date,
                    bookings_tbl.booking_status,
                    bookings_tbl.driver_arrival_time,
                    bookings_tbl.otp,
                    bookings_tbl.gst_amount,
                    bookings_tbl.igst_amount,
                    bookings_tbl.goods_type_id,
                    bookings_tbl.payment_method,
                    bookings_tbl.city_id,
                    bookings_tbl.cancelled_reason,
                    bookings_tbl.cancel_time,
                    bookings_tbl.order_id,
                    bookings_tbl.sender_name,
                    bookings_tbl.sender_number,
                    bookings_tbl.receiver_name,
                    bookings_tbl.receiver_number,
                    goods_driverstbl.driver_first_name,
                    goods_driverstbl.authtoken AS driver_authtoken,
                    customers_tbl.customer_name,
                    customers_tbl.authtoken AS customer_authtoken,
                    bookings_tbl.pickup_address,
                    bookings_tbl.drop_address,
                    customers_tbl.mobile_no AS customer_mobile_no,
                    goods_driverstbl.mobile_no AS driver_mobile_no,
                    vehiclestbl.vehicle_id,
                    vehiclestbl.vehicle_name,
                    vehiclestbl.image
                FROM 
                    vtpartner.bookings_tbl
                INNER JOIN 
                    vtpartner.goods_driverstbl 
                    ON goods_driverstbl.goods_driver_id = bookings_tbl.driver_id
                INNER JOIN 
                    vtpartner.customers_tbl 
                    ON customers_tbl.customer_id = bookings_tbl.customer_id
                INNER JOIN 
                    vtpartner.vehiclestbl 
                    ON vehiclestbl.vehicle_id = goods_driverstbl.vehicle_id
                WHERE bookings_tbl.booking_status!='Cancelled' AND bookings_tbl.booking_status!='End Trip'
                ORDER BY 
                    bookings_tbl.booking_id DESC;
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "booking_id": str(row[0]),
                    "customer_id": str(row[1]),
                    "driver_id": str(row[2]),
                    "pickup_lat": str(row[3]),
                    "pickup_lng": str(row[4]),
                    "destination_lat": str(row[5]),
                    "destination_lng": str(row[6]),
                    "distance": str(row[7]),
                    "total_time": str(row[8]),
                    "total_price": str(row[9]),
                    "base_price": str(row[10]),
                    "booking_timing": str(row[11]),
                    "booking_date": str(row[12]),
                    "booking_status": str(row[13]),
                    "driver_arrival_time": str(row[14]),
                    "otp": str(row[15]),
                    "gst_amount": str(row[16]),
                    "igst_amount": str(row[17]),
                    "goods_type_id": str(row[18]),
                    "payment_method": str(row[19]),
                    "city_id": str(row[20]),
                    "cancelled_reason": str(row[21]),
                    "cancel_time": str(row[22]),
                    "order_id": str(row[23]),
                    "sender_name": str(row[24]),
                    "sender_number": str(row[25]),
                    "receiver_name": str(row[26]),
                    "receiver_number": str(row[27]),
                    "driver_first_name": str(row[28]),
                    "goods_driver_auth_token": str(row[29]),
                    "customer_name": str(row[30]),
                    "customers_auth_token": str(row[31]),
                    "pickup_address": str(row[32]),
                    "drop_address": str(row[33]),
                    "customer_mobile_no": str(row[34]),
                    "driver_mobile_no": str(row[35]),
                    "vehicle_id": str(row[36]),
                    "vehicle_name": str(row[37]),
                    "vehicle_image": str(row[38]),
                })

            return JsonResponse({"results": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_goods_all_cancelled_bookings_details(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")
            if key !=None:
                query = """
                    SELECT 
                    bookings_tbl.booking_id,
                    bookings_tbl.customer_id,
                    bookings_tbl.driver_id,
                    bookings_tbl.pickup_lat,
                    bookings_tbl.pickup_lng,
                    bookings_tbl.destination_lat,
                    bookings_tbl.destination_lng,
                    bookings_tbl.distance,
                    bookings_tbl.time,
                    bookings_tbl.total_price,
                    bookings_tbl.base_price,
                    bookings_tbl.booking_timing,
                    bookings_tbl.booking_date,
                    bookings_tbl.booking_status,
                    bookings_tbl.driver_arrival_time,
                    bookings_tbl.otp,
                    bookings_tbl.gst_amount,
                    bookings_tbl.igst_amount,
                    bookings_tbl.goods_type_id,
                    bookings_tbl.payment_method,
                    bookings_tbl.city_id,
                    bookings_tbl.cancelled_reason,
                    bookings_tbl.cancel_time,
                    bookings_tbl.order_id,
                    bookings_tbl.sender_name,
                    bookings_tbl.sender_number,
                    bookings_tbl.receiver_name,
                    bookings_tbl.receiver_number,
                    goods_driverstbl.driver_first_name,
                    goods_driverstbl.authtoken AS driver_authtoken,
                    customers_tbl.customer_name,
                    customers_tbl.authtoken AS customer_authtoken,
                    bookings_tbl.pickup_address,
                    bookings_tbl.drop_address,
                    customers_tbl.mobile_no AS customer_mobile_no,
                    goods_driverstbl.mobile_no AS driver_mobile_no,
                    vehiclestbl.vehicle_id,
                    vehiclestbl.vehicle_name,
                    vehiclestbl.image
                FROM 
                    vtpartner.bookings_tbl
                INNER JOIN 
                    vtpartner.goods_driverstbl 
                    ON goods_driverstbl.goods_driver_id = bookings_tbl.driver_id
                INNER JOIN 
                    vtpartner.customers_tbl 
                    ON customers_tbl.customer_id = bookings_tbl.customer_id
                INNER JOIN 
                    vtpartner.vehiclestbl 
                    ON vehiclestbl.vehicle_id = goods_driverstbl.vehicle_id
                WHERE bookings_tbl.booking_status ='Cancelled'
                ORDER BY 
                    bookings_tbl.booking_id DESC LIMIT 10;
                """
            else:
                query = """
                SELECT 
                    bookings_tbl.booking_id,
                    bookings_tbl.customer_id,
                    bookings_tbl.driver_id,
                    bookings_tbl.pickup_lat,
                    bookings_tbl.pickup_lng,
                    bookings_tbl.destination_lat,
                    bookings_tbl.destination_lng,
                    bookings_tbl.distance,
                    bookings_tbl.time,
                    bookings_tbl.total_price,
                    bookings_tbl.base_price,
                    bookings_tbl.booking_timing,
                    bookings_tbl.booking_date,
                    bookings_tbl.booking_status,
                    bookings_tbl.driver_arrival_time,
                    bookings_tbl.otp,
                    bookings_tbl.gst_amount,
                    bookings_tbl.igst_amount,
                    bookings_tbl.goods_type_id,
                    bookings_tbl.payment_method,
                    bookings_tbl.city_id,
                    bookings_tbl.cancelled_reason,
                    bookings_tbl.cancel_time,
                    bookings_tbl.order_id,
                    bookings_tbl.sender_name,
                    bookings_tbl.sender_number,
                    bookings_tbl.receiver_name,
                    bookings_tbl.receiver_number,
                    goods_driverstbl.driver_first_name,
                    goods_driverstbl.authtoken AS driver_authtoken,
                    customers_tbl.customer_name,
                    customers_tbl.authtoken AS customer_authtoken,
                    bookings_tbl.pickup_address,
                    bookings_tbl.drop_address,
                    customers_tbl.mobile_no AS customer_mobile_no,
                    goods_driverstbl.mobile_no AS driver_mobile_no,
                    vehiclestbl.vehicle_id,
                    vehiclestbl.vehicle_name,
                    vehiclestbl.image
                FROM 
                    vtpartner.bookings_tbl
                INNER JOIN 
                    vtpartner.goods_driverstbl 
                    ON goods_driverstbl.goods_driver_id = bookings_tbl.driver_id
                INNER JOIN 
                    vtpartner.customers_tbl 
                    ON customers_tbl.customer_id = bookings_tbl.customer_id
                INNER JOIN 
                    vtpartner.vehiclestbl 
                    ON vehiclestbl.vehicle_id = goods_driverstbl.vehicle_id
                WHERE bookings_tbl.booking_status ='Cancelled'
                ORDER BY 
                    bookings_tbl.booking_id DESC;
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "booking_id": str(row[0]),
                    "customer_id": str(row[1]),
                    "driver_id": str(row[2]),
                    "pickup_lat": str(row[3]),
                    "pickup_lng": str(row[4]),
                    "destination_lat": str(row[5]),
                    "destination_lng": str(row[6]),
                    "distance": str(row[7]),
                    "total_time": str(row[8]),
                    "total_price": str(row[9]),
                    "base_price": str(row[10]),
                    "booking_timing": str(row[11]),
                    "booking_date": str(row[12]),
                    "booking_status": str(row[13]),
                    "driver_arrival_time": str(row[14]),
                    "otp": str(row[15]),
                    "gst_amount": str(row[16]),
                    "igst_amount": str(row[17]),
                    "goods_type_id": str(row[18]),
                    "payment_method": str(row[19]),
                    "city_id": str(row[20]),
                    "cancelled_reason": str(row[21]),
                    "cancel_time": str(row[22]),
                    "order_id": str(row[23]),
                    "sender_name": str(row[24]),
                    "sender_number": str(row[25]),
                    "receiver_name": str(row[26]),
                    "receiver_number": str(row[27]),
                    "driver_first_name": str(row[28]),
                    "goods_driver_auth_token": str(row[29]),
                    "customer_name": str(row[30]),
                    "customers_auth_token": str(row[31]),
                    "pickup_address": str(row[32]),
                    "drop_address": str(row[33]),
                    "customer_mobile_no": str(row[34]),
                    "driver_mobile_no": str(row[35]),
                    "vehicle_id": str(row[36]),
                    "vehicle_name": str(row[37]),
                    "vehicle_image": str(row[38]),
                })

            return JsonResponse({"results": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_goods_all_completed_orders_details(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            key = data.get("key")
            if key !=None:
                query = """
                    select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and  vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id order by order_id desc LIMIT 10;
                """
            else:
                query = """
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and  vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id order by order_id desc;
            """

            result = select_query(query)  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "booking_id": str(row[0]),
                    "customer_id": str(row[1]),
                    "driver_id": str(row[2]),
                    "pickup_lat": str(row[3]),
                    "pickup_lng": str(row[4]),
                    "destination_lat": str(row[5]),
                    "destination_lng": str(row[6]),
                    "distance": str(row[7]),
                    "total_time": str(row[8]),
                    "total_price": str(row[9]),
                    "base_price": str(row[10]),
                    "booking_timing": str(row[11]),
                    "booking_date": str(row[12]),
                    "booking_status": str(row[13]),
                    "driver_arrival_time": str(row[14]),
                    "otp": str(row[15]),
                    "gst_amount": str(row[16]),
                    "igst_amount": str(row[17]),
                    "goods_type_id": str(row[18]),
                    "payment_method": str(row[19]),
                    "city_id": str(row[20]),
                    "order_id": str(row[21]),
                    "sender_name": str(row[22]),
                    "sender_number": str(row[23]),
                    "receiver_name": str(row[24]),
                    "receiver_number": str(row[25]),
                    "driver_first_name": str(row[26]),
                    "goods_driver_auth_token": str(row[27]),
                    "customer_name": str(row[28]),
                    "customers_auth_token": str(row[29]),
                    "pickup_address": str(row[30]),
                    "drop_address": str(row[31]),
                    "customer_mobile_no": str(row[32]),
                    "driver_mobile_no": str(row[33]),
                    "vehicle_id": str(row[34]),
                    "vehicle_name": str(row[35]),
                    "vehicle_image": str(row[36]),
                })

            return JsonResponse({"results": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_goods_booking_detail_with_id(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            booking_id = data.get("booking_id")
        
        

            # List of required fields
            required_fields = {
                "booking_id": booking_id,
            
            }
            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)
            
            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )
                
            query = """
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,vehicle_plate_no,vehicle_fuel_type,goods_driverstbl.profile_pic from vtpartner.vehiclestbl,vtpartner.bookings_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=bookings_tbl.driver_id and customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s  and vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id
            """

            result = select_query(query,[booking_id])  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "booking_id": row[0],
                    "customer_id": row[1],
                    "driver_id": row[2],
                    "pickup_lat": row[3],
                    "pickup_lng": row[4],
                    "destination_lat": row[5],
                    "destination_lng": row[6],
                    "distance": row[7],
                    "total_time": row[8],
                    "total_price": row[9],
                    "base_price": row[10],
                    "booking_timing": row[11],
                    "booking_date": row[12],
                    "booking_status": row[13],
                    "driver_arrival_time": row[14],
                    "otp": row[15],
                    "gst_amount": row[16],
                    "igst_amount": row[17],
                    "goods_type_id": row[18],
                    "payment_method": row[19],
                    "city_id": row[20],
                    "cancelled_reason": row[21],
                    "cancel_time": row[22],
                    "order_id": row[23],
                    "sender_name": row[24],
                    "sender_number": row[25],
                    "receiver_name": row[26],
                    "receiver_number": row[27],
                    "driver_first_name": row[28],
                    "goods_driver_auth_token": row[29],
                    "customer_name": row[30],
                    "customers_auth_token": row[31],
                    "pickup_address": row[32],
                    "drop_address": row[33],
                    "customer_mobile_no": row[34],
                    "driver_mobile_no": row[35],
                    "vehicle_id": str(row[36]),
                    "vehicle_name": str(row[37]),
                    "vehicle_image": str(row[38]),
                    "vehicle_plate_no": str(row[39]),
                    "vehicle_fuel_type": str(row[40]),
                    "profile_pic": str(row[41]),

                })

            return JsonResponse({"results": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_goods_order_detail_with_id(request):
    if request.method == "POST":
        data = json.loads(request.body)
        order_id = data.get("order_id")
        
        

        # List of required fields
        required_fields = {
            "order_id": order_id,
        
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
            
        try:
            query = """
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,vehicle_plate_no,vehicle_fuel_type,goods_driverstbl.profile_pic from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and order_id=%s  and vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id
            """
            result = select_query(query,[order_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "booking_id": row[0],
                    "customer_id": row[1],
                    "driver_id": row[2],
                    "pickup_lat": row[3],
                    "pickup_lng": row[4],
                    "destination_lat": row[5],
                    "destination_lng": row[6],
                    "distance": row[7],
                    "total_time": row[8],
                    "total_price": row[9],
                    "base_price": row[10],
                    "booking_timing": row[11],
                    "booking_date": row[12],
                    "booking_status": row[13],
                    "driver_arrival_time": row[14],
                    "otp": row[15],
                    "gst_amount": row[16],
                    "igst_amount": row[17],
                    "goods_type_id": row[18],
                    "payment_method": row[19],
                    "city_id": row[20],
                    # "cancelled_reason": row[21],
                    # "cancel_time": row[22],
                    "order_id": row[21],
                    "sender_name": row[22],
                    "sender_number": row[23],
                    "receiver_name": row[24],
                    "receiver_number": row[25],
                    "driver_first_name": row[26],
                    "goods_driver_auth_token": row[27],
                    "customer_name": row[28],
                    "customers_auth_token": row[29],
                    "pickup_address": row[30],
                    "drop_address": row[31],
                    "customer_mobile_no": row[32],
                    "driver_mobile_no": row[33],
                    "vehicle_id": str(row[34]),
                    "vehicle_name": str(row[35]),
                    "vehicle_image": str(row[36]),
                    "vehicle_plate_no": str(row[37]),
                    "vehicle_fuel_type": str(row[38]),
                    "profile_pic": str(row[39]),

                })

            return JsonResponse({"results": mapped_results}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_goods_booking_detail_history_with_id(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            booking_id = data.get("booking_id")
        
        

            # List of required fields
            required_fields = {
                "booking_id": booking_id,
            
            }
            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)
            
            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                    status=400
                )
                
            query = """
                select booking_history_id,status,time,date from vtpartner.bookings_history_tbl where booking_id=%s ORDER BY booking_history_id ASC 
            """

            result = select_query(query,[booking_id])  # Assuming select_query returns a list of tuples

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map the results to a list of dictionaries
            mapped_results = []
            for row in result:
                # Map columns to their values
                mapped_results.append({
                    "booking_history_id": row[0],
                    "status": row[1],
                    "time": row[2],
                    "date": row[3],
                    

                })

            return JsonResponse({"results": mapped_results}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def goods_driver_current_location(request):
    if request.method == "POST":
        data = json.loads(request.body)
        driver_id = data.get("driver_id")
        
        

        # List of required fields
        required_fields = {
            "driver_id": driver_id,
        
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
       
        try:
            query = """
               select current_lat,current_lng from vtpartner.active_goods_drivertbl where goods_driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "current_lat": row[0],
                    "current_lng": row[1],
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_all_goods_driver_online_current_location(request):
    if request.method == "POST":
        data = json.loads(request.body)
        city_id = data.get("city_id")
        
      
       
        try:
            if city_id!=None:
                query = """
                select active_goods_drivertbl.goods_driver_id,active_goods_drivertbl.current_lat,active_goods_drivertbl.current_lng,driver_first_name,profile_pic,entry_time,current_status,goods_driverstbl.mobile_no from vtpartner.active_goods_drivertbl,vtpartner.goods_driverstbl where active_goods_drivertbl.goods_driver_id=goods_driverstbl.goods_driver_id and city_id=%s
                """
                result = select_query(query,[city_id])  # Assuming select_query is defined elsewhere
            else:
                query = """
                select active_goods_drivertbl.goods_driver_id,active_goods_drivertbl.current_lat,active_goods_drivertbl.current_lng,driver_first_name,profile_pic,entry_time,current_status,goods_driverstbl.mobile_no from vtpartner.active_goods_drivertbl,vtpartner.goods_driverstbl where active_goods_drivertbl.goods_driver_id=goods_driverstbl.goods_driver_id
                """
                result = select_query(query,[])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "goods_driver_id": row[0],
                    "current_lat": row[1],
                    "current_lng": row[2],
                    "driver_first_name": row[3],
                    "profile_pic": row[4],
                    "entry_time": row[5],
                    "current_status": row[6],
                    "mobile_no": row[7],
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_goods_driver_details(request):
    if request.method == "POST":
        data = json.loads(request.body)
        goods_driver_id = data.get("goods_driver_id")
        
        try:
            if goods_driver_id is not None:
                # Use goods_driver_id for filtering
                query = """
                SELECT 
                gd.goods_driver_id, 
                gd.driver_first_name, 
                gd.driver_last_name, 
                gd.profile_pic, 
                gd.is_online, 
                gd.ratings, 
                gd.mobile_no, 
                gd.registration_date, 
                gd.time, 
                gd.r_lat, 
                gd.r_lng, 
                gd.current_lat, 
                gd.current_lng, 
                gd.status, 
                gd.recent_online_pic, 
                gd.is_verified, 
                gd.category_id, 
                gd.vehicle_id, 
                gd.city_id, 
                gd.aadhar_no, 
                gd.pan_card_no, 
                gd.house_no, 
                gd.city_name, 
                gd.full_address, 
                gd.gender, 
                gd.owner_id, 
                gd.aadhar_card_front, 
                gd.aadhar_card_back, 
                gd.pan_card_front, 
                gd.pan_card_back, 
                gd.license_front, 
                gd.license_back, 
                gd.insurance_image, 
                gd.noc_image, 
                gd.pollution_certificate_image, 
                gd.rc_image, 
                gd.vehicle_image, 
                gd.vehicle_plate_image, 
                gd.driving_license_no, 
                gd.vehicle_plate_no, 
                gd.rc_no, 
                gd.insurance_no, 
                gd.noc_no, 
                gd.vehicle_fuel_type, 
                gd.authtoken, 
                gd.otp_no, 
                v.vehicle_name, 
                v.weight, 
                v.description AS vehicle_description, 
                v.image AS vehicle_image, 
                v.size_image, 
                vt.vehicle_type_name,
                gd.reason
            FROM vtpartner.goods_driverstbl gd
            LEFT JOIN vtpartner.vehiclestbl v ON gd.vehicle_id = v.vehicle_id
            LEFT JOIN vtpartner.vehicle_types_tbl vt ON v.vehicle_type_id = vt.vehicle_type_id
            WHERE gd.goods_driver_id = %s

                """
                result = select_query(query, [goods_driver_id])  # Assuming select_query is defined elsewhere
            else:
                return JsonResponse({"message": "goods_driver_id is required"}, status=400)

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Mapping the row to a dictionary with all the column names
            driver_details = {
                "goods_driver_id": result[0][0],
                "driver_first_name": result[0][1],
                "driver_last_name": result[0][2],
                "profile_pic": result[0][3],
                "is_online": result[0][4],
                "ratings": result[0][5],
                "mobile_no": result[0][6],
                "registration_date": result[0][7],
                "time": result[0][8],
                "r_lat": result[0][9],
                "r_lng": result[0][10],
                "current_lat": result[0][11],
                "current_lng": result[0][12],
                "status": result[0][13],
                "recent_online_pic": result[0][14],
                "is_verified": result[0][15],
                "category_id": result[0][16],
                "vehicle_id": result[0][17],
                "city_id": result[0][18],
                "aadhar_no": result[0][19],
                "pan_card_no": result[0][20],
                "house_no": result[0][21],
                "city_name": result[0][22],
                "full_address": result[0][23],
                "gender": result[0][24],
                "owner_id": result[0][25],
                "aadhar_card_front": result[0][26],
                "aadhar_card_back": result[0][27],
                "pan_card_front": result[0][28],
                "pan_card_back": result[0][29],
                "license_front": result[0][30],
                "license_back": result[0][31],
                "insurance_image": result[0][32],
                "noc_image": result[0][33],
                "pollution_certificate_image": result[0][34],
                "rc_image": result[0][35],
                "driver_vehicle_image": result[0][36],
                "vehicle_plate_image": result[0][37],
                "driving_license_no": result[0][38],
                "vehicle_plate_no": result[0][39],
                "rc_no": result[0][40],
                "insurance_no": result[0][41],
                "noc_no": result[0][42],
                "vehicle_fuel_type": result[0][43],
                "authtoken": result[0][44],
                "otp_no": result[0][45],
                "vehicle_name": result[0][46],
                "vehicle_weight": result[0][47],
                "vehicle_description": result[0][48],
                "vehicle_image": result[0][49],
                "vehicle_size_image": result[0][50],
                "vehicle_type_name": result[0][51],
                "reason": result[0][52],
                
            }

            return JsonResponse({"result": driver_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def check_driver_status(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            driver_id = data.get('driver_id')

            if not driver_id:
                return JsonResponse({"message": "Driver ID is required"}, status=400)

            # Check if driver has any active bookings
            query = """
                SELECT current_booking_id 
                FROM vtpartner.active_goods_drivertbl 
                WHERE goods_driver_id = %s
            """
            result = select_query(query, (driver_id,))

            if not result:
                return JsonResponse({"is_free": True}, status=200)

            is_free = result[0][0] == -1
            return JsonResponse({"is_free": is_free}, status=200)

        except Exception as err:
            print("Error checking driver status:", err)
            return JsonResponse({"message": str(err)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def toggle_driver_online_status(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            driver_id = data.get('driver_id')
            new_status = data.get('online_status')  # 1 for online, 0 for offline
            current_lat = data.get('current_lat', 0)
            current_lng = data.get('current_lng', 0)

            if not driver_id:
                return JsonResponse({"message": "Driver ID is required"}, status=400)

            if new_status == 0:
                # Check if driver is free before going offline
                check_query = """
                    SELECT current_booking_id 
                    FROM vtpartner.active_goods_drivertbl 
                    WHERE goods_driver_id = %s
                """
                result = select_query(check_query, (driver_id,))
                
                if result and result[0][0] != -1:
                    return JsonResponse({
                        "message": "Driver has active bookings and cannot go offline"
                    }, status=400)

                # Delete from active drivers table
                delete_driver_query = """
                    DELETE FROM vtpartner.active_goods_drivertbl 
                    WHERE goods_driver_id = %s
                """
                delete_values = (driver_id,)
                delete_query(delete_driver_query, delete_values)

            else:
                # Add to active drivers table
                insert_driver_query = """
                    INSERT INTO vtpartner.active_goods_drivertbl 
                    (goods_driver_id, current_lat, current_lng) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (goods_driver_id) 
                    DO UPDATE SET 
                        current_lat = EXCLUDED.current_lat,
                        current_lng = EXCLUDED.current_lng,
                        entry_time = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
                """
                insert_values = (driver_id, current_lat, current_lng)
                insert_query(insert_driver_query, insert_values)

            # Update driver's online status
            update_driver_query = """
                UPDATE vtpartner.goods_driverstbl 
                SET is_online = %s 
                WHERE goods_driver_id = %s
            """
            update_values = (new_status, driver_id)
            update_query(update_driver_query, update_values)

            return JsonResponse({
                "message": f"Driver status updated to {'online' if new_status == 1 else 'offline'}"
            }, status=200)

        except Exception as err:
            print("Error toggling driver status:", err)
            return JsonResponse({"message": str(err)}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_estimation(request):
    try:
        data = json.loads(request.body)
        request_id = data.get("request_id")
        
        
        # List of required fields
        required_fields = {
            "request_id":request_id,
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
            DELETE from vtpartner.estimation_request_tbl 
            WHERE request_id=%s
        """
        values = [
            request_id,
        ]
        row_count = delete_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

    except Exception as err:
        print("Error executing deleting query estimation_request_tbl", err)
        return JsonResponse({"message": "Error executing deleting query"}, status=500)
   
@csrf_exempt
def delete_enquiry(request):
    try:
        data = json.loads(request.body)
        enquiry_id = data.get("enquiry_id")
        
        
        # List of required fields
        required_fields = {
            "enquiry_id":enquiry_id,
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
            DELETE from vtpartner.enquirytbl 
            WHERE enquiry_id=%s
        """
        values = [
            enquiry_id,
        ]
        row_count = delete_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) inserted"}, status=200)

    except Exception as err:
        print("Error executing deleting query enquirytbl", err)
        return JsonResponse({"message": "Error executing deleting query"}, status=500)

@csrf_exempt
def get_recharge_plans(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            category_id = data.get("category_id")
            page = data.get("page", 1)
            limit = data.get("limit", 10)
            search = data.get("search", "")
            
            offset = (page - 1) * limit
            
            query = """
                SELECT 
                    recharge_plan_id, plan_title, plan_description, 
                    plan_days, plan_epoch, expiry_days, plan_price,
                    category_id, vehicle_id
                FROM vtpartner.goods_driver_recharge_plans_tbl
                WHERE category_id = %s
                AND (
                    LOWER(plan_title) LIKE LOWER(%s) OR
                    LOWER(plan_description) LIKE LOWER(%s)
                )
                ORDER BY plan_epoch DESC
                LIMIT %s OFFSET %s
            """
            
            count_query = """
                SELECT COUNT(*) FROM vtpartner.goods_driver_recharge_plans_tbl
                WHERE category_id = %s
                AND (
                    LOWER(plan_title) LIKE LOWER(%s) OR
                    LOWER(plan_description) LIKE LOWER(%s)
                )
            """
            
            search_pattern = f"%{search}%"
            
            # Get total count
            count_result = select_query(count_query, [category_id, search_pattern, search_pattern])
            total_count = count_result[0][0] if count_result else 0
            
            # Get plans
            result = select_query(query, [category_id, search_pattern, search_pattern, limit, offset])
            
            plans = []
            for row in result:
                plans.append({
                    "recharge_plan_id": row[0],
                    "plan_title": row[1],
                    "plan_description": row[2],
                    "plan_days": row[3],
                    "plan_epoch": row[4],
                    "expiry_days": row[5],
                    "plan_price": row[6],
                    "category_id": row[7],
                    "vehicle_id": row[8]
                })
            
            return JsonResponse({
                "plans": plans,
                "total_pages": math.ceil(total_count / limit)
            }, status=200)
            
        except Exception as err:
            print("Error fetching recharge plans:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_recharge_plan(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            required_fields = {
                "plan_title": data.get("plan_title"),
                "plan_description": data.get("plan_description"),
                "plan_days": data.get("plan_days"),
                "expiry_days": data.get("expiry_days"),
                "plan_price": data.get("plan_price"),
                "category_id": data.get("category_id"),
                "vehicle_id": data.get("vehicle_id", 1)
            }
            
            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({"message": f"Missing fields: {', '.join(missing_fields)}"}, status=400)
            
            query = """
                INSERT INTO vtpartner.goods_driver_recharge_plans_tbl
                (plan_title, plan_description, plan_days, expiry_days, plan_price, category_id, vehicle_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            params = [
                data["plan_title"],
                data["plan_description"],
                data["plan_days"],
                data["expiry_days"],
                data["plan_price"],
                data["category_id"],
                data["vehicle_id"]
            ]
            
            row_count = insert_query(query, params)
            return JsonResponse({"message": f"{row_count} plan added successfully"}, status=200)
            
        except Exception as err:
            print("Error adding recharge plan:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_recharge_plan(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            required_fields = {
                "recharge_plan_id": data.get("recharge_plan_id"),
                "plan_title": data.get("plan_title"),
                "plan_description": data.get("plan_description"),
                "plan_days": data.get("plan_days"),
                "expiry_days": data.get("expiry_days"),
                "plan_price": data.get("plan_price"),
                "category_id": data.get("category_id"),
                "vehicle_id": data.get("vehicle_id")
            }
            
            missing_fields = check_missing_fields(required_fields)
            if missing_fields:
                return JsonResponse({"message": f"Missing fields: {', '.join(missing_fields)}"}, status=400)
            
            query = """
                UPDATE vtpartner.goods_driver_recharge_plans_tbl
                SET plan_title = %s, plan_description = %s, plan_days = %s,
                    expiry_days = %s, plan_price = %s, category_id = %s,
                    vehicle_id = %s, plan_epoch = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
                WHERE recharge_plan_id = %s
            """
            
            params = [
                data["plan_title"],
                data["plan_description"],
                data["plan_days"],
                data["expiry_days"],
                data["plan_price"],
                data["category_id"],
                data["vehicle_id"],
                data["recharge_plan_id"]
            ]
            
            row_count = update_query(query, params)
            return JsonResponse({"message": f"{row_count} plan updated successfully"}, status=200)
            
        except Exception as err:
            print("Error updating recharge plan:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_all_goods_types(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            page = data.get("page", 1)
            limit = data.get("limit", 10)
            search = data.get("search", "")
            
            offset = (page - 1) * limit
            
            query = """
                SELECT goods_type_id, goods_type_name
                FROM vtpartner.goods_type_tbl
                WHERE LOWER(goods_type_name) LIKE LOWER(%s)
                ORDER BY goods_type_id DESC
                LIMIT %s OFFSET %s
            """
            
            count_query = """
                SELECT COUNT(*) FROM vtpartner.goods_type_tbl
                WHERE LOWER(goods_type_name) LIKE LOWER(%s)
            """
            
            search_pattern = f"%{search}%"
            
            # Get total count
            count_result = select_query(count_query, [search_pattern])
            total_count = count_result[0][0] if count_result else 0
            
            # Get goods types
            result = select_query(query, [search_pattern, limit, offset])
            
            goods_types = [
                {
                    "goods_type_id": row[0],
                    "goods_type_name": row[1]
                }
                for row in result
            ]
            
            return JsonResponse({
                "goods_types": goods_types,
                "total_pages": math.ceil(total_count / limit)
            }, status=200)
            
        except Exception as err:
            print("Error fetching goods types:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_goods_type(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            goods_type_name = data.get("goods_type_name")
            
            if not goods_type_name:
                return JsonResponse({"message": "Goods type name is required"}, status=400)
            
            # Check if goods type already exists
            check_query = """
                SELECT COUNT(*) FROM vtpartner.goods_type_tbl
                WHERE LOWER(goods_type_name) = LOWER(%s)
            """
            result = select_query(check_query, [goods_type_name])
            if result[0][0] > 0:
                return JsonResponse({"message": "Goods type already exists"}, status=400)
            
            query = """
                INSERT INTO vtpartner.goods_type_tbl (goods_type_name)
                VALUES (%s)
            """
            
            row_count = insert_query(query, [goods_type_name])
            return JsonResponse({"message": "Goods type added successfully"}, status=200)
            
        except Exception as err:
            print("Error adding goods type:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_goods_type(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            goods_type_id = data.get("goods_type_id")
            goods_type_name = data.get("goods_type_name")
            
            if not all([goods_type_id, goods_type_name]):
                return JsonResponse({"message": "All fields are required"}, status=400)
            
            # Check if name already exists for different ID
            check_query = """
                SELECT COUNT(*) FROM vtpartner.goods_type_tbl
                WHERE LOWER(goods_type_name) = LOWER(%s)
                AND goods_type_id != %s
            """
            result = select_query(check_query, [goods_type_name, goods_type_id])
            if result[0][0] > 0:
                return JsonResponse({"message": "Goods type already exists"}, status=400)
            
            query = """
                UPDATE vtpartner.goods_type_tbl
                SET goods_type_name = %s
                WHERE goods_type_id = %s
            """
            
            row_count = update_query(query, [goods_type_name, goods_type_id])
            return JsonResponse({"message": "Goods type updated successfully"}, status=200)
            
        except Exception as err:
            print("Error updating goods type:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_offline_drivers(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            page = data.get("page", 1)
            limit = data.get("limit", 10)
            search = data.get("search", "")
            
            # Count query for pagination
            count_query = """
                SELECT COUNT(*)
                FROM vtpartner.goods_driverstbl gd
                WHERE gd.is_online = 0 AND gd.status = 1
                AND (
                    LOWER(gd.driver_first_name) LIKE LOWER(%s) OR
                    gd.mobile_no LIKE %s OR
                    CAST(gd.goods_driver_id AS TEXT) LIKE %s
                )
            """
            
            # Main query with all fields
            query = """
                SELECT 
                    gd.goods_driver_id, gd.driver_first_name, gd.driver_last_name, 
                    gd.profile_pic, gd.is_online, gd.ratings, gd.mobile_no, 
                    gd.registration_date, gd.time, gd.r_lat, gd.r_lng, gd.current_lat, 
                    gd.current_lng, gd.status, gd.recent_online_pic, gd.is_verified, 
                    gd.category_id, gd.vehicle_id, gd.city_id, gd.aadhar_no, 
                    gd.pan_card_no, gd.house_no, gd.city_name, gd.full_address, 
                    gd.gender, gd.owner_id, gd.aadhar_card_front, gd.aadhar_card_back, 
                    gd.pan_card_front, gd.pan_card_back, gd.license_front, 
                    gd.license_back, gd.insurance_image, gd.noc_image, 
                    gd.pollution_certificate_image, gd.rc_image, gd.vehicle_image, 
                    gd.vehicle_plate_image, gd.driving_license_no, gd.vehicle_plate_no, 
                    gd.rc_no, gd.insurance_no, gd.noc_no, gd.vehicle_fuel_type,
                    v.vehicle_name,
                    v.image as vehicle_type_image
                FROM vtpartner.goods_driverstbl gd
                LEFT JOIN vtpartner.vehiclestbl v ON gd.vehicle_id = v.vehicle_id AND gd.category_id = 1
                WHERE gd.is_online = 0 
                AND gd.status = 1
                AND (
                    LOWER(gd.driver_first_name) LIKE LOWER(%s) OR
                    gd.mobile_no LIKE %s OR
                    CAST(gd.goods_driver_id AS TEXT) LIKE %s
                )
                ORDER BY gd.goods_driver_id DESC
                OFFSET %s LIMIT %s
            """
            
            search_pattern = f"%{search}%"
            params = [search_pattern, search_pattern, search_pattern]
            
            # Get total count
            total_count_result = select_query(count_query, params)
            total_count = total_count_result[0][0] if total_count_result else 0
            
            # Calculate offset
            offset = (page - 1) * limit
            query_params = params + [offset, limit]
            
            result = select_query(query, query_params)
            
            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map results to match the same structure
            drivers = [
                {
                    "goods_driver_id": row[0],
                    "driver_first_name": row[1],
                    "driver_last_name": row[2],
                    "profile_pic": row[3],
                    "is_online": row[4],
                    "ratings": row[5],
                    "mobile_no": row[6],
                    "registration_date": row[7],
                    "time": row[8],
                    "r_lat": row[9],
                    "r_lng": row[10],
                    "current_lat": row[11],
                    "current_lng": row[12],
                    "status": row[13],
                    "recent_online_pic": row[14],
                    "is_verified": row[15],
                    "category_id": row[16],
                    "vehicle_id": row[17],
                    "city_id": row[18],
                    "aadhar_no": row[19],
                    "pan_card_no": row[20],
                    "house_no": row[21],
                    "city_name": row[22],
                    "full_address": row[23],
                    "gender": row[24],
                    "owner_id": row[25],
                    "aadhar_card_front": row[26],
                    "aadhar_card_back": row[27],
                    "pan_card_front": row[28],
                    "pan_card_back": row[29],
                    "license_front": row[30],
                    "license_back": row[31],
                    "insurance_image": row[32],
                    "noc_image": row[33],
                    "pollution_certificate_image": row[34],
                    "rc_image": row[35],
                    "driver_vehicle_image": row[36],
                    "vehicle_plate_image": row[37],
                    "driving_license_no": row[38],
                    "vehicle_plate_no": row[39],
                    "rc_no": row[40],
                    "insurance_no": row[41],
                    "noc_no": row[42],
                    "vehicle_fuel_type": row[43],
                    "vehicle_name": row[44],
                    "vehicle_image": row[45]
                }
                for row in result
            ]

            return JsonResponse({
                "drivers": drivers,
                "total_count": total_count,
                "total_pages": math.ceil(total_count / limit)
            }, status=200)
            
        except Exception as e:
            print("Error fetching offline drivers:", e)
            return JsonResponse({
                "message": "Internal Server Error"
            }, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_driver_recharge_history(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            driver_id = data.get("driver_id")
            
            if not driver_id:
                return JsonResponse({"message": "Driver ID is required"}, status=400)
            
            query = """
                SELECT 
                    rh.recharge_history_id,
                    rh.recharge_plan_id,
                    rh.driver_id,
                    rh.plan_expiry_time,
                    rh.recharge_time,
                    rh.recharge_date,
                    rh.razorpay_payment_id,
                    rp.plan_title,
                    rp.plan_description,
                    rp.plan_days,
                    rp.plan_price,
                    rp.expiry_days
                FROM vtpartner.goods_driver_recharge_history_tbl rh
                LEFT JOIN vtpartner.goods_driver_recharge_plans_tbl rp 
                ON rh.recharge_plan_id = rp.recharge_plan_id
                WHERE rh.driver_id = %s
                ORDER BY rh.recharge_time DESC
            """
            
            result = select_query(query, [driver_id])
            
            if not result:
                return JsonResponse({
                    "message": "No recharge history found",
                    "history": []
                }, status=200)
            
            history = [{
                "recharge_history_id": row[0],
                "recharge_plan_id": row[1],
                "driver_id": row[2],
                "plan_expiry_time": row[3],
                "recharge_time": row[4],
                "recharge_date": str(row[5]),  # Convert date to string
                "razorpay_payment_id": row[6],
                "plan_title": row[7],
                "plan_description": row[8],
                "plan_days": row[9],
                "plan_price": row[10],
                "expiry_days": row[11]
            } for row in result]
            
            return JsonResponse({
                "status": "success",
                "history": history
            }, status=200)
            
        except Exception as e:
            print("Error fetching recharge history:", e)
            return JsonResponse({
                "message": "Internal Server Error"
            }, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_driver_wallet_balance(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            driver_id = data.get("driver_id")
            
            if not driver_id:
                return JsonResponse({"message": "Driver ID is required"}, status=400)
            
            query = """
                SELECT 
                    w.wallet_id,
                    w.driver_id,
                    w.current_balance,
                    w.last_updated,
                    d.driver_first_name,
                    d.mobile_no
                FROM vtpartner.goods_driver_wallet w
                JOIN vtpartner.goods_driverstbl d ON w.driver_id = d.goods_driver_id
                WHERE w.driver_id = %s
            """
            
            result = select_query(query, [driver_id])
            
            if not result:
                return JsonResponse({
                    "message": "No wallet found",
                    "wallet": None
                }, status=200)
            
            wallet = {
                "wallet_id": result[0][0],
                "driver_id": result[0][1],
                "current_balance": float(result[0][2]),
                "last_updated": result[0][3],
                "driver_name": result[0][4],
                "mobile_no": result[0][5]
            }
            
            return JsonResponse({
                "status": "success",
                "wallet": wallet
            }, status=200)
            
        except Exception as e:
            print("Error fetching wallet:", e)
            return JsonResponse({
                "message": "Internal Server Error"
            }, status=500)

@csrf_exempt
def get_wallet_transactions(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            driver_id = data.get("driver_id")
            
            query = """
                SELECT 
                    transaction_id,
                    transaction_type,
                    amount,
                    status,
                    transaction_time,
                    transaction_date,
                    reference_id,
                    payment_mode,
                    remarks
                FROM vtpartner.goods_driver_wallet_transactions
                WHERE driver_id = %s
                ORDER BY transaction_time DESC
            """
            
            result = select_query(query, [driver_id])
            
            transactions = [{
                "transaction_id": row[0],
                "transaction_type": row[1],
                "amount": float(row[2]),
                "status": row[3],
                "transaction_time": row[4],
                "transaction_date": str(row[5]),
                "reference_id": row[6],
                "payment_mode": row[7],
                "remarks": row[8]
            } for row in result]
            
            return JsonResponse({
                "status": "success",
                "transactions": transactions
            }, status=200)
            
        except Exception as e:
            print("Error fetching transactions:", e)
            return JsonResponse({
                "message": "Internal Server Error"
            }, status=500)

@csrf_exempt
def add_wallet_transaction(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            driver_id = data.get("driver_id")
            amount = data.get("amount")
            transaction_type = data.get("transaction_type")
            payment_mode = data.get("payment_mode")
            reference_id = data.get("reference_id", "NA")
            remarks = data.get("remarks", "NA")
            
            # Get or create wallet
            wallet_query = """
                INSERT INTO vtpartner.goods_driver_wallet (driver_id)
                VALUES (%s)
                ON CONFLICT (driver_id) DO NOTHING
                RETURNING wallet_id
            """
            wallet_result = insert_query(wallet_query, [driver_id])
            
            if not wallet_result:
                wallet_query = "SELECT wallet_id FROM vtpartner.goods_driver_wallet WHERE driver_id = %s"
                wallet_result = select_query(wallet_query, [driver_id])
            
            wallet_id = wallet_result[0][0]
            
            # Add transaction
            transaction_query = """
                INSERT INTO vtpartner.goods_driver_wallet_transactions
                (wallet_id, driver_id, transaction_type, amount, status, payment_mode, reference_id, remarks)
                VALUES (%s, %s, %s, %s, 'COMPLETED', %s, %s, %s)
            """
            insert_query(transaction_query, [
                wallet_id, driver_id, transaction_type, amount, 
                payment_mode, reference_id, remarks
            ])
            
            # Update wallet balance
            update_wallet_query = """
                UPDATE vtpartner.goods_driver_wallet
                SET current_balance = CASE
                    WHEN %s = 'DEPOSIT' THEN current_balance + %s
                    WHEN %s = 'WITHDRAWAL' THEN current_balance - %s
                    ELSE current_balance
                END,
                last_updated = extract(epoch from CURRENT_TIMESTAMP)
                WHERE wallet_id = %s
            """
            update_query(update_wallet_query, [
                transaction_type, amount,
                transaction_type, amount,
                wallet_id
            ])
        
            return JsonResponse({
                "status": "success",
                "message": "Transaction completed successfully"
            }, status=200)
            
        except Exception as e:
            print("Error adding transaction:", e)
            return JsonResponse({
                "message": "Internal Server Error"
            }, status=500)
    
    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_customer_wallet_balance(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            customer_id = data.get("customer_id")
            
            query = """
                SELECT 
                    w.wallet_id,
                    w.customer_id,
                    w.current_balance,
                    w.last_updated,
                    c.customer_name,
                    c.mobile_no,
                    c.email
                FROM vtpartner.customer_wallet w
                JOIN vtpartner.customers_tbl c ON w.customer_id = c.customer_id
                WHERE w.customer_id = %s
            """
            
            result = select_query(query, [customer_id])
            
            if not result:
                return JsonResponse({
                    "message": "No wallet found",
                    "wallet": None
                }, status=200)
            
            wallet = {
                "wallet_id": result[0][0],
                "customer_id": result[0][1],
                "current_balance": float(result[0][2]),
                "last_updated": result[0][3],
                "customer_name": result[0][4],
                "mobile_no": result[0][5],
                "email": result[0][6]
            }
            
            return JsonResponse({
                "status": "success",
                "wallet": wallet
            }, status=200)
            
        except Exception as e:
            print("Error:", e)
            return JsonResponse({
                "message": "Internal Server Error"
            }, status=500)
            
@csrf_exempt
def create_razorpay_order(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            amount = data.get("amount")  # amount in paise
            
            # Just create and return the order details
            return JsonResponse({
                'status': 'success',
                'order_id': 'order_' + str(int(time.time())),  # Generate your own order ID
                'amount': amount
            })
            
        except Exception as e:
            print("Error:", e)
            return JsonResponse({
                'status': 'error',
                'message': str(e)
            }, status=500)