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
import random
from django.middleware.csrf import get_token
from django.views.decorators.csrf import csrf_exempt
from django.db.utils import IntegrityError
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from google.oauth2 import service_account
import google.auth.transport.requests
import os
from dotenv import load_dotenv

from PIL import Image  # Pillow library for image processing

# Utility function to check for missing fields
# def check_missing_fields(fields):
#     missing_fields = [field for field, value in fields.items() if not value]
#     print("missing_fields::",missing_fields)
#     return missing_fields if missing_fields else None
mapKey = "AIzaSyD-vFDMqcEcgyeppWvGrAuhVymvF0Dxue0"

def check_missing_fields(fields):
    # Only consider a field missing if its value is None
    missing_fields = [field for field, value in fields.items() if value is None]
    print("missing_fields::", missing_fields)
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
            print("result::",result)
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
    
def insert_query2(query, params=None):
    if params is None:
        params = ()  # Default to empty tuple if no params are passed
    
    print("Executing insert query:", query)
    print("With parameters:", params)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            
            # If the query has a RETURNING clause, fetch the returned rows
            if cursor.description:
                result = cursor.fetchall()  # Fetch all returned rows if any
                connection.commit()  # Commit after insertion
                return result
            else:
                connection.commit()  # Commit if only affecting rows
                return cursor.rowcount  # Return number of affected rows
    
    except IntegrityError as e:
        print("Integrity Error: Failed to insert data due to integrity error", e)
        raise
    except Exception as e:
        print("General Error executing query:", e)
        raise


def update_query(query, params):
    print("update query::",query)
    print("update query params::",params)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.rowcount

def delete_query(query, params):
    print("delete query::",query)
    print("delete query params::",params)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.rowcount

def insert_query(query, params):
    print("Executing insert query:", query)
    print("With parameters:", params)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            
            # If the query has a RETURNING clause, fetch the returned rows
            if cursor.description:
                result = cursor.fetchall()  # Fetch all returned rows if any
                connection.commit()  # Commit after insertion
                return result
            else:
                connection.commit()  # Commit if only affecting rows
                return cursor.rowcount  # Return number of affected rows
    
    except IntegrityError as e:
        print("Integrity Error: Failed to insert data due to integrity error", e)
        raise
    except Exception as e:
        print("General Error executing query:", e)
        raise

@csrf_exempt
def get_server_key_token():
    # Define the required scopes
    scopes = ["https://www.googleapis.com/auth/firebase.messaging"]
    
    # Load the service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        {
          "type": "service_account",
          "project_id": "vt-partner-8317b",
          "private_key_id": "837e02078949fb8be96ff2b9d6e09dfb779a8efd",
          "private_key":
              "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDH7eZNh/qpGcEo\nK+pcufsqIMDpVUuKKAoNgVdSNpt8KKtKsgt7R/fnCtBaqc8E0e9iJWKFIEtcKPhi\n/sMga5mUJTjwm/1YigSVe620OaUSN7NySGaz7St10A4rx4IAvYkgtZ/Mxr3BL3TA\n+gvl09F3UkICC8AOpLmxnTTOu0wEay/Ms0sVITzdDWBQ2/Q6hzEa2KdbXxqQUgLJ\nkvcDkGzdJhmjSJhXZCUDNXzz4+dkO6x5GSbNt0/vzasvJoRt5LCu1k0X0NNaBGs/\n1/Lzh0excl6lAlZKHDK+vXaX50wEKtizNXBdOBc6XSwUGSeQ6iQbB20/SMOWJ9Ki\nPnZ6XiHlAgMBAAECggEALCWwpqshowiAWgYEfNBGkWFlJ8EBarL3sU6/wPQ09kAm\nvto85c6ZA6gkJPj9MSPIV+RIcnwUl/emDXoTDUwlQAzOG3dehJgJdha23yaheDnb\ngp9RKmbzI1M7ZdhqsQ4pQxNIA5hZG1kGz3wHd4sD5HTCBaChmrouFPXRTNsX6Jt8\nhs5c45ewvXYVem3DD2WY3cV3+B5VF7v3gniU2n0+6AkOO7io6pLVPKXqv1rdeQ/8\nZHkbCmA6EsgXWcWwgvnErIh4ahiDehJFa6ZYqzMXNG1cectTONIfHuQxKWIEFMbS\nvu1blrG49Yf9sDEPGRudThhxziM+jSru4CrSeYUU4QKBgQDuqNRgXU3GWyd0ZRFl\nfRNEYrsyActv7jn83vVVT7rxaD0uEgcKUXaK8KjAKfCWyxErWYEjsBao64LLaqOx\nQ5DaxFE9TEmtvFrArQueMBZh5YzWLL2LuLVxfnuJekFe9PES7CrCaZseB2jA+cfZ\nOk7tD8OXZPOL58b2BZ7HV8xcUQKBgQDWdKvpzs3Jk6KqXKYZSzDSc7O/dduSic3z\ndUbZD9itWJ/SEkDGI+9JJnB6yGsUK7XSg/v4DQ5iTxLkVYPIsKInugv5l83F0sW9\nidejYHGJQpPH3ihPaHOyNJppDx8e1XO3MA5k5CfPa3USzNuZ0X6cprzKxwqG+BMv\nONlXi8wLVQKBgDFNuYoq3F1lCXKXSo+/1hIjn26GRmPaQCqIWQCF1yX2FeWFneS4\nzZeIfiQsxeIxE1v0QqR/xT6iYMPrROPjBHLdabcTIol8xvbVCPhmEMmqpXy9g27w\n+rL2oUjWc9jNG1yAY5kEPiJm/3IWZ/3teM6qmgqVtWaqvESpBpNCBRrxAoGBALyo\nNJfhks0ysFW4XXJA4DkzCbxzfO7DhccXs3S+aPnNzgLqhcsIz7cFNsv8xZ4f+bqw\n2xdSvQWk1FTEGcOSB4R1OZWfgqj1i3j66xVRgW+jfwfDmqiIlcb+WZv0boccdciA\nYRlGUPM6b+pTBxig4AYE9G5afRtQ3uea1jAazixlAoGBAIpka81kFHels4njhIYO\nV04+2CC/bq81pn8puUZNt+xuR0yLLoEPSYglP5Oohc1Eu0FZZYBaIScN9pgrzEvj\nDABU0sf1MQDaRGkcrgPyrVh94RpalkOpdfVnYmOq9iZZ8gI/0DKYemCKTOHxXZgR\nLSP+No/oYiXtfsXoHWkOMRKq\n-----END PRIVATE KEY-----\n",
          "client_email":
              "latest-vt-partner-final@vt-partner-8317b.iam.gserviceaccount.com",
          "client_id": "113906949943313096106",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url":
              "https://www.googleapis.com/robot/v1/metadata/x509/latest-vt-partner-final%40vt-partner-8317b.iam.gserviceaccount.com",
          "universe_domain": "googleapis.com"
        },  # Replace with your actual service account dictionary or JSON file
        scopes=scopes
    )

    # Use a request object for the credentials to refresh and generate an access token
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)

    # Return the access token
    return credentials.token

def sendFMCMsg(deviceToken, msg, title, data,serverToken):
    
    deviceToken = deviceToken.replace('__colon__', ':')
    print(f"deviceToken::{deviceToken}")
    print(f"serverKey::{serverToken}")
    # Validate the device token
    if not deviceToken:
        print("Invalid device token")
        return

    # Check if the token has already been sent a notification
    # (You may want to implement a more robust solution to track notifications)

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {serverToken}',
    }

    body =  {
        "message": {
            "token": deviceToken,
            "notification": {
                "body": msg,
                "title": title
            },
            "data": data
        }
    }

    try:
        response = requests.post("https://fcm.googleapis.com/v1/projects/vt-partner-8317b/messages:send", headers=headers, data=json.dumps(body))
        response_data = response.json()
        print("FCM Response:")
        print(response_data)
        print("Status Code:", response.status_code)
    except requests.exceptions.RequestException as e:
        print("Error sending FCM notification:", e)
        
def sendBulkFMCMsg(deviceTokens, msg, title, data, serverToken):
    # Validate the device tokens
    deviceTokens = [token.replace('__colon__', ':') for token in deviceTokens if token]

    if not deviceTokens:
        print("No valid device tokens provided")
        return

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'key={serverToken}',  # Legacy FCM server key
    }

    body = {
        "registration_ids": deviceTokens,
        "notification": {
            "body": msg,
            "title": title,
        },
        "data": data
    }

    try:
        response = requests.post("https://fcm.googleapis.com/fcm/send", headers=headers, json=body)
        response_data = response.json()
        print("FCM Response:", response_data)
        print("Status Code:", response.status_code)
    except requests.exceptions.RequestException as e:
        print("Error sending FCM bulk notification:", e)


@csrf_exempt
def send_notification_using_api(token: str, title: str, body: str, data: dict) -> None:
    # Retrieve the server key for authorization
    server_key = get_server_key_token()  # Replace with your actual method to get the server key
    print("server_key::",server_key)
    url = "https://fcm.googleapis.com/v1/projects/vt-partner-8317b/messages:send"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {server_key}',
    }

    # Construct the message payload
    message = {
        "message": {
            "token": token,
            "notification": {
                "body": body,
                "title": title
            },
            "data": data
        }
    }

    # Send the notification request
    response = requests.post(url, headers=headers, data=json.dumps(message))

    # Check the response status
    if response.status_code == 200:
        print("Notification sent successfully.")
    else:
        print(f'Notification failed to send: {response.status_code} - {response.text}')

@csrf_exempt
def upload_image(request):
    if request.method == 'POST':
        file = request.FILES['image']  # Assuming the file is named 'image' in the form data
        fs = FileSystemStorage()
        filename = fs.save(file.name, file)
        file_url = fs.url(filename)
        print("file_url::",file_url)
        return JsonResponse({'image_url': file_url})
    else:
        return JsonResponse({'error': 'Invalid request'})

# Create your views here.

@csrf_exempt
def send_otp(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            mobile_no = data.get("mobile_no")

            # Validate mobile_no
            if not mobile_no:
                return JsonResponse({"message": "Mobile number is required."}, status=400)

            # Remove +91 prefix if present and ensure it's exactly 10 digits
            mobile_no = re.sub(r"^\+91", "", mobile_no)  # Remove +91
            if len(mobile_no) > 10:
                mobile_no = mobile_no[-10:]  # Keep the last 10 digits

            if len(mobile_no) != 10 or not mobile_no.isdigit():
                return JsonResponse({"message": "Invalid mobile number."}, status=400)

            # Generate a random 6-digit OTP
            otp = random.randint(100000, 999999)

            # OTP message template
            otp_message = f"VTPartner Your OTP is {otp}. Please use this code to complete your verification. Do not share this OTP with anyone."
            # otp_message = f"<#> Your VT Partner App verification code is {otp}. Please use this code to complete your verification. Do not share this OTP with anyone. Thank you."

            # SMS API endpoint and parameters
            sms_api_url = "http://smsozone.com/api/mt/SendSMS"
            sms_params = {
                "APIKey": "qkUTqWb5NU66z5StCDcrIA",
                "senderid": "VTPART",
                "channel": "Trans",
                "DCS": 0,
                "flashsms": 0,
                "number": mobile_no,
                "text": otp_message,
                "route": 2069,
            }

            # Send the OTP (You might use a library like `requests` to make the API call)
            response = requests.get(sms_api_url, params=sms_params)

            if response.status_code == 200:
                # Success: Return the OTP for testing purposes (Remove in production)
                return JsonResponse({"message": "OTP sent successfully.", "otp": otp}, status=200)
            else:
                # Failure: Handle SMS API error
                return JsonResponse({"error_message": "Failed to send OTP.", "details": response.text}, status=500)

        except Exception as e:
            print("Error:", e)
            return JsonResponse({"error_message": "An error occurred while processing the request."}, status=500)

    return JsonResponse({"error_message": "Method not allowed."}, status=405)


@csrf_exempt
def login_view(request):
    if request.method == "POST":
        data = json.loads(request.body)
        mobile_no = data.get("mobile_no")

         # List of required fields
        required_fields = {
            "mobile_no": mobile_no,
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
        map_data = {
            'intent':'driver',
            'booking_id':'4'
        }
        # sendFMCMsg('dsTW2AoxW0uwrrmSJprFLV:APA91bGjCwFLxNyf2juIY4_sNZV2nLLpOGrwdoYQxnKY5mi4gidNVDbH12QQmZWkIur_7_CFg23QSAJVqYm8fQfc6p0hYZfe65JswAOBYgeWHKE0s6OMrVY','Body','Title',map_data,'ya29.c.c0ASRK0Ga2laM4m15OH5iuGcfjYKOy8PyYN6kUB9p_vtbzriqD6D84fCfpJzUwy_EzqxesTT4h0oUSNAG5hS18GbIJtCtXqnPGS-KP7-l7f3XWZCW9KTnDl5C-Ia9zvZBCn-VkX5mA0_OduO7FYRIQWSuN18dWlPgRtdvMvVxKeMTUg49j9Ayi0RJh6ZBBz54gByigMsWuep3zt4Ld_sKVyXc_SDpOmjPv96GQJ7v0OiDC7rh-QLYT8pSsvBD3-_33CWrWGHw2yWoKx5SiskNppQ3c2nb5nQZpzP9ws4td7FHhQfgB987ylKx8oZFd0ynWmIJhYUkqX73UEftYZ7IhhxKckULvQLoLN_GZA3KIr25Kvoe5Gr370q_5E385Asec9S1ikF1F-lM4xVlghy_o71vwguv-u9Q57Zj4ktUt4_1xooOB7wh4O3SowdomIVYWt_amyaamZ3Ym0qywRmmsVwXJf_a4_3wmuWROzcs-n-aVU9yvcOBB4o4RjMqgdc3ZSX9UMdjyZ2JUm0I7revSn5kRU2maWcf2wOY76z_U3x2zdQw-wlb2lrssicBWt0xsUg8Yz5cB51FIahkgcYkMqsvW0dkIXww-aV5t-Jq1iMkyfgXXpgm47-stnJi0zxmiqMg_MBvF5tiU54Y07n7X26Rbrs7370s-tjlrehn0s0m-9ccweOextjRsmlsVrho0oBSO4neaYXkmuux1Xgo2irSa2Iq4aQOve0kpjhtWca-cfoROn5Ymk690pqjJqfrB56Mb-BssS4WBOwX1dXU5pI0x-0cy790-xuiXe7sB7Zz8dkJ7QI1v1Xdgk78rhBh48mdMbck0MM5do-JF_q_RcmQQyl5xwFlw1QtJ5Fx14vSlwJdacxScRfMsvzWlnM11fXrmmbwu6eU4YFVe6aFqwZJJWruyfg2W8lVyaktdp3kownUjQI1Rvj_eghQXJxaFZc5tgW4rW4r1-o9oru0d39Obnx8cw8bIb0kvSSml_ny9tVyVc0IYSpR')
        try:
            query = """
            SELECT customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email,gst_no,gst_address,pincode FROM
            vtpartner.customers_tbl WHERE mobile_no=%s
            """
            params = [mobile_no]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                try:
                    #Insert if not found
                    query = """
                        INSERT INTO vtpartner.customers_tbl (
                            mobile_no
                        ) VALUES (%s) RETURNING customer_id
                    """
                    values = [mobile_no]
                    new_result = insert_query(query, values)
                    print("new_result::",new_result)
                    if new_result:
                        print("new_result[0][0]::",new_result[0][0])
                        customer_id = new_result[0][0]
                        response_value = [
                            {
                                "customer_id":customer_id
                            }
                        ]
                        return JsonResponse({"result": response_value}, status=200)
                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "customer_id": row[0],
                    "customer_name": row[1],
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
                    "full_address": row[13],
                    "email": row[14],
                    "gst_no": row[15],
                    "gst_address": row[16],
                    "pincode": row[17],
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def customer_registration(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        customer_id = data.get("customer_id")
        customer_name = data.get("customer_name")
        r_lat = data.get("r_lat")
        r_lng = data.get("r_lng")
        full_address = data.get("full_address")
        purpose = data.get("purpose")
        email = data.get("email")
        pincode = data.get("pincode")
        
        
        
        # List of required fields
        required_fields = {
            "customer_id":customer_id,
            "customer_name":customer_name,
            "r_lat":r_lat,
            "r_lng":r_lng,
            "full_address":full_address,
            "purpose":purpose,
            "email":email,
            "pincode":pincode,
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
            UPDATE vtpartner.customers_tbl 
            SET customer_name=%s, r_lat=%s, r_lng=%s, current_lat=%s, current_lng=%s, full_address=%s, purpose=%s, email=%s, pincode=%s
            WHERE customer_id=%s
        """
        values = [
            customer_name ,
            r_lat ,
            r_lng ,
            r_lat ,
            r_lng ,
            full_address ,
            purpose ,
            email ,
            pincode ,
            customer_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing add new faq query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)

@csrf_exempt 
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
                    category_id ASC
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
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def all_cities(request):
    if request.method == "POST":
        try:
            query = """
                SELECT 
                city_id, 
                city_name, 
                pincode, 
                bg_image
            FROM 
                vtpartner.available_citys_tbl 
            WHERE status ='1'
            ORDER BY 
                city_name ASC
            """
            result = select_query(query)  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            city_details = [
                {
                    "city_id": row[0],
                    "city_name": row[1],
                    "pincode": row[2],
                    "bg_image": row[3],
                }
                for row in result
            ]

            return JsonResponse({"results": city_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
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
                ORDER BY vehicle_name ASC
            """
            result = select_query(query,[category_id])  # Assuming select_query is defined elsewhere

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

            return JsonResponse({"results": vehicle_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def all_vehicles_with_price_details(request):
    if request.method == "POST":
        try:
            body = json.loads(request.body)
            category_id = body.get("category_id")
            price_type_id = body.get("price_type_id")
            city_id = body.get("city_id")
            # List of required fields
            required_fields = {
                "category_id": category_id,
                "price_type_id": price_type_id,
                "city_id": city_id,
            }

            # Check for missing fields
            missing_fields = check_missing_fields(required_fields)  # Assuming check_missing_fields is defined

            # If there are missing fields, return an error response
            if missing_fields:
                return JsonResponse({
                    "message": f"Missing required fields: {', '.join(missing_fields)}"
                }, status=400)

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
                vcwp.starting_price_per_km,
                vcwp.base_fare,
                vcwp.minimum_time
                FROM
                vtpartner.vehiclestbl v
                JOIN
                vtpartner.vehicle_types_tbl vt
                ON v.vehicle_type_id = vt.vehicle_type_id
                JOIN
                vtpartner.vehicle_city_wise_price_tbl vcwp
                ON v.vehicle_id = vcwp.vehicle_id
                WHERE
                v.category_id = %s
                AND vcwp.price_type_id = %s
                AND vcwp.city_id = %s
                ORDER BY
                v.vehicle_id
            """
            result = select_query(query,[category_id,price_type_id,city_id])  # Assuming select_query is defined elsewhere

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
                    "starting_price_per_km": row[8],
                    "base_fare": row[9],
                    "minimum_time": row[10],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": vehicle_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def allowed_pin_code(request):
    if request.method == "POST":
        data = json.loads(request.body)
        pincode = data.get("pincode")

         # List of required fields
        required_fields = {
            "pincode": pincode,
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
                
                
        try:
            query = """
            select city_id from vtpartner.allowed_pincodes_tbl where pincode=%s and status='1'
            """
            params = [pincode]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "city_id": row[0]
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


def get_goods_driver_auth_token( goods_driver_id):
    auth_token = ""
    try:
        query = """
        select authtoken from vtpartner.goods_driverstbl where goods_driver_id=%s 
        """
        params = [goods_driver_id]
        result = select_query(query, params)  # Assuming select_query is defined elsewhere

        if not result:
            return JsonResponse({"message": "No Data Found"}, status=404)

        # Extract auth_token from the result
        auth_token = result[0][0]  # Get the first row, first column (auth token)
        
        return auth_token

    except Exception as err:
        print("Error in finding the auth token for goods driver:", err)
        return auth_token

def get_goods_driver_auth_token2(goods_driver_id):
    auth_token = ""
    try:
        query = """
        SELECT authtoken FROM vtpartner.goods_driverstbl WHERE goods_driver_id=%s
        """
        params = [goods_driver_id]
        result = select_query(query, params)  # Assuming select_query is defined elsewhere

        if not result:
            print(f"No auth token found for goods_driver_id: {goods_driver_id}")
            return None  # Return None instead of empty string for clarity

        # Extract auth_token from the result
        auth_token = result[0][0]  # Get the first row, first column (auth token)
        
        return auth_token

    except Exception as err:
        print("Error in finding the auth token for goods driver:", err)
        return None

def get_cab_driver_auth_token2(cab_driver_id):
    auth_token = ""
    try:
        query = """
        SELECT authtoken FROM vtpartner.cab_driverstbl WHERE cab_driver_id=%s
        """
        params = [cab_driver_id]
        result = select_query(query, params)  # Assuming select_query is defined elsewhere

        if not result:
            print(f"No auth token found for cab_driver_id: {cab_driver_id}")
            return None  # Return None instead of empty string for clarity

        # Extract auth_token from the result
        auth_token = result[0][0]  # Get the first row, first column (auth token)
        
        return auth_token

    except Exception as err:
        print("Error in finding the auth token for cab driver:", err)
        return None

#New Booking 
@csrf_exempt
def new_goods_delivery_booking(request):
    if request.method == "POST":
        data = json.loads(request.body)
        
        # Read the individual fields from the JSON data
        customer_id = data.get("customer_id")
        driver_id = data.get("driver_id")
        pickup_lat = data.get("pickup_lat")
        pickup_lng = data.get("pickup_lng")
        destination_lat = data.get("destination_lat")
        destination_lng = data.get("destination_lng")
        distance = data.get("distance")
        time = data.get("time")
        total_price = data.get("total_price")
        base_price = data.get("base_price")
        otp = random.randint(1000, 9999)  # Generate a random 4-digit OTP
        gst_amount = data.get("gst_amount")
        igst_amount = data.get("igst_amount")
        goods_type_id = data.get("goods_type_id")
        payment_method = data.get("payment_method")
        city_id = data.get("city_id")
        sender_name = data.get("sender_name")
        sender_number = data.get("sender_number")
        receiver_name = data.get("receiver_name")
        receiver_number = data.get("receiver_number")
        pickup_address = data.get("pickup_address")
        drop_address = data.get("drop_address")
        server_access_token = data.get("server_access_token")

        # List of required fields
        required_fields = {
            "customer_id":customer_id,
            "driver_id":driver_id,
            "pickup_lat":pickup_lat,
            "pickup_lng":pickup_lng,
            "destination_lat":destination_lat,
            "destination_lng":destination_lng,
            "distance":distance,
            "time":time,
            "total_price":total_price,
            "base_price":base_price,
            "otp":str(otp),
            "gst_amount":gst_amount,
            "igst_amount":igst_amount,
            "goods_type_id":goods_type_id,
            "payment_method":payment_method,
            "city_id":city_id,
            "sender_name":sender_name,
            "sender_number":sender_number,
            "receiver_name":receiver_name,
            "receiver_number":receiver_number,
            "pickup_address":pickup_address,
            "drop_address":drop_address,
            "server_access_token":server_access_token,
        }

        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        driver_auth_token = get_goods_driver_auth_token(driver_id)

        
        
        try:
            # Insert record in the booking table
            query_insert = """
                INSERT INTO vtpartner.bookings_tbl (
                    customer_id, driver_id, pickup_lat, pickup_lng, destination_lat, destination_lng, 
                    distance, time, total_price, base_price, booking_timing, booking_date, 
                    otp, gst_amount, igst_amount, 
                    payment_method, city_id,sender_name,sender_number,receiver_name,receiver_number,pickup_address,drop_address
                ) 
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE,  %s, %s, %s, 
                    %s, %s,%s, %s,%s, %s,%s, %s
                ) 
                RETURNING booking_id;
            """

            insert_values = [
                customer_id, '-1', pickup_lat, pickup_lng, destination_lat, destination_lng, 
                distance, time, total_price, base_price, otp, 
                gst_amount, igst_amount, payment_method, city_id,sender_name,sender_number,receiver_name,receiver_number,pickup_address,drop_address
            ]

            # Assuming insert_query is a function that runs the query
            new_result = insert_query(query_insert, insert_values)
            
            if new_result:
                booking_id = new_result[0][0]  # Extracting booking_id from the result
                response_value = [{"booking_id": booking_id}]
                
                #send notification to goods driver
                fcm_data = {
                    'intent':'driver',
                    'booking_id':str(booking_id)
                }
                sendFMCMsg(driver_auth_token,f'You have a new Ride Request for \nPickup Location : {pickup_address}. \n Drop Location : {drop_address}','New Ride Request',fcm_data,server_access_token)

                # Send success response
                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def search_nearby_drivers(request):
    if request.method == "POST":
        data = json.loads(request.body)
        lat = data.get("lat")
        lng = data.get("lng")
        city_id = data.get("city_id")
        vehicle_id = data.get("vehicle_id")
        radius_km = data.get("radius_km", 5)  # Radius in kilometers

        if lat is None or lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
            # Haversine formula to calculate the distance in kilometers
            #SELECT main.active_id, main.goods_driver_id, main.current_lat, main.current_lng, 
#        main.entry_time, main.current_status, goods_driverstbl.driver_first_name,
#        goods_driverstbl.profile_pic, vehiclestbl.image AS vehicle_image, 
#        vehiclestbl.vehicle_name,weight,
#        (6371 * acos(
#            cos(radians(%s)) * cos(radians(main.current_lat)) *
#            cos(radians(main.current_lng) - radians(%s)) +
#            sin(radians(%s)) * sin(radians(main.current_lat))
#        )) AS distance
# FROM vtpartner.active_goods_drivertbl AS main
# INNER JOIN (
#     SELECT goods_driver_id, MAX(entry_time) AS max_entry_time
#     FROM vtpartner.active_goods_drivertbl
#     GROUP BY goods_driver_id
# ) AS latest ON main.goods_driver_id = latest.goods_driver_id
#               AND main.entry_time = latest.max_entry_time
# JOIN vtpartner.goods_driverstbl ON main.goods_driver_id = goods_driverstbl.goods_driver_id
# JOIN vtpartner.vehiclestbl ON goods_driverstbl.vehicle_id = vehiclestbl.vehicle_id
# WHERE main.current_status = 1
#   AND (6371 * acos(
#          cos(radians(%s)) * cos(radians(main.current_lat)) *
#          cos(radians(main.current_lng) - radians(%s)) +
#          sin(radians(%s)) * sin(radians(main.current_lat))
#      )) <= %s
#   AND goods_driverstbl.category_id = vehiclestbl.category_id
#   AND goods_driverstbl.category_id = '1'
# ORDER BY distance;
#             """
#             values = [lat, lng, lat, lat, lng, lat, radius_km]

            query = """
            SELECT 
    main.active_id, 
    main.goods_driver_id, 
    main.current_lat, 
    main.current_lng, 
    main.entry_time, 
    main.current_status, 
    goods_driverstbl.driver_first_name,
    goods_driverstbl.profile_pic, 
    vehiclestbl.image AS vehicle_image, 
    vehiclestbl.vehicle_name,
    vehiclestbl.weight,
    vehicle_city_wise_price_tbl.starting_price_per_km,
    vehicle_city_wise_price_tbl.base_fare,
    vehiclestbl.vehicle_id,
    (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
    )) AS distance
FROM vtpartner.active_goods_drivertbl AS main
INNER JOIN (
    SELECT goods_driver_id, MAX(entry_time) AS max_entry_time
    FROM vtpartner.active_goods_drivertbl
    GROUP BY goods_driver_id
) AS latest ON main.goods_driver_id = latest.goods_driver_id
             AND main.entry_time = latest.max_entry_time
JOIN vtpartner.goods_driverstbl ON main.goods_driver_id = goods_driverstbl.goods_driver_id
JOIN vtpartner.vehiclestbl ON goods_driverstbl.vehicle_id = vehiclestbl.vehicle_id
JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
AND vehicle_city_wise_price_tbl.city_id = %s  AND goods_driverstbl.vehicle_id=%s
WHERE main.current_status = 1
  AND (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
      )) <= %s
  AND goods_driverstbl.category_id = vehiclestbl.category_id
  AND goods_driverstbl.category_id = '1'
ORDER BY distance;

            """
            values = [lat, lng, lat,city_id,vehicle_id, lat, lng, lat, radius_km]

            # Execute the query
            nearby_drivers = select_query(query, values)

            # Format response
            drivers_list = [
                {
                    "active_id": driver[0],
                    "goods_driver_id": driver[1],
                    "latitude": driver[2],
                    "longitude": driver[3],
                    "entry_time": driver[4],
                    "current_status": driver[5],
                    "driver_name": driver[6],
                    "driver_profile_pic": driver[7],
                    "vehicle_image": driver[8],
                    "vehicle_name": driver[9],
                    "weight": driver[10],
                    "starting_price_per_km": driver[11],
                    "base_fare": driver[12],
                    "vehicle_id": driver[13],
                    "distance": driver[14]
                }
                for driver in nearby_drivers
            ]

            return JsonResponse({"nearby_drivers": drivers_list}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_firebase_customer_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        customer_id = data.get("customer_id")
        authToken = data.get("authToken")
        

        # List of required fields
        required_fields = {
            "customer_id": customer_id,
            "authToken": authToken,
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
                UPDATE vtpartner.customers_tbl 
                SET 
                    authtoken = %s
                WHERE customer_id = %s
                """
            values = [
                    authToken,
                    customer_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def booking_details_live_track(request):
    if request.method == "POST":
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
            
        try:
            query = """
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,vehicle_plate_no,vehicle_fuel_type,goods_driverstbl.profile_pic from vtpartner.vehiclestbl,vtpartner.bookings_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=bookings_tbl.driver_id and customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s and booking_status!='End Trip' and vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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

                    
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def goods_order_details(request):
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
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,vehicle_plate_no,vehicle_fuel_type,goods_driverstbl.profile_pic,orders_tbl.ratings,pickup_time,drop_time from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and order_id=%s and vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id
            """
            result = select_query(query,[order_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "ratings": str(row[40]),
                    "pickup_time": str(row[41]),
                    "drop_time": str(row[42]),

                    
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def cancel_booking(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        customer_id = data.get("customer_id")
        driver_id = data.get("driver_id")
        server_token = data.get("server_token")
        pickup_address = data.get("pickup_address")
        cancel_reason = data.get("cancel_reason")
        
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "server_token": server_token,
            "customer_id": customer_id,
            "driver_id": driver_id,
            "cancel_reason": cancel_reason,
        
        
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
                UPDATE vtpartner.bookings_tbl set booking_status='Cancelled',cancelled_reason=%s WHERE booking_id=%s
            """
            row_count = update_query(query,[cancel_reason,booking_id])  
            
            #Updating it in Booking History Table to maintain record at what time it was cancelled
            query2 = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
            values2 = [
                    booking_id,
                    'Cancelled'
                ]

            # Execute the query
            row_count = insert_query(query2, values2)
            
            query3 = """
            update vtpartner.active_goods_drivertbl set current_status='1' where goods_driver_id=%s
            """
            values3 = [
                    driver_id
            ]
            # Execute the query
            row_count = update_query(query3, values3)
            #Send Notitification to customer and driver
            customer_auth_token = get_customer_auth_token(customer_id)
            driver_auth_token = get_goods_driver_auth_token(driver_id)
            
            #send notification to goods driver for booking cancelled
            fcm_data = {
                'intent':'driver_home',
                'booking_id':str(booking_id)
            }
            sendFMCMsg(
            driver_auth_token,
            f'The ride request has been canceled by the customer. \nPickup Location: {pickup_address}.',
            f'Ride Canceled - [Booking ID: {str(booking_id)}]',
            fcm_data,
            server_token
            )

            
            #send notification to Customer for booking cancellation confirmation.
            fcm_data2 = {
                'intent':'customer_home',
                'booking_id':str(booking_id)
            }
            sendFMCMsg(
                customer_auth_token,
                f'Your ride request has been successfully canceled. \nPickup Location: {pickup_address}.',
                'Ride Cancellation Confirmation',
                fcm_data2,
                server_token
            )

            
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def save_order_ratings(request):
    if request.method == "POST":
        data = json.loads(request.body)
        ratings = data.get("ratings")
        ratings_description = data.get("ratings_description")
        order_id = data.get("order_id")
        
        

        # List of required fields
        required_fields = {
            "ratings": ratings,
            "ratings_description": ratings_description,
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
                UPDATE vtpartner.orders_tbl set ratings=%s,rating_description=%s WHERE order_id=%s
            """
            row_count = update_query(query,[ratings,ratings_description,order_id])  
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def customers_all_bookings(request):
    if request.method == "POST":
        data = json.loads(request.body)
        customer_id = data.get("customer_id")
        
        

        # List of required fields
        required_fields = {
            "customer_id": customer_id,
        
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
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image from vtpartner.vehiclestbl,vtpartner.bookings_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=bookings_tbl.driver_id and customers_tbl.customer_id=bookings_tbl.customer_id and bookings_tbl.customer_id=%s and booking_completed='-1' and vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id order by booking_id desc;
            """
            result = select_query(query,[customer_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def customers_all_orders(request):
    if request.method == "POST":
        data = json.loads(request.body)
        customer_id = data.get("customer_id")
        
        

        # List of required fields
        required_fields = {
            "customer_id": customer_id,
        
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
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and orders_tbl.customer_id=%s and  vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id order by order_id desc
            """
            result = select_query(query,[customer_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)


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
def get_all_sub_categories(request):
    if request.method == "POST": 
        
        data = json.loads(request.body)
        cat_id = data.get("cat_id")

         # List of required fields
        required_fields = {
            "cat_id": cat_id,
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
                   
        try:
            query = """
            select sub_cat_id,sub_cat_name,image from vtpartner.sub_categorytbl where cat_id=%s order by sub_cat_name asc
            """
            
            result = select_query(query,[cat_id])

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "sub_cat_id": row[0],
                    "sub_cat_name": row[1],
                    "image": row[2]
                    
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_all_sub_services(request):
    if request.method == "POST": 
        data = json.loads(request.body)
        sub_cat_id = data.get("sub_cat_id")

         # List of required fields
        required_fields = {
            "sub_cat_id": sub_cat_id,
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
        try:
            query = """
            select service_id,service_name,service_image from vtpartner.other_servicestbl where sub_cat_id=%s order by service_name asc
            """
            
            result = select_query(query,[sub_cat_id])

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "service_id": row[0],
                    "service_name": row[1],
                    "service_image": row[2]
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)




#Goods Driver Api's
@csrf_exempt
def goods_driver_login_view(request):
    if request.method == "POST":
        data = json.loads(request.body)
        mobile_no = data.get("mobile_no")

         # List of required fields
        required_fields = {
            "mobile_no": mobile_no,
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
                
                
        try:
            query = """
            SELECT goods_driver_id,driver_first_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,city_id FROM
            vtpartner.goods_driverstbl WHERE mobile_no=%s
            """
            params = [mobile_no]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                try:
                    #Insert if not found
                    query = """
                        INSERT INTO vtpartner.goods_driverstbl (
                            mobile_no
                        ) VALUES (%s) RETURNING goods_driver_id
                    """
                    values = [mobile_no]
                    new_result = insert_query(query, values)
                    print("new_result::",new_result)
                    if new_result:
                        print("new_result[0][0]::",new_result[0][0])
                        goods_driver_id = new_result[0][0]
                        response_value = [
                            {
                                "goods_driver_id":goods_driver_id
                            }
                        ]
                        return JsonResponse({"result": response_value}, status=200)
                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
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
                    "full_address": row[13],
                    "city_id": row[14],
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def goods_driver_registration(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        goods_driver_id = data.get("goods_driver_id")
        driver_first_name = data.get("driver_first_name")
        profile_pic = data.get("profile_pic")
        mobile_no = data.get("mobile_no")
        r_lat = data.get("r_lat")
        r_lng = data.get("r_lng")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")
        recent_online_pic = data.get("recent_online_pic")
        vehicle_id = data.get("vehicle_id")
        city_id = data.get("city_id")
        aadhar_no = data.get("aadhar_no")
        pan_card_no = data.get("pan_card_no")
        full_address = data.get("full_address")
        gender = data.get("gender")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
        insurance_image = data.get("insurance_image")
        noc_image = data.get("noc_image")
        pollution_certificate_image = data.get("pollution_certificate_image")
        rc_image = data.get("rc_image")
        vehicle_image = data.get("vehicle_image")
        vehicle_plate_image = data.get("vehicle_plate_image")
        driving_license_no = data.get("driving_license_no")
        vehicle_plate_no = data.get("vehicle_plate_no")
        rc_no = data.get("rc_no")
        insurance_no = data.get("insurance_no")
        noc_no = data.get("noc_no")
        vehicle_fuel_type = data.get("vehicle_fuel_type")
        owner_name = data.get("owner_name")
        owner_mobile_no = data.get("owner_mobile_no")
        owner_photo_url = data.get("owner_photo_url")
        owner_address = data.get("owner_address")
        owner_city_name = data.get("owner_city_name")
        
        
        
        
        # List of required fields
        required_fields = {
            "goods_driver_id":goods_driver_id,
            "driver_first_name":driver_first_name,
            "profile_pic":profile_pic,
            "mobile_no":mobile_no,
            "r_lat":r_lat,
            "r_lng":r_lng,
            "current_lat":current_lat,
            "current_lng":current_lng,
            "recent_online_pic":recent_online_pic,
            "vehicle_id":vehicle_id,
            "city_id":city_id,
            "aadhar_no":aadhar_no,
            "pan_card_no":pan_card_no,
            "full_address":full_address,
            "gender":gender,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            "license_front":license_front,
            "license_back":license_back,
            "insurance_image":insurance_image,
            "noc_image":noc_image,
            "pollution_certificate_image":pollution_certificate_image,
            "rc_image":rc_image,
            "vehicle_image":vehicle_image,
            "vehicle_plate_image":vehicle_plate_image,
            "driving_license_no":driving_license_no,
            "vehicle_plate_no":vehicle_plate_no,
            "rc_no":rc_no,
            "insurance_no":insurance_no,
            "noc_no":noc_no,
            "vehicle_fuel_type":vehicle_fuel_type,
            "owner_name":owner_name,
            "owner_mobile_no":owner_mobile_no,
            "owner_photo_url":owner_photo_url,
            "owner_address":owner_address,
            "owner_city_name":owner_city_name,
        }

        # Use the utility function to check for missing fields
        missing_fields = check_missing_fields(required_fields)

        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
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
                            owner_name, owner_mobile_no,  city_name, address, profile_photo
                        ) VALUES (%s, %s, %s,  %s, %s) RETURNING owner_id
                    """
                    owner_values = [owner_name, owner_mobile_no,  owner_city_name, owner_address, owner_photo_url]
                    new_owner_result = insert_query(insert_owner_query, owner_values)
                    if new_owner_result:
                        owner_id = new_owner_result[0][0]
                    else:
                        raise Exception("Failed to retrieve owner ID from insert operation")
            except Exception as error:
                print("Owner error::", error)
        print("owner_id::",owner_id)
        
        query = """
            UPDATE vtpartner.goods_driverstbl 
            SET 
            driver_first_name = %s,
            profile_pic = %s,
            mobile_no = %s,
            r_lat = %s,
            r_lng = %s,
            current_lat = %s,
            current_lng = %s,
            recent_online_pic = %s,
            category_id = %s,
            vehicle_id = %s,
            city_id = %s,
            aadhar_no = %s,
            pan_card_no = %s,
            full_address = %s,
            gender = %s,
            owner_id = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            license_front = %s,
            license_back = %s,
            insurance_image = %s,
            noc_image = %s,
            pollution_certificate_image = %s,
            rc_image = %s,
            vehicle_image = %s,
            vehicle_plate_image = %s,
            driving_license_no = %s,
            vehicle_plate_no = %s,
            rc_no = %s,
            insurance_no = %s,
            noc_no = %s,
            vehicle_fuel_type = %s
            WHERE goods_driver_id=%s
        """
        values = [
            driver_first_name,
            profile_pic,
            mobile_no,
            r_lat,
            r_lng,
            r_lat,
            r_lng,
            recent_online_pic,
            '1',
            vehicle_id,
            city_id,
            aadhar_no,
            pan_card_no,
            full_address,
            gender,
            owner_id,
            aadhar_card_front,
            aadhar_card_back,
            pan_card_front,
            pan_card_back,
            license_front,
            license_back,
            insurance_image,
            noc_image,
            pollution_certificate_image,
            rc_image,
            vehicle_image,
            vehicle_plate_image,
            driving_license_no,
            vehicle_plate_no,
            rc_no,
            insurance_no,
            noc_no,
            vehicle_fuel_type,
            goods_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)

@csrf_exempt
def goods_driver_aadhar_details_update(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        goods_driver_id = data.get("goods_driver_id")
        aadhar_no = data.get("aadhar_no")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "goods_driver_id":goods_driver_id,
            "aadhar_no":aadhar_no,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            aadhar_no = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            WHERE goods_driver_id=%s
        """
        values = [
            aadhar_no,
            aadhar_card_front,
            aadhar_card_back,
            goods_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def goods_driver_pan_card_details_update(request):
    try:
        data = json.loads(request.body)

        goods_driver_id = data.get("goods_driver_id")
        pan_card_no = data.get("pan_card_no")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "goods_driver_id":goods_driver_id,
            "pan_card_no":pan_card_no,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            pan_card_no = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            WHERE goods_driver_id=%s
        """
        values = [
            pan_card_no,
            pan_card_front,
            pan_card_back,
            goods_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def goods_driver_driving_license_details_update(request):
    try:
        data = json.loads(request.body)

        goods_driver_id = data.get("goods_driver_id")
        driving_license_no = data.get("driving_license_no")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "goods_driver_id":goods_driver_id,
            "driving_license_no":driving_license_no,
            "license_front":license_front,
            "license_back":license_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            driving_license_no = %s,
            license_front = %s,
            license_back = %s,
            WHERE goods_driver_id=%s
        """
        values = [
            driving_license_no,
            license_front,
            license_back,
            goods_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def goods_driver_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        goods_driver_id = data.get("goods_driver_id")

         # List of required fields
        required_fields = {
            "goods_driver_id": goods_driver_id,
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
                
                
        try:
            query = """
            select is_online,status,driver_first_name,recent_online_pic,profile_pic,mobile_no from vtpartner.goods_driverstbl where goods_driver_id=%s
            """
            params = [goods_driver_id]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "is_online": row[0],
                    "status": row[1],  
                    "driver_first_name": row[2],  
                    "recent_online_pic": row[3],  
                    "profile_pic": row[4],  
                    "mobile_no": row[5],  
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def goods_driver_update_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        goods_driver_id = data.get("goods_driver_id")
        recent_online_pic = data.get("recent_online_pic")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "status": status,
            "goods_driver_id": goods_driver_id,
            "lat": lat,
            "lng": lng
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

            if status == 1:
                # Include recent_online_pic in the query when status is 1
                query = """
                UPDATE vtpartner.goods_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s,
                    recent_online_pic = %s
                WHERE goods_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    recent_online_pic,
                    goods_driver_id
                ]
                

            else:
                # Exclude recent_online_pic when status is not 1
                query = """
                UPDATE vtpartner.goods_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s
                WHERE goods_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    goods_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)
            
            #insert record in attendance table
            query_insert = """
                    INSERT INTO vtpartner.goods_driver_attendance_tbl(driver_id, time, date, status) 
                    VALUES (%s, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE, %s)
                """

            insert_values = [
                goods_driver_id,
                status
            ]
            
            row_count = insert_query(query_insert,insert_values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_goods_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        goods_driver_id = data.get("goods_driver_id")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")

         # List of required fields
        required_fields = {
            "status": status,
            "goods_driver_id": goods_driver_id,
            "current_lat": current_lat,
            "current_lng": current_lng,
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
                
                
        try:
            query = """
            INSERT INTO vtpartner.active_goods_drivertbl 
            (goods_driver_id,current_lat,current_lng,current_status)
            VALUES (%s,%s,%s,%s) RETURNING active_id
            """
            values = [
                goods_driver_id,
                current_lat,
                current_lng,
                status,
            ]
            new_result = insert_query(query, values)
            print("new_result::",new_result)
            if new_result:
                print("new_result[0][0]::",new_result[0][0])
                active_id = new_result[0][0]
                response_value = [
                    {
                        "active_id":active_id
                    }
                ]
                # Send success response
                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_goods_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        goods_driver_id = data.get("goods_driver_id")
        

         # List of required fields
        required_fields = {
            "goods_driver_id": goods_driver_id,
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
                
                
        try:
            query = """
            DELETE FROM vtpartner.active_goods_drivertbl 
            WHERE goods_driver_id=%s
            """
            values = [
                goods_driver_id,
            ]
            row_count = delete_query(query, values)
            try:
                query2 = """
                UPDATE vtpartner.goods_driverstbl
                SET is_online=0
                WHERE goods_driver_id=%s
                """
                values2 = [
                    goods_driver_id,
                ]
                row_count = update_query(query2, values2)
                
                # Send success response
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
               

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_goods_drivers_current_location(request):
    if request.method == "POST":
        data = json.loads(request.body)
        goods_driver_id = data.get("goods_driver_id")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "goods_driver_id": goods_driver_id,
            "lat": lat,
            "lng": lng
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
                UPDATE vtpartner.active_goods_drivertbl 
                SET 
                    current_lat = %s,
                    current_lng = %s
                WHERE goods_driver_id = %s
                """
            values = [
                    lat,
                    lng,
                    goods_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt
def get_nearby_drivers(request):
    if request.method == "POST":
        data = json.loads(request.body)
        lat = data.get("lat")
        lng = data.get("lng")
        city_id = data.get("city_id")
        price_type = data.get("price_type", 1)
        radius_km = data.get("radius_km", 5)  # Radius in kilometers

        if lat is None or lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
         

            query = """
            SELECT 
    main.active_id, 
    main.goods_driver_id, 
    main.current_lat, 
    main.current_lng, 
    main.entry_time, 
    main.current_status, 
    goods_driverstbl.driver_first_name,
    goods_driverstbl.profile_pic, 
    vehiclestbl.image AS vehicle_image, 
    vehiclestbl.vehicle_name,
    vehiclestbl.weight,
    vehicle_city_wise_price_tbl.starting_price_per_km,
    vehicle_city_wise_price_tbl.base_fare,
    vehiclestbl.vehicle_id,
    vehiclestbl.size_image,
    (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
    )) AS distance
FROM vtpartner.active_goods_drivertbl AS main
INNER JOIN (
    SELECT goods_driver_id, MAX(entry_time) AS max_entry_time
    FROM vtpartner.active_goods_drivertbl
    GROUP BY goods_driver_id
) AS latest ON main.goods_driver_id = latest.goods_driver_id
             AND main.entry_time = latest.max_entry_time
JOIN vtpartner.goods_driverstbl ON main.goods_driver_id = goods_driverstbl.goods_driver_id
JOIN vtpartner.vehiclestbl ON goods_driverstbl.vehicle_id = vehiclestbl.vehicle_id
JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
WHERE main.current_status = 1
  AND (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
      )) <= %s
  AND goods_driverstbl.category_id = vehiclestbl.category_id
  AND goods_driverstbl.category_id = '1'
ORDER BY distance;

            """
            values = [lat, lng, lat,city_id,price_type, lat, lng, lat, radius_km]

            # Execute the query
            nearby_drivers = select_query(query, values)
            

            # Format response
            drivers_list = [
                {
                    "active_id": driver[0],
                    "goods_driver_id": driver[1],
                    "latitude": driver[2],
                    "longitude": driver[3],
                    "entry_time": driver[4],
                    "current_status": driver[5],
                    "driver_name": driver[6],
                    "driver_profile_pic": driver[7],
                    "vehicle_image": driver[8],
                    "vehicle_name": driver[9],
                    "weight": driver[10],
                    "starting_price_per_km": driver[11],
                    "base_fare": driver[12],
                    "vehicle_id": driver[13],
                    "size_image": driver[14],
                    "distance": driver[15]
                }
                for driver in nearby_drivers
            ]

            return JsonResponse({"nearby_drivers": drivers_list}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)
    

@csrf_exempt
def generate_new_goods_drivers_booking_id_get_nearby_drivers_with_fcm_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        # lat = data.get("lat")
        # lng = data.get("lng")
        # city_id = data.get("city_id")
        price_type = data.get("price_type", 1)
        radius_km = data.get("radius_km", 5)  # Radius in kilometers
        vehicle_id = data.get("vehicle_id")  # Vehicle ID
        # Read the individual fields from the JSON data
        customer_id = data.get("customer_id")
        pickup_lat = data.get("pickup_lat")
        pickup_lng = data.get("pickup_lng")
        destination_lat = data.get("destination_lat")
        destination_lng = data.get("destination_lng")
        distance = data.get("distance")
        time = data.get("time")
        total_price = data.get("total_price")
        base_price = data.get("base_price")
        otp = random.randint(1000, 9999)  # Generate a random 4-digit OTP
        gst_amount = data.get("gst_amount")
        igst_amount = data.get("igst_amount")
        goods_type_id = data.get("goods_type_id")
        payment_method = data.get("payment_method")
        city_id = data.get("city_id")
        sender_name = data.get("sender_name")
        sender_number = data.get("sender_number")
        receiver_name = data.get("receiver_name")
        receiver_number = data.get("receiver_number")
        pickup_address = data.get("pickup_address")
        drop_address = data.get("drop_address")
        server_access_token = data.get("server_access_token")

        # List of required fields
        required_fields = {
            "city_id":city_id,
            "price_type":price_type,
            "radius_km":radius_km,
            "vehicle_id":vehicle_id,
            "customer_id":customer_id,
            "pickup_lat":pickup_lat,
            "pickup_lng":pickup_lng,
            "destination_lat":destination_lat,
            "destination_lng":destination_lng,
            "distance":distance,
            "time":time,
            "total_price":total_price,
            "base_price":base_price,
            "otp":str(otp),
            "gst_amount":gst_amount,
            "igst_amount":igst_amount,
            "goods_type_id":goods_type_id,
            "payment_method":payment_method,
            "city_id":city_id,
            "sender_name":sender_name,
            "sender_number":sender_number,
            "receiver_name":receiver_name,
            "receiver_number":receiver_number,
            "pickup_address":pickup_address,
            "drop_address":drop_address,
            "server_access_token":server_access_token,
        }

        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        

        
        
        
        

        if pickup_lat is None or pickup_lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
            
            # Insert record in the booking table
            query_insert = """
                INSERT INTO vtpartner.bookings_tbl (
                    customer_id, driver_id, pickup_lat, pickup_lng, destination_lat, destination_lng, 
                    distance, time, total_price, base_price, booking_timing, booking_date, 
                    otp, gst_amount, igst_amount, 
                    payment_method, city_id,sender_name,sender_number,receiver_name,receiver_number,pickup_address,drop_address
                ) 
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE,  %s, %s, %s, 
                    %s, %s,%s, %s,%s, %s,%s, %s
                ) 
                RETURNING booking_id;
            """

            insert_values = [
                customer_id, '-1', pickup_lat, pickup_lng, destination_lat, destination_lng, 
                distance, time, total_price, base_price, otp, 
                gst_amount, igst_amount, payment_method, city_id,sender_name,sender_number,receiver_name,receiver_number,pickup_address,drop_address
            ]

            # Assuming insert_query is a function that runs the query
            new_result = insert_query(query_insert, insert_values)
            
            if new_result:
                booking_id = new_result[0][0]  # Extracting booking_id from the result
                response_value = [{"booking_id": booking_id}]
                
                #send notification to all goods driver
                fcm_data = {
                    'intent':'driver',
                    'booking_id':str(booking_id)
                }
                query = """
                    SELECT 
                    main.active_id, 
                    main.goods_driver_id, 
                    main.current_lat, 
                    main.current_lng, 
                    main.entry_time, 
                    main.current_status, 
                    goods_driverstbl.driver_first_name,
                    goods_driverstbl.profile_pic, 
                    vehiclestbl.image AS vehicle_image, 
                    vehiclestbl.vehicle_name,
                    vehiclestbl.weight,
                    vehicle_city_wise_price_tbl.starting_price_per_km,
                    vehicle_city_wise_price_tbl.base_fare,
                    vehiclestbl.vehicle_id,
                    vehiclestbl.size_image,
                    goods_driverstbl.authtoken,
                    (6371 * acos(
                        cos(radians(%s)) * cos(radians(main.current_lat)) *
                        cos(radians(main.current_lng) - radians(%s)) +
                        sin(radians(%s)) * sin(radians(main.current_lat))
                    )) AS distance
                FROM vtpartner.active_goods_drivertbl AS main
                INNER JOIN (
                    SELECT goods_driver_id, MAX(entry_time) AS max_entry_time
                    FROM vtpartner.active_goods_drivertbl
                    GROUP BY goods_driver_id
                ) AS latest ON main.goods_driver_id = latest.goods_driver_id
                            AND main.entry_time = latest.max_entry_time
                JOIN vtpartner.goods_driverstbl ON main.goods_driver_id = goods_driverstbl.goods_driver_id
                JOIN vtpartner.vehiclestbl ON goods_driverstbl.vehicle_id = vehiclestbl.vehicle_id
                JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
                AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
                WHERE main.current_status = 1
                AND (6371 * acos(
                        cos(radians(%s)) * cos(radians(main.current_lat)) *
                        cos(radians(main.current_lng) - radians(%s)) +
                        sin(radians(%s)) * sin(radians(main.current_lat))
                    )) <= %s
                AND goods_driverstbl.category_id = vehiclestbl.category_id
                AND goods_driverstbl.category_id = '1' AND  goods_driverstbl.vehicle_id=%s
                ORDER BY distance;

                """
                values = [pickup_lat, pickup_lng, pickup_lat,city_id,price_type, pickup_lat, pickup_lng, pickup_lat, radius_km,vehicle_id]

                # Execute the query
                nearby_drivers = select_query(query, values)
                

                # Format response
                # drivers_list = [
                #     {
                #         "active_id": driver[0],
                #         "goods_driver_id": driver[1],
                #         "latitude": driver[2],
                #         "longitude": driver[3],
                #         "entry_time": driver[4],
                #         "current_status": driver[5],
                #         "driver_name": driver[6],
                #         "driver_profile_pic": driver[7],
                #         "vehicle_image": driver[8],
                #         "vehicle_name": driver[9],
                #         "weight": driver[10],
                #         "starting_price_per_km": driver[11],
                #         "base_fare": driver[12],
                #         "vehicle_id": driver[13],
                #         "size_image": driver[14],
                #         "auth_token": driver[15],
                #         "distance": driver[16]
                #     }
                #     for driver in nearby_drivers
                # ]

                # Send notifications to all the online drivers
                # for driver in nearby_drivers:
                #     driver_auth_token = get_goods_driver_auth_token(driver[1])
                #     sendFMCMsg(
                #         driver_auth_token,
                #         f'You have a new Ride Request for \nPickup Location: {pickup_address}. \nDrop Location: {drop_address}',
                #         'New Goods Ride Request',
                #         fcm_data,
                #         server_access_token
                #     )
                for driver in nearby_drivers:
                    try:
                        driver_auth_token = get_goods_driver_auth_token2(driver[1])  # driver[1] assumed to be goods_driver_id
                        print(f"driver_auth_token ->{driver[1]} {driver_auth_token}")
                        
                        if driver_auth_token:
                            sendFMCMsg(
                                driver_auth_token,
                                f"You have a new Ride Request for \nPickup Location: {pickup_address}. \nDrop Location: {drop_address}",
                                "New Goods Ride Request",
                                fcm_data,
                                server_access_token
                            )
                            print(f"Notification sent to driver ID {driver[1]}")
                        else:
                            print(f"Skipped notification for driver ID {driver[1]} due to missing auth token")
                    except Exception as err:
                        print(f"Error sending notification to driver ID {driver[1]}: {err}")


                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def distance(request):
    if request.method == "POST":
        data = json.loads(request.body)
        origins = data.get("origins")
        destinations = data.get("destinations")
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
def get_all_goods_types(request):
    if request.method == "POST":        
        try:
            query = """
            select goods_type_id,goods_type_name from vtpartner.goods_type_tbl
            """
            
            result = select_query(query)

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "goods_type_id": row[0],
                    "goods_type_name": row[1]
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def get_all_guide_lines(request):
    if request.method == "POST":   
        data = json.loads(request.body)
        category_id = data.get("category_id",1)
        
        

        # List of required fields
        required_fields = {
            "category_id": category_id,
            
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
            select guide_id,guide_line from vtpartner.guide_lines_tbl where category_id=%s
            """
            
            result = select_query(query,[category_id])

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "guide_id": row[0],
                    "guide_line": row[1]
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_firebase_goods_driver_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        goods_driver_id = data.get("goods_driver_id")
        authToken = data.get("authToken")
        

        # List of required fields
        required_fields = {
            "goods_driver_id": goods_driver_id,
            "authToken": authToken,
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
                UPDATE vtpartner.goods_driverstbl 
                SET 
                    authtoken = %s
                WHERE goods_driver_id = %s
                """
            values = [
                    authToken,
                    goods_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def booking_details_for_ride_acceptance(request):
    if request.method == "POST":
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
            
        try:
            query = """
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,customer_name,customers_tbl.authtoken,pickup_address,drop_address from vtpartner.bookings_tbl,vtpartner.customers_tbl where customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "customer_name": row[28],
                    "customers_auth_token": row[29],
                    "pickup_address": row[30],
                    "drop_address": row[31],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

def get_customer_auth_token(customer_id):
    auth_token = ""
    try:
        query = """
        select authtoken from vtpartner.customers_tbl where customer_id=%s 
        """
        params = [customer_id]
        result = select_query(query, params)  # Assuming select_query is defined elsewhere

        if not result:
            return JsonResponse({"message": "No Data Found"}, status=404)

        # Extract auth_token from the result
        auth_token = result[0][0]  # Get the first row, first column (auth token)
        
        return auth_token

    except Exception as err:
        print("Error in finding the auth token for goods driver:", err)
        return auth_token
    
@csrf_exempt 
def goods_driver_booking_accepted(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "server_token": server_token,
            "customer_id":customer_id
        
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
                select driver_id from vtpartner.bookings_tbl where booking_id=%s and driver_id!='-1'
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                #Update booking status and driver assinged
                try:

                    query = """
                       update vtpartner.bookings_tbl set driver_id=%s ,booking_status='Driver Accepted' where booking_id=%s
                        """
                    values = [
                            driver_id,
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(query, values)
                    #Inserting record in booking_history table
                    try:

                        query = """
                           insert into vtpartner.bookings_history_tbl (status,booking_id) values ('Driver Accepted',%s)
                            """
                        values = [
                                booking_id
                            ]

                        # Execute the query
                        row_count = insert_query(query, values)
                        #Updating driver status to occupied
                        try:

                            query = """
                               update vtpartner.active_goods_drivertbl set current_status='2' where goods_driver_id=%s
                                """
                            values = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query, values)



                            #get the customer auth token
                            auth_token = get_customer_auth_token(customer_id)
                            
                            #send Fcm notification to customer saying driver assigned
                            customer_data = {
                                'intent':'live_tracking',
                                'booking_id':str(booking_id)
                            }
                            sendFMCMsg(auth_token,'You have been assigned a driver','Driver Assigned',customer_data,server_token)

                            # Send success response
                            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                        
                        
                        

                    except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500)

                    

                except Exception as err:
                    print("Error executing updating booking status to accepted:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)

            # Checking if driver is assigned
            ret_driver_id = result[0][0]
            return JsonResponse({"message": "No Data Found"}, status=404)
            
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def update_booking_status_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        total_payment = data.get("total_payment")

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "booking_status": booking_status,
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
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP Verified":
                    body = "You're OTP is Verified Successfully!"
                    title = "OTP Verification"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                    # Update Pickup epoch here
                    update_pickup_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET pickup_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_pickup_epoch_query, values)
                elif booking_status == "Make Payment":
                   body = f"Please do the payment against Booking ID {booking_id}. Total Amount=Rs.{total_payment}/-"
                   title = "Make Payment"
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                    # Update Drop epoch here
                    update_drop_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET drop_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_drop_epoch_query, values)
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)
    
@csrf_exempt 
def generate_order_id_for_booking_id_goods_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount")
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "booking_status": booking_status,
            "server_token": server_token,
            "customer_id": customer_id,
            "total_amount": total_amount,
            
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        #Updating the Status to Trip Ended
        
        try:

            query = """
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP verified":
                    body = "Your trip otp is verified"
                    title = "Trip OTP Verified"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                elif booking_status == "Ongoing":
                    body = "Trip has been started from your pickup location"
                    title = "Ongoing"
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
                
                #Generating Order ID
                try:
                    query = """
                    INSERT INTO vtpartner.orders_tbl (
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    )
                    SELECT 
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    FROM vtpartner.bookings_tbl
                    WHERE booking_id = %s
                    RETURNING order_id;
                    """

                    

                    # Execute the query
                    ret_result = insert_query2(query,[booking_id])
                    #get order_id from here
                    if ret_result!=None:
                        order_id = ret_result[0][0]
                        try:
                            query2 = """
                            update vtpartner.active_goods_drivertbl set current_status='1' where goods_driver_id=%s
                            """
                            values2 = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query2, values2)
                            #success
                            try:
                                query3 = """
                                update vtpartner.bookings_tbl set booking_completed='1' where booking_id=%s
                                """
                                values3 = [
                                        booking_id
                                    ]

                                # Execute the query
                                row_count = update_query(query3, values3)
                                
                                query_update = """
                                update vtpartner.orders_tbl set payment_method=%s,payment_id=%s where order_id=%s
                                """
                                values_update = [
                                        payment_method,
                                        payment_id,
                                        order_id
                                    ]

                                # Execute the query
                                row_count = update_query(query_update, values_update)
                                
                                #Adding the amount to driver earnings table
                                try:
                                    query4 = """
                                    insert into vtpartner.goods_driver_earningstbl(driver_id,amount,order_id,payment_id,payment_mode) values (%s,%s,%s,%s,%s)
                                    """
                                    values4 = [
                                            driver_id,
                                            total_amount,
                                            order_id,
                                            payment_id,
                                            payment_method
                                        ]

                                    # Execute the query
                                    row_count = insert_query(query4, values4)
                                    #success
                                    #Adding the amount to driver earnings table
                                    try:
                                        query5 = """
                                            UPDATE vtpartner.goods_driver_topup_recharge_current_points_tbl
                                            SET 
                                                used_points = used_points + %s,
                                                remaining_points = CASE 
                                                    WHEN remaining_points >= %s THEN remaining_points - %s
                                                    ELSE 0
                                                END,
                                                negative_points = CASE
                                                    WHEN remaining_points < %s THEN negative_points + (%s - remaining_points)
                                                    ELSE negative_points
                                                END,
                                                last_updated_time = date_part('epoch'::text, CURRENT_TIMESTAMP)
                                            WHERE driver_id = %s
                                        """
                                        values5 = [
                                                total_amount,  # %s for updating used_points
                                                total_amount,  # %s for checking remaining_points
                                                total_amount,  # %s for deducting from remaining_points
                                                total_amount,  # %s for checking if negative_points should be updated
                                                total_amount,  # %s for calculating difference for negative_points
                                                driver_id          # %s for identifying the driver
                                            ]

                                        # Execute the query
                                        row_count = insert_query(query5, values5)
                                        #success
                                        return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                                    except Exception as err:
                                        print("Error executing query:", err)
                                        return JsonResponse({"message": "An error occurred"}, status=500)
                                except Exception as err:
                                    print("Error executing query:", err)
                                    return JsonResponse({"message": "An error occurred"}, status=500)
                                
                                    #success
                                    #return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                            except Exception as err:
                                print("Error executing query:", err)
                                return JsonResponse({"message": "An error occurred"}, status=500)
                            
                            
                            # return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                    

                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
        
            except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500) 
        

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_goods_driver_recharge_list(request):
    if request.method == "POST":
        data = json.loads(request.body)
        category_id = data.get("category_id")
        

        # List of required fields
        required_fields = {
            "category_id": category_id,
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
               select recharge_id,amount,points,status,description,valid_days from vtpartner.recharge_plans_tbl where category_id=%s
                ORDER BY 
                    amount ASC
            """
            result = select_query(query,[category_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "recharge_id": row[0],
                    "amount": row[1],
                    "points": row[2],
                    "status": row[3],
                    "description": row[4],
                    "valid_days": row[5]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_goods_driver_current_recharge_details(request):
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
               select topup_id,recharge_id,allotted_points,used_points,remaining_points,negative_points,valid_till_date,status from vtpartner.goods_driver_topup_recharge_current_points_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "topup_id": row[0],
                    "recharge_id": row[1],
                    "allotted_points": row[2],
                    "used_points": row[3],
                    "remaining_points": row[4],
                    "negative_points": row[5],
                    "valid_till_date": row[6],
                    "status": row[7]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def get_goods_driver_recharge_history_details(request):
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
               select history_id,recharge_id,amount,allotted_points,date,valid_till_date,status,payment_method,payment_id,transaction_type,admin_id,last_recharge_negative_points from vtpartner.goods_driver_topup_recharge_history_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "history_id": row[0],
                    "recharge_id": row[1],
                    "amount": row[2],
                    "allotted_points": row[3],
                    "date": row[4],
                    "valid_till_date": row[5],
                    "status": row[6],
                    "payment_method": row[7],
                    "payment_id": row[8],
                    "transaction_type": row[9],
                    "admin_id": row[10],
                    "last_recharge_negative_points": row[11],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def new_goods_driver_recharge(request):
    if request.method == "POST":
        data = json.loads(request.body)
        driver_id = data.get("driver_id")
        topup_id = data.get("topup_id")
        recharge_id = data.get("recharge_id")
        amount = data.get("amount")
        allotted_points = data.get("allotted_points")
        valid_till_date = data.get("valid_till_date")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        negative_points = data.get("previous_negative_points")
        last_validity_date = data.get("last_validity_date")

        # List of required fields
        required_fields = {
            "driver_id": driver_id,
            "topup_id": topup_id,
            "recharge_id": recharge_id,
            "amount": amount,
            "allotted_points": allotted_points,
            "valid_till_date": valid_till_date,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "previous_negative_points": negative_points,
            "last_validity_date": last_validity_date,
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        if negative_points > 0:
            allotted_points -= negative_points
        try:
            query = """
                insert into vtpartner.goods_driver_topup_recharge_history_tbl(driver_id,recharge_id,amount,allotted_points,valid_till_date,payment_method,payment_id,last_recharge_negative_points) values (%s,%s,%s,%s,%s,%s,%s,%s)
                """
            values = [
                    driver_id,
                    recharge_id,
                    amount,
                    allotted_points,
                    valid_till_date,
                    payment_method,
                    payment_id,
                    negative_points
                ]
            # Execute the query
            row_count = insert_query(query, values)
            
            #checking if recharge has been expired
            # last_validity_date_obj = datetime.strptime(last_validity_date, "%Y-%m-%d")

            # # Get the current date
            # current_date = datetime.now()

            # # Check if last_validity_date is greater than the current date
            # isExpired = False
            # if last_validity_date_obj > current_date:
            #     print("The last validity date is in the future.")
            # else:
            #     isExpired = True
            #     print("The last validity date is today or has passed.")

            # Updating Booking History Table
            try:
                # if negative_points > 0 or isExpired:
                if negative_points > 0 :
                    query = """
                    update vtpartner.goods_driver_topup_recharge_current_points_tbl set recharge_id=%s,allotted_points=%s,valid_till_date=%s,remaining_points=%s,negative_points='0',used_points='0' where topup_id=%s and driver_id=%s
                    """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            allotted_points,
                            topup_id,
                            driver_id
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)
                else:
                    query = """
                        INSERT INTO vtpartner.goods_driver_topup_recharge_current_points_tbl (recharge_id,allotted_points,valid_till_date,driver_id,remaining_points) VALUES (%s,%s,%s,%s,%s)
                        """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            driver_id,
                            allotted_points
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)

                
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def goods_driver_all_orders(request):
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
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,goods_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,goods_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,orders_tbl.ratings,orders_tbl.rating_description from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.goods_driverstbl,vtpartner.customers_tbl where goods_driverstbl.goods_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and orders_tbl.driver_id=%s and  vehiclestbl.vehicle_id=goods_driverstbl.vehicle_id order by order_id desc
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "ratings": str(row[37]),
                    "rating_description": str(row[38]),
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def goods_driver_whole_year_earnings(request):
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
                SELECT
                months.month_index,
                COALESCE(SUM(e.amount), 0.0) AS total_earnings
            FROM (
                SELECT 1 AS month_index UNION ALL
                SELECT 2 UNION ALL
                SELECT 3 UNION ALL
                SELECT 4 UNION ALL
                SELECT 5 UNION ALL
                SELECT 6 UNION ALL
                SELECT 7 UNION ALL
                SELECT 8 UNION ALL
                SELECT 9 UNION ALL
                SELECT 10 UNION ALL
                SELECT 11 UNION ALL
                SELECT 12
            ) AS months
            LEFT JOIN vtpartner.goods_driver_earningstbl e
                ON EXTRACT(MONTH FROM e.earning_date) = months.month_index
                AND e.driver_id = %s
            GROUP BY months.month_index
            ORDER BY months.month_index;

            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            earning_details = [
                {
                    "month_index": row[0],
                    "total_earnings": row[1],
                   
                }
                for row in result
            ]

            return JsonResponse({"results": earning_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def goods_driver_todays_earnings(request):
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
            # Query to get today's earnings and rides count
            query = """
                SELECT COALESCE(SUM(amount), 0) AS todays_earnings, 
                       COUNT(*) AS todays_rides 
                FROM vtpartner.goods_driver_earningstbl 
                WHERE driver_id = %s AND earning_date = CURRENT_DATE;
            """
            result = select_query(query, [driver_id])  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Extract the first row from the result
            row = result[0]
            earning_details = {
                "todays_earnings": row[0],
                "todays_rides": row[1],
            }

            return JsonResponse({"results": [earning_details]}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


#Cab Driver Api's
@csrf_exempt
def cab_driver_login_view(request):
    if request.method == "POST":
        data = json.loads(request.body)
        mobile_no = data.get("mobile_no")

         # List of required fields
        required_fields = {
            "mobile_no": mobile_no,
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
                
                
        try:
            query = """
            SELECT cab_driver_id,driver_first_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,city_id FROM
            vtpartner.cab_driverstbl WHERE mobile_no=%s
            """
            params = [mobile_no]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                try:
                    #Insert if not found
                    query = """
                        INSERT INTO vtpartner.cab_driverstbl (
                            mobile_no
                        ) VALUES (%s) RETURNING cab_driver_id
                    """
                    values = [mobile_no]
                    new_result = insert_query(query, values)
                    print("new_result::",new_result)
                    if new_result:
                        print("new_result[0][0]::",new_result[0][0])
                        cab_driver_id = new_result[0][0]
                        response_value = [
                            {
                                "cab_driver_id":cab_driver_id
                            }
                        ]
                        return JsonResponse({"result": response_value}, status=200)
                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "cab_driver_id": row[0],
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
                    "full_address": row[13],
                    "city_id": row[14],
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def cab_driver_registration(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        cab_driver_id = data.get("cab_driver_id")
        driver_first_name = data.get("driver_first_name")
        profile_pic = data.get("profile_pic")
        mobile_no = data.get("mobile_no")
        r_lat = data.get("r_lat")
        r_lng = data.get("r_lng")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")
        recent_online_pic = data.get("recent_online_pic")
        vehicle_id = data.get("vehicle_id")
        city_id = data.get("city_id")
        aadhar_no = data.get("aadhar_no")
        pan_card_no = data.get("pan_card_no")
        full_address = data.get("full_address")
        gender = data.get("gender")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
        insurance_image = data.get("insurance_image")
        noc_image = data.get("noc_image")
        pollution_certificate_image = data.get("pollution_certificate_image")
        rc_image = data.get("rc_image")
        vehicle_image = data.get("vehicle_image")
        vehicle_plate_image = data.get("vehicle_plate_image")
        driving_license_no = data.get("driving_license_no")
        vehicle_plate_no = data.get("vehicle_plate_no")
        rc_no = data.get("rc_no")
        insurance_no = data.get("insurance_no")
        noc_no = data.get("noc_no")
        vehicle_fuel_type = data.get("vehicle_fuel_type")
        owner_name = data.get("owner_name")
        owner_mobile_no = data.get("owner_mobile_no")
        owner_photo_url = data.get("owner_photo_url")
        owner_address = data.get("owner_address")
        owner_city_name = data.get("owner_city_name")
        
        
        
        
        # List of required fields
        required_fields = {
            "cab_driver_id":cab_driver_id,
            "driver_first_name":driver_first_name,
            "profile_pic":profile_pic,
            "mobile_no":mobile_no,
            "r_lat":r_lat,
            "r_lng":r_lng,
            "current_lat":current_lat,
            "current_lng":current_lng,
            "recent_online_pic":recent_online_pic,
            "vehicle_id":vehicle_id,
            "city_id":city_id,
            "aadhar_no":aadhar_no,
            "pan_card_no":pan_card_no,
            "full_address":full_address,
            "gender":gender,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            "license_front":license_front,
            "license_back":license_back,
            "insurance_image":insurance_image,
            "noc_image":noc_image,
            "pollution_certificate_image":pollution_certificate_image,
            "rc_image":rc_image,
            "vehicle_image":vehicle_image,
            "vehicle_plate_image":vehicle_plate_image,
            "driving_license_no":driving_license_no,
            "vehicle_plate_no":vehicle_plate_no,
            "rc_no":rc_no,
            "insurance_no":insurance_no,
            "noc_no":noc_no,
            "vehicle_fuel_type":vehicle_fuel_type,
            "owner_name":owner_name,
            "owner_mobile_no":owner_mobile_no,
            "owner_photo_url":owner_photo_url,
            "owner_address":owner_address,
            "owner_city_name":owner_city_name,
        }

        # Use the utility function to check for missing fields
        missing_fields = check_missing_fields(required_fields)

        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
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
                            owner_name, owner_mobile_no,  city_name, address, profile_photo
                        ) VALUES (%s, %s, %s,  %s, %s) RETURNING owner_id
                    """
                    owner_values = [owner_name, owner_mobile_no,  owner_city_name, owner_address, owner_photo_url]
                    new_owner_result = insert_query(insert_owner_query, owner_values)
                    if new_owner_result:
                        owner_id = new_owner_result[0][0]
                    else:
                        raise Exception("Failed to retrieve owner ID from insert operation")
            except Exception as error:
                print("Owner error::", error)
        print("owner_id::",owner_id)
        
        query = """
            UPDATE vtpartner.cab_driverstbl 
            SET 
            driver_first_name = %s,
            profile_pic = %s,
            mobile_no = %s,
            r_lat = %s,
            r_lng = %s,
            current_lat = %s,
            current_lng = %s,
            recent_online_pic = %s,
            category_id = %s,
            vehicle_id = %s,
            city_id = %s,
            aadhar_no = %s,
            pan_card_no = %s,
            full_address = %s,
            gender = %s,
            owner_id = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            license_front = %s,
            license_back = %s,
            insurance_image = %s,
            noc_image = %s,
            pollution_certificate_image = %s,
            rc_image = %s,
            vehicle_image = %s,
            vehicle_plate_image = %s,
            driving_license_no = %s,
            vehicle_plate_no = %s,
            rc_no = %s,
            insurance_no = %s,
            noc_no = %s,
            vehicle_fuel_type = %s
            WHERE cab_driver_id=%s
        """
        values = [
            driver_first_name,
            profile_pic,
            mobile_no,
            r_lat,
            r_lng,
            r_lat,
            r_lng,
            recent_online_pic,
            '2',
            vehicle_id,
            city_id,
            aadhar_no,
            pan_card_no,
            full_address,
            gender,
            owner_id,
            aadhar_card_front,
            aadhar_card_back,
            pan_card_front,
            pan_card_back,
            license_front,
            license_back,
            insurance_image,
            noc_image,
            pollution_certificate_image,
            rc_image,
            vehicle_image,
            vehicle_plate_image,
            driving_license_no,
            vehicle_plate_no,
            rc_no,
            insurance_no,
            noc_no,
            vehicle_fuel_type,
            cab_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)

@csrf_exempt
def cab_driver_aadhar_details_update(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        cab_driver_id = data.get("cab_driver_id")
        aadhar_no = data.get("aadhar_no")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "cab_driver_id":cab_driver_id,
            "aadhar_no":aadhar_no,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            aadhar_no = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            WHERE cab_driver_id=%s
        """
        values = [
            aadhar_no,
            aadhar_card_front,
            aadhar_card_back,
            cab_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def cab_driver_pan_card_details_update(request):
    try:
        data = json.loads(request.body)

        cab_driver_id = data.get("cab_driver_id")
        pan_card_no = data.get("pan_card_no")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "cab_driver_id":cab_driver_id,
            "pan_card_no":pan_card_no,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            pan_card_no = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            WHERE cab_driver_id=%s
        """
        values = [
            pan_card_no,
            pan_card_front,
            pan_card_back,
            cab_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def cab_driver_driving_license_details_update(request):
    try:
        data = json.loads(request.body)

        cab_driver_id = data.get("cab_driver_id")
        driving_license_no = data.get("driving_license_no")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "cab_driver_id":cab_driver_id,
            "driving_license_no":driving_license_no,
            "license_front":license_front,
            "license_back":license_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            driving_license_no = %s,
            license_front = %s,
            license_back = %s,
            WHERE cab_driver_id=%s
        """
        values = [
            driving_license_no,
            license_front,
            license_back,
            cab_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def cab_driver_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        cab_driver_id = data.get("cab_driver_id")

         # List of required fields
        required_fields = {
            "cab_driver_id": cab_driver_id,
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
                
                
        try:
            query = """
            select is_online,status,driver_first_name,recent_online_pic,profile_pic,mobile_no from vtpartner.cab_driverstbl where cab_driver_id=%s
            """
            params = [cab_driver_id]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "is_online": row[0],
                    "status": row[1],  
                    "driver_first_name": row[2],  
                    "recent_online_pic": row[3],  
                    "profile_pic": row[4],  
                    "mobile_no": row[5],  
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def cab_driver_update_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        cab_driver_id = data.get("cab_driver_id")
        recent_online_pic = data.get("recent_online_pic")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "status": status,
            "cab_driver_id": cab_driver_id,
            "lat": lat,
            "lng": lng
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

            if status == 1:
                # Include recent_online_pic in the query when status is 1
                query = """
                UPDATE vtpartner.cab_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s,
                    recent_online_pic = %s
                WHERE cab_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    recent_online_pic,
                    cab_driver_id
                ]
                

            else:
                # Exclude recent_online_pic when status is not 1
                query = """
                UPDATE vtpartner.cab_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s
                WHERE cab_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    cab_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)
            
            #insert record in attendance table
            query_insert = """
                    INSERT INTO vtpartner.cab_driver_attendance_tbl(driver_id, time, date, status) 
                    VALUES (%s, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE, %s)
                """

            insert_values = [
                cab_driver_id,
                status
            ]
            
            row_count = insert_query(query_insert,insert_values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_cab_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        cab_driver_id = data.get("cab_driver_id")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")

         # List of required fields
        required_fields = {
            "status": status,
            "cab_driver_id": cab_driver_id,
            "current_lat": current_lat,
            "current_lng": current_lng,
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
                
                
        try:
            query = """
            INSERT INTO vtpartner.active_cab_drivertbl 
            (cab_driver_id,current_lat,current_lng,current_status)
            VALUES (%s,%s,%s,%s) RETURNING active_id
            """
            values = [
                cab_driver_id,
                current_lat,
                current_lng,
                status,
            ]
            new_result = insert_query(query, values)
            print("new_result::",new_result)
            if new_result:
                print("new_result[0][0]::",new_result[0][0])
                active_id = new_result[0][0]
                response_value = [
                    {
                        "active_id":active_id
                    }
                ]
                # Send success response
                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_cab_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        cab_driver_id = data.get("cab_driver_id")
        

         # List of required fields
        required_fields = {
            "cab_driver_id": cab_driver_id,
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
                
                
        try:
            query = """
            DELETE FROM vtpartner.active_cab_drivertbl 
            WHERE cab_driver_id=%s
            """
            values = [
                cab_driver_id,
            ]
            row_count = delete_query(query, values)
            try:
                query2 = """
                UPDATE vtpartner.cab_driverstbl
                SET is_online=0
                WHERE cab_driver_id=%s
                """
                values2 = [
                    cab_driver_id,
                ]
                row_count = update_query(query2, values2)
                
                # Send success response
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_cab_drivers_current_location(request):
    if request.method == "POST":
        data = json.loads(request.body)
        cab_driver_id = data.get("cab_driver_id")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "cab_driver_id": cab_driver_id,
            "lat": lat,
            "lng": lng
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
                UPDATE vtpartner.active_cab_drivertbl 
                SET 
                    current_lat = %s,
                    current_lng = %s
                WHERE cab_driver_id = %s
                """
            values = [
                    lat,
                    lng,
                    cab_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt
def get_nearby_cab_drivers(request):
    if request.method == "POST":
        data = json.loads(request.body)
        lat = data.get("lat")
        lng = data.get("lng")
        city_id = data.get("city_id")
        price_type = data.get("price_type", 1)
        radius_km = data.get("radius_km", 5)  # Radius in kilometers

        if lat is None or lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
            # Haversine formula to calculate the distance in kilometers
            #SELECT main.active_id, main.cab_driver_id, main.current_lat, main.current_lng, 
#        main.entry_time, main.current_status, cab_driverstbl.driver_first_name,
#        cab_driverstbl.profile_pic, vehiclestbl.image AS vehicle_image, 
#        vehiclestbl.vehicle_name,weight,
#        (6371 * acos(
#            cos(radians(%s)) * cos(radians(main.current_lat)) *
#            cos(radians(main.current_lng) - radians(%s)) +
#            sin(radians(%s)) * sin(radians(main.current_lat))
#        )) AS distance
# FROM vtpartner.active_cab_drivertbl AS main
# INNER JOIN (
#     SELECT cab_driver_id, MAX(entry_time) AS max_entry_time
#     FROM vtpartner.active_cab_drivertbl
#     GROUP BY cab_driver_id
# ) AS latest ON main.cab_driver_id = latest.cab_driver_id
#               AND main.entry_time = latest.max_entry_time
# JOIN vtpartner.cab_driverstbl ON main.cab_driver_id = cab_driverstbl.cab_driver_id
# JOIN vtpartner.vehiclestbl ON cab_driverstbl.vehicle_id = vehiclestbl.vehicle_id
# WHERE main.current_status = 1
#   AND (6371 * acos(
#          cos(radians(%s)) * cos(radians(main.current_lat)) *
#          cos(radians(main.current_lng) - radians(%s)) +
#          sin(radians(%s)) * sin(radians(main.current_lat))
#      )) <= %s
#   AND cab_driverstbl.category_id = vehiclestbl.category_id
#   AND cab_driverstbl.category_id = '1'
# ORDER BY distance;
#             """
#             values = [lat, lng, lat, lat, lng, lat, radius_km]

            query = """
            SELECT 
    main.active_id, 
    main.cab_driver_id, 
    main.current_lat, 
    main.current_lng, 
    main.entry_time, 
    main.current_status, 
    cab_driverstbl.driver_first_name,
    cab_driverstbl.profile_pic, 
    vehiclestbl.image AS vehicle_image, 
    vehiclestbl.vehicle_name,
    vehiclestbl.weight,
    vehicle_city_wise_price_tbl.starting_price_per_km,
    vehicle_city_wise_price_tbl.base_fare,
    vehiclestbl.vehicle_id,
    vehiclestbl.size_image,
    (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
    )) AS distance
FROM vtpartner.active_cab_drivertbl AS main
INNER JOIN (
    SELECT cab_driver_id, MAX(entry_time) AS max_entry_time
    FROM vtpartner.active_cab_drivertbl
    GROUP BY cab_driver_id
) AS latest ON main.cab_driver_id = latest.cab_driver_id
             AND main.entry_time = latest.max_entry_time
JOIN vtpartner.cab_driverstbl ON main.cab_driver_id = cab_driverstbl.cab_driver_id
JOIN vtpartner.vehiclestbl ON cab_driverstbl.vehicle_id = vehiclestbl.vehicle_id
JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
WHERE main.current_status = 1
  AND (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
      )) <= %s
  AND cab_driverstbl.category_id = vehiclestbl.category_id
  AND cab_driverstbl.category_id = '1'
ORDER BY distance;

            """
            values = [lat, lng, lat,city_id,price_type, lat, lng, lat, radius_km]

            # Execute the query
            nearby_drivers = select_query(query, values)
            

            # Format response
            drivers_list = [
                {
                    "active_id": driver[0],
                    "cab_driver_id": driver[1],
                    "latitude": driver[2],
                    "longitude": driver[3],
                    "entry_time": driver[4],
                    "current_status": driver[5],
                    "driver_name": driver[6],
                    "driver_profile_pic": driver[7],
                    "vehicle_image": driver[8],
                    "vehicle_name": driver[9],
                    "weight": driver[10],
                    "starting_price_per_km": driver[11],
                    "base_fare": driver[12],
                    "vehicle_id": driver[13],
                    "size_image": driver[14],
                    "distance": driver[15]
                }
                for driver in nearby_drivers
            ]

            return JsonResponse({"nearby_drivers": drivers_list}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)




@csrf_exempt
def update_firebase_cab_driver_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        cab_driver_id = data.get("cab_driver_id")
        authToken = data.get("authToken")
        

        # List of required fields
        required_fields = {
            "cab_driver_id": cab_driver_id,
            "authToken": authToken,
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
                UPDATE vtpartner.cab_driverstbl 
                SET 
                    authtoken = %s
                WHERE cab_driver_id = %s
                """
            values = [
                    authToken,
                    cab_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def cab_booking_details_for_ride_acceptance(request):
    if request.method == "POST":
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
            
        try:
            query = """
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,customer_name,customers_tbl.authtoken,pickup_address,drop_address from vtpartner.bookings_tbl,vtpartner.customers_tbl where customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "customer_name": row[28],
                    "customers_auth_token": row[29],
                    "pickup_address": row[30],
                    "drop_address": row[31],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def cab_driver_booking_accepted(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "server_token": server_token,
            "customer_id":customer_id
        
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
                select driver_id from vtpartner.bookings_tbl where booking_id=%s and driver_id!='-1'
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                #Update booking status and driver assinged
                try:

                    query = """
                       update vtpartner.bookings_tbl set driver_id=%s ,booking_status='Driver Accepted' where booking_id=%s
                        """
                    values = [
                            driver_id,
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(query, values)
                    #Inserting record in booking_history table
                    try:

                        query = """
                           insert into vtpartner.bookings_history_tbl (status,booking_id) values ('Driver Accepted',%s)
                            """
                        values = [
                                booking_id
                            ]

                        # Execute the query
                        row_count = insert_query(query, values)
                        #Updating driver status to occupied
                        try:

                            query = """
                               update vtpartner.active_cab_drivertbl set current_status='2' where cab_driver_id=%s
                                """
                            values = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query, values)



                            #get the customer auth token
                            auth_token = get_customer_auth_token(customer_id)
                            
                            #send Fcm notification to customer saying driver assigned
                            customer_data = {
                                'intent':'live_tracking',
                                'booking_id':str(booking_id)
                            }
                            sendFMCMsg(auth_token,'You have been assigned a driver','Driver Assigned',customer_data,server_token)

                            # Send success response
                            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                        
                        
                        

                    except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500)

                    

                except Exception as err:
                    print("Error executing updating booking status to accepted:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)

            # Checking if driver is assigned
            ret_driver_id = result[0][0]
            return JsonResponse({"message": "No Data Found"}, status=404)
            
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def update_booking_status_cab_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "booking_status": booking_status,
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
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP Verified":
                    body = "You're OTP is Verified Successfully!"
                    title = "OTP Verification"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                    # Update Pickup epoch here
                    update_pickup_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET pickup_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_pickup_epoch_query, values)
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                    # Update Drop epoch here
                    update_drop_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET drop_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_drop_epoch_query, values)
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)
    
@csrf_exempt 
def generate_order_id_for_booking_id_cab_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount")
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "booking_status": booking_status,
            "server_token": server_token,
            "customer_id": customer_id,
            "total_amount": total_amount,
            
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        #Updating the Status to Trip Ended
        
        try:

            query = """
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP verified":
                    body = "Your trip otp is verified"
                    title = "Trip OTP Verified"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                elif booking_status == "Ongoing":
                    body = "Trip has been started from your pickup location"
                    title = "Ongoing"
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
                
                #Generating Order ID
                try:
                    query = """
                    INSERT INTO vtpartner.orders_tbl (
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    )
                    SELECT 
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    FROM vtpartner.bookings_tbl
                    WHERE booking_id = %s
                    RETURNING order_id;
                    """

                    

                    # Execute the query
                    ret_result = insert_query2(query,[booking_id])
                    #get order_id from here
                    if ret_result!=None:
                        order_id = ret_result[0][0]
                        try:
                            query2 = """
                            update vtpartner.active_cab_drivertbl set current_status='1' where cab_driver_id=%s
                            """
                            values2 = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query2, values2)
                            #success
                            try:
                                query3 = """
                                update vtpartner.bookings_tbl set booking_completed='1' where booking_id=%s
                                """
                                values3 = [
                                        booking_id
                                    ]

                                # Execute the query
                                row_count = update_query(query3, values3)
                                
                                query_update = """
                                update vtpartner.orders_tbl set payment_method=%s,payment_id=%s where order_id=%s
                                """
                                values_update = [
                                        payment_method,
                                        payment_id,
                                        order_id
                                    ]

                                # Execute the query
                                row_count = update_query(query_update, values_update)
                                
                                #Adding the amount to driver earnings table
                                try:
                                    query4 = """
                                    insert into vtpartner.cab_driver_earningstbl(driver_id,amount,order_id,payment_id,payment_mode) values (%s,%s,%s,%s,%s)
                                    """
                                    values4 = [
                                            driver_id,
                                            total_amount,
                                            order_id,
                                            payment_id,
                                            payment_method
                                        ]

                                    # Execute the query
                                    row_count = insert_query(query4, values4)
                                    #success
                                    #Adding the amount to driver earnings table
                                    try:
                                        query5 = """
                                            UPDATE vtpartner.cab_driver_topup_recharge_current_points_tbl
                                            SET 
                                                used_points = used_points + %s,
                                                remaining_points = CASE 
                                                    WHEN remaining_points >= %s THEN remaining_points - %s
                                                    ELSE 0
                                                END,
                                                negative_points = CASE
                                                    WHEN remaining_points < %s THEN negative_points + (%s - remaining_points)
                                                    ELSE negative_points
                                                END,
                                                last_updated_time = date_part('epoch'::text, CURRENT_TIMESTAMP)
                                            WHERE driver_id = %s
                                        """
                                        values5 = [
                                                total_amount,  # %s for updating used_points
                                                total_amount,  # %s for checking remaining_points
                                                total_amount,  # %s for deducting from remaining_points
                                                total_amount,  # %s for checking if negative_points should be updated
                                                total_amount,  # %s for calculating difference for negative_points
                                                driver_id          # %s for identifying the driver
                                            ]

                                        # Execute the query
                                        row_count = insert_query(query5, values5)
                                        #success
                                        return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                                    except Exception as err:
                                        print("Error executing query:", err)
                                        return JsonResponse({"message": "An error occurred"}, status=500)
                                except Exception as err:
                                    print("Error executing query:", err)
                                    return JsonResponse({"message": "An error occurred"}, status=500)
                                
                                    #success
                                    #return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                            except Exception as err:
                                print("Error executing query:", err)
                                return JsonResponse({"message": "An error occurred"}, status=500)
                            
                            
                            # return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                    

                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
        
            except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500) 
        

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_cab_driver_recharge_list(request):
    if request.method == "POST":
        data = json.loads(request.body)
        category_id = data.get("category_id")
        

        # List of required fields
        required_fields = {
            "category_id": category_id,
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
               select recharge_id,amount,points,status,description,valid_days from vtpartner.recharge_plans_tbl where category_id=%s
                ORDER BY 
                    amount ASC
            """
            result = select_query(query,[category_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "recharge_id": row[0],
                    "amount": row[1],
                    "points": row[2],
                    "status": row[3],
                    "description": row[4],
                    "valid_days": row[5]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_cab_driver_current_recharge_details(request):
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
               select topup_id,recharge_id,allotted_points,used_points,remaining_points,negative_points,valid_till_date,status from vtpartner.cab_driver_topup_recharge_current_points_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "topup_id": row[0],
                    "recharge_id": row[1],
                    "allotted_points": row[2],
                    "used_points": row[3],
                    "remaining_points": row[4],
                    "negative_points": row[5],
                    "valid_till_date": row[6],
                    "status": row[7]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def get_cab_driver_recharge_history_details(request):
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
               select history_id,recharge_id,amount,allotted_points,date,valid_till_date,status,payment_method,payment_id,transaction_type,admin_id,last_recharge_negative_points from vtpartner.cab_driver_topup_recharge_history_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "history_id": row[0],
                    "recharge_id": row[1],
                    "amount": row[2],
                    "allotted_points": row[3],
                    "date": row[4],
                    "valid_till_date": row[5],
                    "status": row[6],
                    "payment_method": row[7],
                    "payment_id": row[8],
                    "transaction_type": row[9],
                    "admin_id": row[10],
                    "last_recharge_negative_points": row[11],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def new_cab_driver_recharge(request):
    if request.method == "POST":
        data = json.loads(request.body)
        driver_id = data.get("driver_id")
        topup_id = data.get("topup_id")
        recharge_id = data.get("recharge_id")
        amount = data.get("amount")
        allotted_points = data.get("allotted_points")
        valid_till_date = data.get("valid_till_date")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        negative_points = data.get("previous_negative_points")
        last_validity_date = data.get("last_validity_date")

        # List of required fields
        required_fields = {
            "driver_id": driver_id,
            "topup_id": topup_id,
            "recharge_id": recharge_id,
            "amount": amount,
            "allotted_points": allotted_points,
            "valid_till_date": valid_till_date,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "previous_negative_points": negative_points,
            "last_validity_date": last_validity_date,
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        if negative_points > 0:
            allotted_points -= negative_points
        try:
            query = """
                insert into vtpartner.cab_driver_topup_recharge_history_tbl(driver_id,recharge_id,amount,allotted_points,valid_till_date,payment_method,payment_id,last_recharge_negative_points) values (%s,%s,%s,%s,%s,%s,%s,%s)
                """
            values = [
                    driver_id,
                    recharge_id,
                    amount,
                    allotted_points,
                    valid_till_date,
                    payment_method,
                    payment_id,
                    negative_points
                ]
            # Execute the query
            row_count = insert_query(query, values)
            
            # #checking if recharge has been expired
            # last_validity_date_obj = datetime.strptime(last_validity_date, "%Y-%m-%d")

            # # Get the current date
            # current_date = datetime.now()

            # # Check if last_validity_date is greater than the current date
            # isExpired = False
            # if last_validity_date_obj > current_date:
            #     print("The last validity date is in the future.")
            # else:
            #     isExpired = True
            #     print("The last validity date is today or has passed.")

            # Updating Booking History Table
            try:
                if negative_points > 0 :
                    query = """
                    update vtpartner.cab_driver_topup_recharge_current_points_tbl set recharge_id=%s,allotted_points=%s,valid_till_date=%s,remaining_points=%s,negative_points='0',used_points='0' where topup_id=%s and driver_id=%s
                    """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            allotted_points,
                            topup_id,
                            driver_id
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)
                else:
                    query = """
                        INSERT INTO vtpartner.cab_driver_topup_recharge_current_points_tbl (recharge_id,allotted_points,valid_till_date,driver_id,remaining_points) VALUES (%s,%s,%s,%s,%s)
                        """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            driver_id,
                            allotted_points
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)

                
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def cab_driver_all_orders(request):
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
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,cab_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,cab_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,orders_tbl.ratings,orders_tbl.rating_description from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.cab_driverstbl,vtpartner.customers_tbl where cab_driverstbl.cab_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and orders_tbl.driver_id=%s and  vehiclestbl.vehicle_id=cab_driverstbl.vehicle_id order by order_id desc
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "cab_driver_auth_token": str(row[27]),
                    "customer_name": str(row[28]),
                    "customers_auth_token": str(row[29]),
                    "pickup_address": str(row[30]),
                    "drop_address": str(row[31]),
                    "customer_mobile_no": str(row[32]),
                    "driver_mobile_no": str(row[33]),
                    "vehicle_id": str(row[34]),
                    "vehicle_name": str(row[35]),
                    "vehicle_image": str(row[36]),
                    "ratings": str(row[37]),
                    "rating_description": str(row[38]),
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def cab_driver_whole_year_earnings(request):
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
                SELECT
                months.month_index,
                COALESCE(SUM(e.amount), 0.0) AS total_earnings
            FROM (
                SELECT 1 AS month_index UNION ALL
                SELECT 2 UNION ALL
                SELECT 3 UNION ALL
                SELECT 4 UNION ALL
                SELECT 5 UNION ALL
                SELECT 6 UNION ALL
                SELECT 7 UNION ALL
                SELECT 8 UNION ALL
                SELECT 9 UNION ALL
                SELECT 10 UNION ALL
                SELECT 11 UNION ALL
                SELECT 12
            ) AS months
            LEFT JOIN vtpartner.cab_driver_earningstbl e
                ON EXTRACT(MONTH FROM e.earning_date) = months.month_index
                AND e.driver_id = %s
            GROUP BY months.month_index
            ORDER BY months.month_index;

            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            earning_details = [
                {
                    "month_index": row[0],
                    "total_earnings": row[1],
                   
                }
                for row in result
            ]

            return JsonResponse({"results": earning_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def cab_driver_todays_earnings(request):
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
            # Query to get today's earnings and rides count
            query = """
                SELECT COALESCE(SUM(amount), 0) AS todays_earnings, 
                       COUNT(*) AS todays_rides 
                FROM vtpartner.cab_driver_earningstbl 
                WHERE driver_id = %s AND earning_date = CURRENT_DATE;
            """
            result = select_query(query, [driver_id])  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Extract the first row from the result
            row = result[0]
            earning_details = {
                "todays_earnings": row[0],
                "todays_rides": row[1],
            }

            return JsonResponse({"results": [earning_details]}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def generate_new_cab_drivers_booking_id_get_nearby_drivers_with_fcm_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        # lat = data.get("lat")
        # lng = data.get("lng")
        # city_id = data.get("city_id")
        price_type = data.get("price_type", 1)
        radius_km = data.get("radius_km", 5)  # Radius in kilometers
        vehicle_id = data.get("vehicle_id")  # Vehicle ID
        # Read the individual fields from the JSON data
        customer_id = data.get("customer_id")
        pickup_lat = data.get("pickup_lat")
        pickup_lng = data.get("pickup_lng")
        destination_lat = data.get("destination_lat")
        destination_lng = data.get("destination_lng")
        distance = data.get("distance")
        time = data.get("time")
        total_price = data.get("total_price")
        base_price = data.get("base_price")
        otp = random.randint(1000, 9999)  # Generate a random 4-digit OTP
        gst_amount = data.get("gst_amount")
        igst_amount = data.get("igst_amount")
        
        payment_method = data.get("payment_method")
        city_id = data.get("city_id")
        pickup_address = data.get("pickup_address")
        drop_address = data.get("drop_address")
        server_access_token = data.get("server_access_token")

        # List of required fields
        required_fields = {
            "city_id":city_id,
            "price_type":price_type,
            "radius_km":radius_km,
            "vehicle_id":vehicle_id,
            "customer_id":customer_id,
            "pickup_lat":pickup_lat,
            "pickup_lng":pickup_lng,
            "destination_lat":destination_lat,
            "destination_lng":destination_lng,
            "distance":distance,
            "time":time,
            "total_price":total_price,
            "base_price":base_price,
            "otp":str(otp),
            "gst_amount":gst_amount,
            "igst_amount":igst_amount,
            
            "payment_method":payment_method,
            "city_id":city_id,
            "pickup_address":pickup_address,
            "drop_address":drop_address,
            "server_access_token":server_access_token,
        }

        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        

        
        
        
        

        if pickup_lat is None or pickup_lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
            
            # Insert record in the booking table
            query_insert = """
                INSERT INTO vtpartner.cab_bookings_tbl (
                    customer_id, driver_id, pickup_lat, pickup_lng, destination_lat, destination_lng, 
                    distance, time, total_price, base_price, booking_timing, booking_date, 
                    otp, gst_amount, igst_amount, 
                    payment_method, city_id,pickup_address,drop_address
                ) 
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE,  %s, %s, %s, 
                    %s, %s,%s, %s
                ) 
                RETURNING booking_id;
            """

            insert_values = [
                customer_id, '-1', pickup_lat, pickup_lng, destination_lat, destination_lng, 
                distance, time, total_price, base_price, otp, 
                gst_amount, igst_amount, payment_method, city_id,pickup_address,drop_address
            ]

            # Assuming insert_query is a function that runs the query
            new_result = insert_query(query_insert, insert_values)
            
            if new_result:
                booking_id = new_result[0][0]  # Extracting booking_id from the result
                response_value = [{"booking_id": booking_id}]
                
                #send notification to all goods driver
                fcm_data = {
                    'intent':'cab_driver',
                    'booking_id':str(booking_id)
                }
                query = """
                    SELECT 
                    main.active_id, 
                    main.cab_driver_id, 
                    main.current_lat, 
                    main.current_lng, 
                    main.entry_time, 
                    main.current_status, 
                    cab_driverstbl.driver_first_name,
                    cab_driverstbl.profile_pic, 
                    vehiclestbl.image AS vehicle_image, 
                    vehiclestbl.vehicle_name,
                    vehiclestbl.weight,
                    vehicle_city_wise_price_tbl.starting_price_per_km,
                    vehicle_city_wise_price_tbl.base_fare,
                    vehiclestbl.vehicle_id,
                    vehiclestbl.size_image,
                    cab_driverstbl.authtoken,
                    (6371 * acos(
                        cos(radians(%s)) * cos(radians(main.current_lat)) *
                        cos(radians(main.current_lng) - radians(%s)) +
                        sin(radians(%s)) * sin(radians(main.current_lat))
                    )) AS distance
                FROM vtpartner.active_cab_drivertbl AS main
                INNER JOIN (
                    SELECT cab_driver_id, MAX(entry_time) AS max_entry_time
                    FROM vtpartner.active_cab_drivertbl
                    GROUP BY cab_driver_id
                ) AS latest ON main.cab_driver_id = latest.cab_driver_id
                            AND main.entry_time = latest.max_entry_time
                JOIN vtpartner.cab_driverstbl ON main.cab_driver_id = cab_driverstbl.cab_driver_id
                JOIN vtpartner.vehiclestbl ON cab_driverstbl.vehicle_id = vehiclestbl.vehicle_id
                JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
                AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
                WHERE main.current_status = 1
                AND (6371 * acos(
                        cos(radians(%s)) * cos(radians(main.current_lat)) *
                        cos(radians(main.current_lng) - radians(%s)) +
                        sin(radians(%s)) * sin(radians(main.current_lat))
                    )) <= %s
                AND cab_driverstbl.category_id = vehiclestbl.category_id
                AND cab_driverstbl.category_id = '2' AND  cab_driverstbl.vehicle_id=%s
                ORDER BY distance ASC;

                """
                values = [pickup_lat, pickup_lng, pickup_lat,city_id,price_type, pickup_lat, pickup_lng, pickup_lat, radius_km,vehicle_id]

                # Execute the query
                nearby_drivers = select_query(query, values)
                

                # Format response
                # drivers_list = [
                #     {
                #         "active_id": driver[0],
                #         "goods_driver_id": driver[1],
                #         "latitude": driver[2],
                #         "longitude": driver[3],
                #         "entry_time": driver[4],
                #         "current_status": driver[5],
                #         "driver_name": driver[6],
                #         "driver_profile_pic": driver[7],
                #         "vehicle_image": driver[8],
                #         "vehicle_name": driver[9],
                #         "weight": driver[10],
                #         "starting_price_per_km": driver[11],
                #         "base_fare": driver[12],
                #         "vehicle_id": driver[13],
                #         "size_image": driver[14],
                #         "auth_token": driver[15],
                #         "distance": driver[16]
                #     }
                #     for driver in nearby_drivers
                # ]

                # Send notifications to all the online drivers
                # for driver in nearby_drivers:
                #     driver_auth_token = get_goods_driver_auth_token(driver[1])
                #     sendFMCMsg(
                #         driver_auth_token,
                #         f'You have a new Ride Request for \nPickup Location: {pickup_address}. \nDrop Location: {drop_address}',
                #         'New Goods Ride Request',
                #         fcm_data,
                #         server_access_token
                #     )
                for driver in nearby_drivers:
                    try:
                        
                        driver_auth_token = get_cab_driver_auth_token2(driver[1])  # driver[1] assumed to be goods_driver_id
                        print(f"driver_auth_token ->{driver[1]} {driver_auth_token}")
                        
                        if driver_auth_token:
                            sendFMCMsg(
                                driver_auth_token,
                                f"You have a new Ride Request for \nPickup Location: {pickup_address}. \nDrop Location: {drop_address}",
                                "New Cab Ride Request",
                                fcm_data,
                                server_access_token
                            )
                            print(f"Notification sent to cab driver ID {driver[1]}")
                        else:
                            print(f"Skipped notification for cab driver ID {driver[1]} due to missing auth token")
                    except Exception as err:
                        print(f"Error sending notification to cab driver ID {driver[1]}: {err}")


                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)



#All Driver Api's
@csrf_exempt
def other_driver_login_view(request):
    if request.method == "POST":
        data = json.loads(request.body)
        mobile_no = data.get("mobile_no")

         # List of required fields
        required_fields = {
            "mobile_no": mobile_no,
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
                
                
        try:
            query = """
            SELECT other_driver_id,driver_first_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,city_id FROM
            vtpartner.other_driverstbl WHERE mobile_no=%s
            """
            params = [mobile_no]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                try:
                    #Insert if not found
                    query = """
                        INSERT INTO vtpartner.other_driverstbl (
                            mobile_no
                        ) VALUES (%s) RETURNING other_driver_id
                    """
                    values = [mobile_no]
                    new_result = insert_query(query, values)
                    print("new_result::",new_result)
                    if new_result:
                        print("new_result[0][0]::",new_result[0][0])
                        other_driver_id = new_result[0][0]
                        response_value = [
                            {
                                "other_driver_id":other_driver_id
                            }
                        ]
                        return JsonResponse({"result": response_value}, status=200)
                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
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
                    "full_address": row[13],
                    "city_id": row[14],
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def other_driver_registration(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        other_driver_id = data.get("other_driver_id")
        driver_first_name = data.get("driver_first_name")
        profile_pic = data.get("profile_pic")
        mobile_no = data.get("mobile_no")
        r_lat = data.get("r_lat")
        r_lng = data.get("r_lng")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")
        recent_online_pic = data.get("recent_online_pic")
        city_id = data.get("city_id")
        aadhar_no = data.get("aadhar_no")
        pan_card_no = data.get("pan_card_no")
        full_address = data.get("full_address")
        gender = data.get("gender")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
        sub_cat_id = data.get("sub_cat_id")
        service_id = data.get("service_id")
        
        
        
        
        
        # List of required fields
        required_fields = {
            "other_driver_id":other_driver_id,
            "driver_first_name":driver_first_name,
            "profile_pic":profile_pic,
            "mobile_no":mobile_no,
            "r_lat":r_lat,
            "r_lng":r_lng,
            "current_lat":current_lat,
            "current_lng":current_lng,
            "recent_online_pic":recent_online_pic,
            "city_id":city_id,
            "aadhar_no":aadhar_no,
            "pan_card_no":pan_card_no,
            "full_address":full_address,
            "gender":gender,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            "license_front":license_front,
            "license_back":license_back,
            "sub_cat_id":sub_cat_id,
            "service_id":service_id,
           
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
            UPDATE vtpartner.other_driverstbl 
            SET 
            driver_first_name = %s,
            profile_pic = %s,
            mobile_no = %s,
            r_lat = %s,
            r_lng = %s,
            current_lat = %s,
            current_lng = %s,
            recent_online_pic = %s,
            category_id = %s,
            city_id = %s,
            aadhar_no = %s,
            pan_card_no = %s,
            full_address = %s,
            gender = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            license_front = %s,
            license_back = %s,
            sub_cat_id = %s,
            service_id = %s
            
            WHERE other_driver_id=%s
        """
        values = [
            driver_first_name,
            profile_pic,
            mobile_no,
            r_lat,
            r_lng,
            r_lat,
            r_lng,
            recent_online_pic,
            '4',
            city_id,
            aadhar_no,
            pan_card_no,
            full_address,
            gender,
            aadhar_card_front,
            aadhar_card_back,
            pan_card_front,
            pan_card_back,
            license_front,
            license_back,
            sub_cat_id,
            service_id,
            other_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)

@csrf_exempt
def other_driver_aadhar_details_update(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        other_driver_id = data.get("other_driver_id")
        aadhar_no = data.get("aadhar_no")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "other_driver_id":other_driver_id,
            "aadhar_no":aadhar_no,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            aadhar_no = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            WHERE other_driver_id=%s
        """
        values = [
            aadhar_no,
            aadhar_card_front,
            aadhar_card_back,
            other_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def other_driver_pan_card_details_update(request):
    try:
        data = json.loads(request.body)

        other_driver_id = data.get("other_driver_id")
        pan_card_no = data.get("pan_card_no")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "other_driver_id":other_driver_id,
            "pan_card_no":pan_card_no,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            pan_card_no = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            WHERE other_driver_id=%s
        """
        values = [
            pan_card_no,
            pan_card_front,
            pan_card_back,
            other_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def other_driver_driving_license_details_update(request):
    try:
        data = json.loads(request.body)

        other_driver_id = data.get("other_driver_id")
        driving_license_no = data.get("driving_license_no")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "other_driver_id":other_driver_id,
            "driving_license_no":driving_license_no,
            "license_front":license_front,
            "license_back":license_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            driving_license_no = %s,
            license_front = %s,
            license_back = %s,
            WHERE other_driver_id=%s
        """
        values = [
            driving_license_no,
            license_front,
            license_back,
            other_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def other_driver_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        other_driver_id = data.get("other_driver_id")

         # List of required fields
        required_fields = {
            "other_driver_id": other_driver_id,
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
                
                
        try:
            query = """
            select is_online,status,driver_first_name,recent_online_pic,profile_pic,mobile_no from vtpartner.other_driverstbl where other_driver_id=%s
            """
            params = [other_driver_id]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "is_online": row[0],
                    "status": row[1],  
                    "driver_first_name": row[2],  
                    "recent_online_pic": row[3],  
                    "profile_pic": row[4],  
                    "mobile_no": row[5],  
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def other_driver_update_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        other_driver_id = data.get("other_driver_id")
        recent_online_pic = data.get("recent_online_pic")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "status": status,
            "other_driver_id": other_driver_id,
            "lat": lat,
            "lng": lng
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

            if status == 1:
                # Include recent_online_pic in the query when status is 1
                query = """
                UPDATE vtpartner.other_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s,
                    recent_online_pic = %s
                WHERE other_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    recent_online_pic,
                    other_driver_id
                ]
                

            else:
                # Exclude recent_online_pic when status is not 1
                query = """
                UPDATE vtpartner.other_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s
                WHERE other_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    other_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)
            
            #insert record in attendance table
            query_insert = """
                    INSERT INTO vtpartner.other_driver_attendance_tbl(driver_id, time, date, status) 
                    VALUES (%s, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE, %s)
                """

            insert_values = [
                other_driver_id,
                status
            ]
            
            row_count = insert_query(query_insert,insert_values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_other_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        other_driver_id = data.get("other_driver_id")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")

         # List of required fields
        required_fields = {
            "status": status,
            "other_driver_id": other_driver_id,
            "current_lat": current_lat,
            "current_lng": current_lng,
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
                
                
        try:
            query = """
            INSERT INTO vtpartner.active_other_drivertbl 
            (other_driver_id,current_lat,current_lng,current_status)
            VALUES (%s,%s,%s,%s) RETURNING active_id
            """
            values = [
                other_driver_id,
                current_lat,
                current_lng,
                status,
            ]
            new_result = insert_query(query, values)
            print("new_result::",new_result)
            if new_result:
                print("new_result[0][0]::",new_result[0][0])
                active_id = new_result[0][0]
                response_value = [
                    {
                        "active_id":active_id
                    }
                ]
                # Send success response
                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_other_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        other_driver_id = data.get("other_driver_id")
        

         # List of required fields
        required_fields = {
            "other_driver_id": other_driver_id,
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
                
                
        try:
            query = """
            DELETE FROM vtpartner.active_other_drivertbl 
            WHERE other_driver_id=%s
            """
            values = [
                other_driver_id,
            ]
            row_count = delete_query(query, values)
            
            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_other_drivers_current_location(request):
    if request.method == "POST":
        data = json.loads(request.body)
        other_driver_id = data.get("other_driver_id")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "other_driver_id": other_driver_id,
            "lat": lat,
            "lng": lng
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
                UPDATE vtpartner.active_other_drivertbl 
                SET 
                    current_lat = %s,
                    current_lng = %s
                WHERE other_driver_id = %s
                """
            values = [
                    lat,
                    lng,
                    other_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt
def get_nearby_other_drivers(request):
    if request.method == "POST":
        data = json.loads(request.body)
        lat = data.get("lat")
        lng = data.get("lng")
        city_id = data.get("city_id")
        price_type = data.get("price_type", 1)
        radius_km = data.get("radius_km", 5)  # Radius in kilometers

        if lat is None or lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
            # Haversine formula to calculate the distance in kilometers
            #SELECT main.active_id, main.other_driver_id, main.current_lat, main.current_lng, 
#        main.entry_time, main.current_status, other_driverstbl.driver_first_name,
#        other_driverstbl.profile_pic, vehiclestbl.image AS vehicle_image, 
#        vehiclestbl.vehicle_name,weight,
#        (6371 * acos(
#            cos(radians(%s)) * cos(radians(main.current_lat)) *
#            cos(radians(main.current_lng) - radians(%s)) +
#            sin(radians(%s)) * sin(radians(main.current_lat))
#        )) AS distance
# FROM vtpartner.active_other_drivertbl AS main
# INNER JOIN (
#     SELECT other_driver_id, MAX(entry_time) AS max_entry_time
#     FROM vtpartner.active_other_drivertbl
#     GROUP BY other_driver_id
# ) AS latest ON main.other_driver_id = latest.other_driver_id
#               AND main.entry_time = latest.max_entry_time
# JOIN vtpartner.other_driverstbl ON main.other_driver_id = other_driverstbl.other_driver_id
# JOIN vtpartner.vehiclestbl ON other_driverstbl.vehicle_id = vehiclestbl.vehicle_id
# WHERE main.current_status = 1
#   AND (6371 * acos(
#          cos(radians(%s)) * cos(radians(main.current_lat)) *
#          cos(radians(main.current_lng) - radians(%s)) +
#          sin(radians(%s)) * sin(radians(main.current_lat))
#      )) <= %s
#   AND other_driverstbl.category_id = vehiclestbl.category_id
#   AND other_driverstbl.category_id = '1'
# ORDER BY distance;
#             """
#             values = [lat, lng, lat, lat, lng, lat, radius_km]

            query = """
            SELECT 
    main.active_id, 
    main.other_driver_id, 
    main.current_lat, 
    main.current_lng, 
    main.entry_time, 
    main.current_status, 
    other_driverstbl.driver_first_name,
    other_driverstbl.profile_pic, 
    vehiclestbl.image AS vehicle_image, 
    vehiclestbl.vehicle_name,
    vehiclestbl.weight,
    vehicle_city_wise_price_tbl.starting_price_per_km,
    vehicle_city_wise_price_tbl.base_fare,
    vehiclestbl.vehicle_id,
    vehiclestbl.size_image,
    (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
    )) AS distance
FROM vtpartner.active_other_drivertbl AS main
INNER JOIN (
    SELECT other_driver_id, MAX(entry_time) AS max_entry_time
    FROM vtpartner.active_other_drivertbl
    GROUP BY other_driver_id
) AS latest ON main.other_driver_id = latest.other_driver_id
             AND main.entry_time = latest.max_entry_time
JOIN vtpartner.other_driverstbl ON main.other_driver_id = other_driverstbl.other_driver_id
JOIN vtpartner.vehiclestbl ON other_driverstbl.vehicle_id = vehiclestbl.vehicle_id
JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
WHERE main.current_status = 1
  AND (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
      )) <= %s
  AND other_driverstbl.category_id = vehiclestbl.category_id
  AND other_driverstbl.category_id = '1'
ORDER BY distance;

            """
            values = [lat, lng, lat,city_id,price_type, lat, lng, lat, radius_km]

            # Execute the query
            nearby_drivers = select_query(query, values)
            

            # Format response
            drivers_list = [
                {
                    "active_id": driver[0],
                    "other_driver_id": driver[1],
                    "latitude": driver[2],
                    "longitude": driver[3],
                    "entry_time": driver[4],
                    "current_status": driver[5],
                    "driver_name": driver[6],
                    "driver_profile_pic": driver[7],
                    "vehicle_image": driver[8],
                    "vehicle_name": driver[9],
                    "weight": driver[10],
                    "starting_price_per_km": driver[11],
                    "base_fare": driver[12],
                    "vehicle_id": driver[13],
                    "size_image": driver[14],
                    "distance": driver[15]
                }
                for driver in nearby_drivers
            ]

            return JsonResponse({"nearby_drivers": drivers_list}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)




@csrf_exempt
def update_firebase_other_driver_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        other_driver_id = data.get("other_driver_id")
        authToken = data.get("authToken")
        

        # List of required fields
        required_fields = {
            "other_driver_id": other_driver_id,
            "authToken": authToken,
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
                UPDATE vtpartner.other_driverstbl 
                SET 
                    authtoken = %s
                WHERE other_driver_id = %s
                """
            values = [
                    authToken,
                    other_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def jcb_crane_booking_details_for_ride_acceptance(request):
    if request.method == "POST":
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
            
        try:
            query = """
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,customer_name,customers_tbl.authtoken,pickup_address,drop_address from vtpartner.bookings_tbl,vtpartner.customers_tbl where customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "customer_name": row[28],
                    "customers_auth_token": row[29],
                    "pickup_address": row[30],
                    "drop_address": row[31],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def other_driver_booking_accepted(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "server_token": server_token,
            "customer_id":customer_id
        
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
                select driver_id from vtpartner.bookings_tbl where booking_id=%s and driver_id!='-1'
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                #Update booking status and driver assinged
                try:

                    query = """
                       update vtpartner.bookings_tbl set driver_id=%s ,booking_status='Driver Accepted' where booking_id=%s
                        """
                    values = [
                            driver_id,
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(query, values)
                    #Inserting record in booking_history table
                    try:

                        query = """
                           insert into vtpartner.bookings_history_tbl (status,booking_id) values ('Driver Accepted',%s)
                            """
                        values = [
                                booking_id
                            ]

                        # Execute the query
                        row_count = insert_query(query, values)
                        #Updating driver status to occupied
                        try:

                            query = """
                               update vtpartner.active_other_drivertbl set current_status='2' where other_driver_id=%s
                                """
                            values = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query, values)



                            #get the customer auth token
                            auth_token = get_customer_auth_token(customer_id)
                            
                            #send Fcm notification to customer saying driver assigned
                            customer_data = {
                                'intent':'live_tracking',
                                'booking_id':str(booking_id)
                            }
                            sendFMCMsg(auth_token,'You have been assigned a driver','Driver Assigned',customer_data,server_token)

                            # Send success response
                            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                        
                        
                        

                    except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500)

                    

                except Exception as err:
                    print("Error executing updating booking status to accepted:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)

            # Checking if driver is assigned
            ret_driver_id = result[0][0]
            return JsonResponse({"message": "No Data Found"}, status=404)
            
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def update_booking_status_other_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "booking_status": booking_status,
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
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP Verified":
                    body = "You're OTP is Verified Successfully!"
                    title = "OTP Verification"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                    # Update Pickup epoch here
                    update_pickup_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET pickup_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_pickup_epoch_query, values)
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                    # Update Drop epoch here
                    update_drop_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET drop_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_drop_epoch_query, values)
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)
    
@csrf_exempt 
def generate_order_id_for_booking_id_other_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount")
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "booking_status": booking_status,
            "server_token": server_token,
            "customer_id": customer_id,
            "total_amount": total_amount,
            
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        #Updating the Status to Trip Ended
        
        try:

            query = """
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP verified":
                    body = "Your trip otp is verified"
                    title = "Trip OTP Verified"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                elif booking_status == "Ongoing":
                    body = "Trip has been started from your pickup location"
                    title = "Ongoing"
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
                
                #Generating Order ID
                try:
                    query = """
                    INSERT INTO vtpartner.orders_tbl (
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    )
                    SELECT 
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    FROM vtpartner.bookings_tbl
                    WHERE booking_id = %s
                    RETURNING order_id;
                    """

                    

                    # Execute the query
                    ret_result = insert_query2(query,[booking_id])
                    #get order_id from here
                    if ret_result!=None:
                        order_id = ret_result[0][0]
                        try:
                            query2 = """
                            update vtpartner.active_other_drivertbl set current_status='1' where other_driver_id=%s
                            """
                            values2 = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query2, values2)
                            #success
                            try:
                                query3 = """
                                update vtpartner.bookings_tbl set booking_completed='1' where booking_id=%s
                                """
                                values3 = [
                                        booking_id
                                    ]

                                # Execute the query
                                row_count = update_query(query3, values3)
                                
                                query_update = """
                                update vtpartner.orders_tbl set payment_method=%s,payment_id=%s where order_id=%s
                                """
                                values_update = [
                                        payment_method,
                                        payment_id,
                                        order_id
                                    ]

                                # Execute the query
                                row_count = update_query(query_update, values_update)
                                
                                #Adding the amount to driver earnings table
                                try:
                                    query4 = """
                                    insert into vtpartner.other_driver_earningstbl(driver_id,amount,order_id,payment_id,payment_mode) values (%s,%s,%s,%s,%s)
                                    """
                                    values4 = [
                                            driver_id,
                                            total_amount,
                                            order_id,
                                            payment_id,
                                            payment_method
                                        ]

                                    # Execute the query
                                    row_count = insert_query(query4, values4)
                                    #success
                                    #Adding the amount to driver earnings table
                                    try:
                                        query5 = """
                                            UPDATE vtpartner.other_driver_topup_recharge_current_points_tbl
                                            SET 
                                                used_points = used_points + %s,
                                                remaining_points = CASE 
                                                    WHEN remaining_points >= %s THEN remaining_points - %s
                                                    ELSE 0
                                                END,
                                                negative_points = CASE
                                                    WHEN remaining_points < %s THEN negative_points + (%s - remaining_points)
                                                    ELSE negative_points
                                                END,
                                                last_updated_time = date_part('epoch'::text, CURRENT_TIMESTAMP)
                                            WHERE driver_id = %s
                                        """
                                        values5 = [
                                                total_amount,  # %s for updating used_points
                                                total_amount,  # %s for checking remaining_points
                                                total_amount,  # %s for deducting from remaining_points
                                                total_amount,  # %s for checking if negative_points should be updated
                                                total_amount,  # %s for calculating difference for negative_points
                                                driver_id          # %s for identifying the driver
                                            ]

                                        # Execute the query
                                        row_count = insert_query(query5, values5)
                                        #success
                                        return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                                    except Exception as err:
                                        print("Error executing query:", err)
                                        return JsonResponse({"message": "An error occurred"}, status=500)
                                except Exception as err:
                                    print("Error executing query:", err)
                                    return JsonResponse({"message": "An error occurred"}, status=500)
                                
                                    #success
                                    #return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                            except Exception as err:
                                print("Error executing query:", err)
                                return JsonResponse({"message": "An error occurred"}, status=500)
                            
                            
                            # return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                    

                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
        
            except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500) 
        

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_other_driver_recharge_list(request):
    if request.method == "POST":
        data = json.loads(request.body)
        category_id = data.get("category_id")
        

        # List of required fields
        required_fields = {
            "category_id": category_id,
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
               select recharge_id,amount,points,status,description,valid_days from vtpartner.recharge_plans_tbl where category_id=%s
                ORDER BY 
                    amount ASC
            """
            result = select_query(query,[category_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "recharge_id": row[0],
                    "amount": row[1],
                    "points": row[2],
                    "status": row[3],
                    "description": row[4],
                    "valid_days": row[5]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_other_driver_current_recharge_details(request):
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
               select topup_id,recharge_id,allotted_points,used_points,remaining_points,negative_points,valid_till_date,status from vtpartner.other_driver_topup_recharge_current_points_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "topup_id": row[0],
                    "recharge_id": row[1],
                    "allotted_points": row[2],
                    "used_points": row[3],
                    "remaining_points": row[4],
                    "negative_points": row[5],
                    "valid_till_date": row[6],
                    "status": row[7]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def get_other_driver_recharge_history_details(request):
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
               select history_id,recharge_id,amount,allotted_points,date,valid_till_date,status,payment_method,payment_id,transaction_type,admin_id,last_recharge_negative_points from vtpartner.other_driver_topup_recharge_history_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "history_id": row[0],
                    "recharge_id": row[1],
                    "amount": row[2],
                    "allotted_points": row[3],
                    "date": row[4],
                    "valid_till_date": row[5],
                    "status": row[6],
                    "payment_method": row[7],
                    "payment_id": row[8],
                    "transaction_type": row[9],
                    "admin_id": row[10],
                    "last_recharge_negative_points": row[11],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def new_other_driver_recharge(request):
    if request.method == "POST":
        data = json.loads(request.body)
        driver_id = data.get("driver_id")
        topup_id = data.get("topup_id")
        recharge_id = data.get("recharge_id")
        amount = data.get("amount")
        allotted_points = data.get("allotted_points")
        valid_till_date = data.get("valid_till_date")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        negative_points = data.get("previous_negative_points")
        last_validity_date = data.get("last_validity_date")

        # List of required fields
        required_fields = {
            "driver_id": driver_id,
            "topup_id": topup_id,
            "recharge_id": recharge_id,
            "amount": amount,
            "allotted_points": allotted_points,
            "valid_till_date": valid_till_date,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "previous_negative_points": negative_points,
            "last_validity_date": last_validity_date,
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        if negative_points > 0:
            allotted_points -= negative_points
        try:
            query = """
                insert into vtpartner.other_driver_topup_recharge_history_tbl(driver_id,recharge_id,amount,allotted_points,valid_till_date,payment_method,payment_id,last_recharge_negative_points) values (%s,%s,%s,%s,%s,%s,%s,%s)
                """
            values = [
                    driver_id,
                    recharge_id,
                    amount,
                    allotted_points,
                    valid_till_date,
                    payment_method,
                    payment_id,
                    negative_points
                ]
            # Execute the query
            row_count = insert_query(query, values)
            
            #checking if recharge has been expired
            last_validity_date_obj = datetime.strptime(last_validity_date, "%Y-%m-%d")

            # Get the current date
            current_date = datetime.now()

            # Check if last_validity_date is greater than the current date
            isExpired = False
            if last_validity_date_obj > current_date:
                print("The last validity date is in the future.")
            else:
                isExpired = True
                print("The last validity date is today or has passed.")

            # Updating Booking History Table
            try:
                if negative_points > 0 or isExpired:
                    query = """
                    update vtpartner.other_driver_topup_recharge_current_points_tbl set recharge_id=%s,allotted_points=%s,valid_till_date=%s,remaining_points=%s,negative_points='0',used_points='0' where topup_id=%s and driver_id=%s
                    """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            allotted_points,
                            topup_id,
                            driver_id
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)
                else:
                    query = """
                        INSERT INTO vtpartner.other_driver_topup_recharge_current_points_tbl (recharge_id,allotted_points,valid_till_date,driver_id,remaining_points) VALUES (%s,%s,%s,%s,%s)
                        """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            driver_id,
                            allotted_points
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)

                
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def other_driver_all_orders(request):
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
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,other_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,other_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,orders_tbl.ratings,orders_tbl.rating_description from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.other_driverstbl,vtpartner.customers_tbl where other_driverstbl.other_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and orders_tbl.driver_id=%s and  vehiclestbl.vehicle_id=other_driverstbl.vehicle_id order by order_id desc
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "other_driver_auth_token": str(row[27]),
                    "customer_name": str(row[28]),
                    "customers_auth_token": str(row[29]),
                    "pickup_address": str(row[30]),
                    "drop_address": str(row[31]),
                    "customer_mobile_no": str(row[32]),
                    "driver_mobile_no": str(row[33]),
                    "vehicle_id": str(row[34]),
                    "vehicle_name": str(row[35]),
                    "vehicle_image": str(row[36]),
                    "ratings": str(row[37]),
                    "rating_description": str(row[38]),
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def other_driver_whole_year_earnings(request):
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
                SELECT
                months.month_index,
                COALESCE(SUM(e.amount), 0.0) AS total_earnings
            FROM (
                SELECT 1 AS month_index UNION ALL
                SELECT 2 UNION ALL
                SELECT 3 UNION ALL
                SELECT 4 UNION ALL
                SELECT 5 UNION ALL
                SELECT 6 UNION ALL
                SELECT 7 UNION ALL
                SELECT 8 UNION ALL
                SELECT 9 UNION ALL
                SELECT 10 UNION ALL
                SELECT 11 UNION ALL
                SELECT 12
            ) AS months
            LEFT JOIN vtpartner.other_driver_earningstbl e
                ON EXTRACT(MONTH FROM e.earning_date) = months.month_index
                AND e.driver_id = %s
            GROUP BY months.month_index
            ORDER BY months.month_index;

            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            earning_details = [
                {
                    "month_index": row[0],
                    "total_earnings": row[1],
                   
                }
                for row in result
            ]

            return JsonResponse({"results": earning_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def other_driver_todays_earnings(request):
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
            # Query to get today's earnings and rides count
            query = """
                SELECT COALESCE(SUM(amount), 0) AS todays_earnings, 
                       COUNT(*) AS todays_rides 
                FROM vtpartner.other_driver_earningstbl 
                WHERE driver_id = %s AND earning_date = CURRENT_DATE;
            """
            result = select_query(query, [driver_id])  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Extract the first row from the result
            row = result[0]
            earning_details = {
                "todays_earnings": row[0],
                "todays_rides": row[1],
            }

            return JsonResponse({"results": [earning_details]}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


#JCB Crane Driver Api's
@csrf_exempt
def jcb_crane_driver_login_view(request):
    if request.method == "POST":
        data = json.loads(request.body)
        mobile_no = data.get("mobile_no")

         # List of required fields
        required_fields = {
            "mobile_no": mobile_no,
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
                
                
        try:
            query = """
            SELECT jcb_crane_driver_id,driver_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,city_id FROM
            vtpartner.jcb_crane_driverstbl WHERE mobile_no=%s
            """
            params = [mobile_no]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                try:
                    #Insert if not found
                    query = """
                        INSERT INTO vtpartner.jcb_crane_driverstbl (
                            mobile_no
                        ) VALUES (%s) RETURNING jcb_crane_driver_id
                    """
                    values = [mobile_no]
                    new_result = insert_query(query, values)
                    print("new_result::",new_result)
                    if new_result:
                        print("new_result[0][0]::",new_result[0][0])
                        jcb_crane_driver_id = new_result[0][0]
                        response_value = [
                            {
                                "jcb_crane_driver_id":jcb_crane_driver_id
                            }
                        ]
                        return JsonResponse({"result": response_value}, status=200)
                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "jcb_crane_driver_id": row[0],
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
                    "full_address": row[13],
                    "city_id": row[14],
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def jcb_crane_driver_registration(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        driver_first_name = data.get("driver_first_name")
        profile_pic = data.get("profile_pic")
        mobile_no = data.get("mobile_no")
        r_lat = data.get("r_lat")
        r_lng = data.get("r_lng")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")
        recent_online_pic = data.get("recent_online_pic")
        vehicle_id = data.get("vehicle_id")
        city_id = data.get("city_id")
        aadhar_no = data.get("aadhar_no")
        pan_card_no = data.get("pan_card_no")
        full_address = data.get("full_address")
        gender = data.get("gender")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
        insurance_image = data.get("insurance_image")
        noc_image = data.get("noc_image")
        pollution_certificate_image = data.get("pollution_certificate_image")
        rc_image = data.get("rc_image")
        vehicle_image = data.get("vehicle_image")
        vehicle_plate_image = data.get("vehicle_plate_image")
        driving_license_no = data.get("driving_license_no")
        vehicle_plate_no = data.get("vehicle_plate_no")
        rc_no = data.get("rc_no")
        insurance_no = data.get("insurance_no")
        noc_no = data.get("noc_no")
        vehicle_fuel_type = data.get("vehicle_fuel_type")
        owner_name = data.get("owner_name")
        owner_mobile_no = data.get("owner_mobile_no")
        owner_photo_url = data.get("owner_photo_url")
        owner_address = data.get("owner_address")
        owner_city_name = data.get("owner_city_name")
        sub_cat_id = data.get("sub_cat_id")
        service_id = data.get("service_id")
        
        
        
        
        # List of required fields
        required_fields = {
            "jcb_crane_driver_id":jcb_crane_driver_id,
            "driver_first_name":driver_first_name,
            "profile_pic":profile_pic,
            "mobile_no":mobile_no,
            "r_lat":r_lat,
            "r_lng":r_lng,
            "current_lat":current_lat,
            "current_lng":current_lng,
            "recent_online_pic":recent_online_pic,
            "vehicle_id":vehicle_id,
            "city_id":city_id,
            "aadhar_no":aadhar_no,
            "pan_card_no":pan_card_no,
            "full_address":full_address,
            "gender":gender,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            "license_front":license_front,
            "license_back":license_back,
            "insurance_image":insurance_image,
            "noc_image":noc_image,
            "pollution_certificate_image":pollution_certificate_image,
            "rc_image":rc_image,
            "vehicle_image":vehicle_image,
            "vehicle_plate_image":vehicle_plate_image,
            "driving_license_no":driving_license_no,
            "vehicle_plate_no":vehicle_plate_no,
            "rc_no":rc_no,
            "insurance_no":insurance_no,
            "noc_no":noc_no,
            "vehicle_fuel_type":vehicle_fuel_type,
            "owner_name":owner_name,
            "owner_mobile_no":owner_mobile_no,
            "owner_photo_url":owner_photo_url,
            "owner_address":owner_address,
            "owner_city_name":owner_city_name,
            "sub_cat_id":sub_cat_id,
            "service_id":service_id,
        }

        # Use the utility function to check for missing fields
        missing_fields = check_missing_fields(required_fields)

        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
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
                            owner_name, owner_mobile_no,  city_name, address, profile_photo
                        ) VALUES (%s, %s, %s,  %s, %s) RETURNING owner_id
                    """
                    owner_values = [owner_name, owner_mobile_no,  owner_city_name, owner_address, owner_photo_url]
                    new_owner_result = insert_query(insert_owner_query, owner_values)
                    if new_owner_result:
                        owner_id = new_owner_result[0][0]
                    else:
                        raise Exception("Failed to retrieve owner ID from insert operation")
            except Exception as error:
                print("Owner error::", error)
        print("owner_id::",owner_id)
        
        query = """
            UPDATE vtpartner.jcb_crane_driverstbl 
            SET 
            driver_name = %s,
            profile_pic = %s,
            mobile_no = %s,
            r_lat = %s,
            r_lng = %s,
            current_lat = %s,
            current_lng = %s,
            recent_online_pic = %s,
            category_id = %s,
            vehicle_id = %s,
            city_id = %s,
            aadhar_no = %s,
            pan_card_no = %s,
            full_address = %s,
            gender = %s,
            owner_id = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            license_front = %s,
            license_back = %s,
            insurance_image = %s,
            noc_image = %s,
            pollution_certificate_image = %s,
            rc_image = %s,
            vehicle_image = %s,
            vehicle_plate_image = %s,
            driving_license_no = %s,
            vehicle_plate_no = %s,
            rc_no = %s,
            insurance_no = %s,
            noc_no = %s,
            vehicle_fuel_type = %s,
            sub_cat_id = %s,
            service_id = %s
            WHERE jcb_crane_driver_id=%s
        """
        values = [
            driver_first_name,
            profile_pic,
            mobile_no,
            r_lat,
            r_lng,
            r_lat,
            r_lng,
            recent_online_pic,
            '3',
            vehicle_id,
            city_id,
            aadhar_no,
            pan_card_no,
            full_address,
            gender,
            owner_id,
            aadhar_card_front,
            aadhar_card_back,
            pan_card_front,
            pan_card_back,
            license_front,
            license_back,
            insurance_image,
            noc_image,
            pollution_certificate_image,
            rc_image,
            vehicle_image,
            vehicle_plate_image,
            driving_license_no,
            vehicle_plate_no,
            rc_no,
            insurance_no,
            noc_no,
            vehicle_fuel_type,
            sub_cat_id,
            service_id,
            jcb_crane_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)

@csrf_exempt
def jcb_crane_driver_aadhar_details_update(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        aadhar_no = data.get("aadhar_no")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "jcb_crane_driver_id":jcb_crane_driver_id,
            "aadhar_no":aadhar_no,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            aadhar_no = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            WHERE jcb_crane_driver_id=%s
        """
        values = [
            aadhar_no,
            aadhar_card_front,
            aadhar_card_back,
            jcb_crane_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def jcb_crane_driver_pan_card_details_update(request):
    try:
        data = json.loads(request.body)

        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        pan_card_no = data.get("pan_card_no")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "jcb_crane_driver_id":jcb_crane_driver_id,
            "pan_card_no":pan_card_no,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            pan_card_no = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            WHERE jcb_crane_driver_id=%s
        """
        values = [
            pan_card_no,
            pan_card_front,
            pan_card_back,
            jcb_crane_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def jcb_crane_driver_driving_license_details_update(request):
    try:
        data = json.loads(request.body)

        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        driving_license_no = data.get("driving_license_no")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "jcb_crane_driver_id":jcb_crane_driver_id,
            "driving_license_no":driving_license_no,
            "license_front":license_front,
            "license_back":license_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            driving_license_no = %s,
            license_front = %s,
            license_back = %s,
            WHERE jcb_crane_driver_id=%s
        """
        values = [
            driving_license_no,
            license_front,
            license_back,
            jcb_crane_driver_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def jcb_crane_driver_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")

         # List of required fields
        required_fields = {
            "jcb_crane_driver_id": jcb_crane_driver_id,
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
                
                
        try:
            query = """
            select is_online,status,driver_name,recent_online_pic,profile_pic,mobile_no from vtpartner.jcb_crane_driverstbl where jcb_crane_driver_id=%s
            """
            params = [jcb_crane_driver_id]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "is_online": row[0],
                    "status": row[1],  
                    "driver_first_name": row[2],  
                    "recent_online_pic": row[3],  
                    "profile_pic": row[4],  
                    "mobile_no": row[5],  
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def jcb_crane_driver_update_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        recent_online_pic = data.get("recent_online_pic")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "status": status,
            "jcb_crane_driver_id": jcb_crane_driver_id,
            "lat": lat,
            "lng": lng
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

            if status == 1:
                # Include recent_online_pic in the query when status is 1
                query = """
                UPDATE vtpartner.jcb_crane_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s,
                    recent_online_pic = %s
                WHERE jcb_crane_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    recent_online_pic,
                    jcb_crane_driver_id
                ]
                

            else:
                # Exclude recent_online_pic when status is not 1
                query = """
                UPDATE vtpartner.jcb_crane_driverstbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s
                WHERE jcb_crane_driver_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    jcb_crane_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)
            
            #insert record in attendance table
            query_insert = """
                    INSERT INTO vtpartner.jcb_crane_driver_attendance_tbl(driver_id, time, date, status) 
                    VALUES (%s, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE, %s)
                """

            insert_values = [
                jcb_crane_driver_id,
                status
            ]
            
            row_count = insert_query(query_insert,insert_values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_jcb_crane_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")

         # List of required fields
        required_fields = {
            "status": status,
            "jcb_crane_driver_id": jcb_crane_driver_id,
            "current_lat": current_lat,
            "current_lng": current_lng,
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
                
                
        try:
            query = """
            INSERT INTO vtpartner.active_jcb_crane_drivertbl 
            (jcb_crane_driver_id,current_lat,current_lng,current_status)
            VALUES (%s,%s,%s,%s) RETURNING active_id
            """
            values = [
                jcb_crane_driver_id,
                current_lat,
                current_lng,
                status,
            ]
            new_result = insert_query(query, values)
            print("new_result::",new_result)
            if new_result:
                print("new_result[0][0]::",new_result[0][0])
                active_id = new_result[0][0]
                response_value = [
                    {
                        "active_id":active_id
                    }
                ]
                # Send success response
                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_jcb_crane_driver_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        

         # List of required fields
        required_fields = {
            "jcb_crane_driver_id": jcb_crane_driver_id,
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
                
                
        try:
            query = """
            DELETE FROM vtpartner.active_jcb_crane_drivertbl 
            WHERE jcb_crane_driver_id=%s
            """
            values = [
                jcb_crane_driver_id,
            ]
            row_count = delete_query(query, values)
            
            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_jcb_crane_drivers_current_location(request):
    if request.method == "POST":
        data = json.loads(request.body)
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "jcb_crane_driver_id": jcb_crane_driver_id,
            "lat": lat,
            "lng": lng
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
                UPDATE vtpartner.active_jcb_crane_drivertbl 
                SET 
                    current_lat = %s,
                    current_lng = %s
                WHERE jcb_crane_driver_id = %s
                """
            values = [
                    lat,
                    lng,
                    jcb_crane_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt
def get_nearby_jcb_crane_drivers(request):
    if request.method == "POST":
        data = json.loads(request.body)
        lat = data.get("lat")
        lng = data.get("lng")
        city_id = data.get("city_id")
        price_type = data.get("price_type", 1)
        radius_km = data.get("radius_km", 5)  # Radius in kilometers

        if lat is None or lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
            # Haversine formula to calculate the distance in kilometers
            #SELECT main.active_id, main.jcb_crane_driver_id, main.current_lat, main.current_lng, 
#        main.entry_time, main.current_status, jcb_crane_driverstbl.driver_name,
#        jcb_crane_driverstbl.profile_pic, vehiclestbl.image AS vehicle_image, 
#        vehiclestbl.vehicle_name,weight,
#        (6371 * acos(
#            cos(radians(%s)) * cos(radians(main.current_lat)) *
#            cos(radians(main.current_lng) - radians(%s)) +
#            sin(radians(%s)) * sin(radians(main.current_lat))
#        )) AS distance
# FROM vtpartner.active_jcb_crane_drivertbl AS main
# INNER JOIN (
#     SELECT jcb_crane_driver_id, MAX(entry_time) AS max_entry_time
#     FROM vtpartner.active_jcb_crane_drivertbl
#     GROUP BY jcb_crane_driver_id
# ) AS latest ON main.jcb_crane_driver_id = latest.jcb_crane_driver_id
#               AND main.entry_time = latest.max_entry_time
# JOIN vtpartner.jcb_crane_driverstbl ON main.jcb_crane_driver_id = jcb_crane_driverstbl.jcb_crane_driver_id
# JOIN vtpartner.vehiclestbl ON jcb_crane_driverstbl.vehicle_id = vehiclestbl.vehicle_id
# WHERE main.current_status = 1
#   AND (6371 * acos(
#          cos(radians(%s)) * cos(radians(main.current_lat)) *
#          cos(radians(main.current_lng) - radians(%s)) +
#          sin(radians(%s)) * sin(radians(main.current_lat))
#      )) <= %s
#   AND jcb_crane_driverstbl.category_id = vehiclestbl.category_id
#   AND jcb_crane_driverstbl.category_id = '1'
# ORDER BY distance;
#             """
#             values = [lat, lng, lat, lat, lng, lat, radius_km]

            query = """
            SELECT 
    main.active_id, 
    main.jcb_crane_driver_id, 
    main.current_lat, 
    main.current_lng, 
    main.entry_time, 
    main.current_status, 
    jcb_crane_driverstbl.driver_name,
    jcb_crane_driverstbl.profile_pic, 
    vehiclestbl.image AS vehicle_image, 
    vehiclestbl.vehicle_name,
    vehiclestbl.weight,
    vehicle_city_wise_price_tbl.starting_price_per_km,
    vehicle_city_wise_price_tbl.base_fare,
    vehiclestbl.vehicle_id,
    vehiclestbl.size_image,
    (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
    )) AS distance
FROM vtpartner.active_jcb_crane_drivertbl AS main
INNER JOIN (
    SELECT jcb_crane_driver_id, MAX(entry_time) AS max_entry_time
    FROM vtpartner.active_jcb_crane_drivertbl
    GROUP BY jcb_crane_driver_id
) AS latest ON main.jcb_crane_driver_id = latest.jcb_crane_driver_id
             AND main.entry_time = latest.max_entry_time
JOIN vtpartner.jcb_crane_driverstbl ON main.jcb_crane_driver_id = jcb_crane_driverstbl.jcb_crane_driver_id
JOIN vtpartner.vehiclestbl ON jcb_crane_driverstbl.vehicle_id = vehiclestbl.vehicle_id
JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
WHERE main.current_status = 1
  AND (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
      )) <= %s
  AND jcb_crane_driverstbl.category_id = vehiclestbl.category_id
  AND jcb_crane_driverstbl.category_id = '1'
ORDER BY distance;

            """
            values = [lat, lng, lat,city_id,price_type, lat, lng, lat, radius_km]

            # Execute the query
            nearby_drivers = select_query(query, values)
            

            # Format response
            drivers_list = [
                {
                    "active_id": driver[0],
                    "jcb_crane_driver_id": driver[1],
                    "latitude": driver[2],
                    "longitude": driver[3],
                    "entry_time": driver[4],
                    "current_status": driver[5],
                    "driver_name": driver[6],
                    "driver_profile_pic": driver[7],
                    "vehicle_image": driver[8],
                    "vehicle_name": driver[9],
                    "weight": driver[10],
                    "starting_price_per_km": driver[11],
                    "base_fare": driver[12],
                    "vehicle_id": driver[13],
                    "size_image": driver[14],
                    "distance": driver[15]
                }
                for driver in nearby_drivers
            ]

            return JsonResponse({"nearby_drivers": drivers_list}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)




@csrf_exempt
def update_firebase_jcb_crane_driver_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        jcb_crane_driver_id = data.get("jcb_crane_driver_id")
        authToken = data.get("authToken")
        

        # List of required fields
        required_fields = {
            "jcb_crane_driver_id": jcb_crane_driver_id,
            "authToken": authToken,
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
                UPDATE vtpartner.jcb_crane_driverstbl 
                SET 
                    authtoken = %s
                WHERE jcb_crane_driver_id = %s
                """
            values = [
                    authToken,
                    jcb_crane_driver_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def other_driver_booking_details_for_ride_acceptance(request):
    if request.method == "POST":
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
            
        try:
            query = """
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,customer_name,customers_tbl.authtoken,pickup_address,drop_address from vtpartner.bookings_tbl,vtpartner.customers_tbl where customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "customer_name": row[28],
                    "customers_auth_token": row[29],
                    "pickup_address": row[30],
                    "drop_address": row[31],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def jcb_crane_driver_booking_accepted(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "server_token": server_token,
            "customer_id":customer_id
        
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
                select driver_id from vtpartner.bookings_tbl where booking_id=%s and driver_id!='-1'
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                #Update booking status and driver assinged
                try:

                    query = """
                       update vtpartner.bookings_tbl set driver_id=%s ,booking_status='Driver Accepted' where booking_id=%s
                        """
                    values = [
                            driver_id,
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(query, values)
                    #Inserting record in booking_history table
                    try:

                        query = """
                           insert into vtpartner.bookings_history_tbl (status,booking_id) values ('Driver Accepted',%s)
                            """
                        values = [
                                booking_id
                            ]

                        # Execute the query
                        row_count = insert_query(query, values)
                        #Updating driver status to occupied
                        try:

                            query = """
                               update vtpartner.active_jcb_crane_drivertbl set current_status='2' where jcb_crane_driver_id=%s
                                """
                            values = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query, values)



                            #get the customer auth token
                            auth_token = get_customer_auth_token(customer_id)
                            
                            #send Fcm notification to customer saying driver assigned
                            customer_data = {
                                'intent':'live_tracking',
                                'booking_id':str(booking_id)
                            }
                            sendFMCMsg(auth_token,'You have been assigned a driver','Driver Assigned',customer_data,server_token)

                            # Send success response
                            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                        
                        
                        

                    except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500)

                    

                except Exception as err:
                    print("Error executing updating booking status to accepted:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)

            # Checking if driver is assigned
            ret_driver_id = result[0][0]
            return JsonResponse({"message": "No Data Found"}, status=404)
            
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def update_booking_status_jcb_crane_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "booking_status": booking_status,
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
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP Verified":
                    body = "You're OTP is Verified Successfully!"
                    title = "OTP Verification"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                    # Update Pickup epoch here
                    update_pickup_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET pickup_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_pickup_epoch_query, values)
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                    # Update Drop epoch here
                    update_drop_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET drop_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_drop_epoch_query, values)
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)
    
@csrf_exempt 
def generate_order_id_for_booking_id_jcb_crane_driver(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount")
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "booking_status": booking_status,
            "server_token": server_token,
            "customer_id": customer_id,
            "total_amount": total_amount,
            
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        #Updating the Status to Trip Ended
        
        try:

            query = """
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP verified":
                    body = "Your trip otp is verified"
                    title = "Trip OTP Verified"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                elif booking_status == "Ongoing":
                    body = "Trip has been started from your pickup location"
                    title = "Ongoing"
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
                
                #Generating Order ID
                try:
                    query = """
                    INSERT INTO vtpartner.orders_tbl (
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    )
                    SELECT 
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    FROM vtpartner.bookings_tbl
                    WHERE booking_id = %s
                    RETURNING order_id;
                    """

                    

                    # Execute the query
                    ret_result = insert_query2(query,[booking_id])
                    #get order_id from here
                    if ret_result!=None:
                        order_id = ret_result[0][0]
                        try:
                            query2 = """
                            update vtpartner.active_jcb_crane_drivertbl set current_status='1' where jcb_crane_driver_id=%s
                            """
                            values2 = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query2, values2)
                            #success
                            try:
                                query3 = """
                                update vtpartner.bookings_tbl set booking_completed='1' where booking_id=%s
                                """
                                values3 = [
                                        booking_id
                                    ]

                                # Execute the query
                                row_count = update_query(query3, values3)
                                
                                query_update = """
                                update vtpartner.orders_tbl set payment_method=%s,payment_id=%s where order_id=%s
                                """
                                values_update = [
                                        payment_method,
                                        payment_id,
                                        order_id
                                    ]

                                # Execute the query
                                row_count = update_query(query_update, values_update)
                                
                                #Adding the amount to driver earnings table
                                try:
                                    query4 = """
                                    insert into vtpartner.jcb_crane_driver_earningstbl(driver_id,amount,order_id,payment_id,payment_mode) values (%s,%s,%s,%s,%s)
                                    """
                                    values4 = [
                                            driver_id,
                                            total_amount,
                                            order_id,
                                            payment_id,
                                            payment_method
                                        ]

                                    # Execute the query
                                    row_count = insert_query(query4, values4)
                                    #success
                                    #Adding the amount to driver earnings table
                                    try:
                                        query5 = """
                                            UPDATE vtpartner.jcb_crane_driver_topup_recharge_current_points_tbl
                                            SET 
                                                used_points = used_points + %s,
                                                remaining_points = CASE 
                                                    WHEN remaining_points >= %s THEN remaining_points - %s
                                                    ELSE 0
                                                END,
                                                negative_points = CASE
                                                    WHEN remaining_points < %s THEN negative_points + (%s - remaining_points)
                                                    ELSE negative_points
                                                END,
                                                last_updated_time = date_part('epoch'::text, CURRENT_TIMESTAMP)
                                            WHERE driver_id = %s
                                        """
                                        values5 = [
                                                total_amount,  # %s for updating used_points
                                                total_amount,  # %s for checking remaining_points
                                                total_amount,  # %s for deducting from remaining_points
                                                total_amount,  # %s for checking if negative_points should be updated
                                                total_amount,  # %s for calculating difference for negative_points
                                                driver_id          # %s for identifying the driver
                                            ]

                                        # Execute the query
                                        row_count = insert_query(query5, values5)
                                        #success
                                        return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                                    except Exception as err:
                                        print("Error executing query:", err)
                                        return JsonResponse({"message": "An error occurred"}, status=500)
                                except Exception as err:
                                    print("Error executing query:", err)
                                    return JsonResponse({"message": "An error occurred"}, status=500)
                                
                                    #success
                                    #return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                            except Exception as err:
                                print("Error executing query:", err)
                                return JsonResponse({"message": "An error occurred"}, status=500)
                            
                            
                            # return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                    

                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
        
            except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500) 
        

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_jcb_crane_driver_recharge_list(request):
    if request.method == "POST":
        data = json.loads(request.body)
        category_id = data.get("category_id")
        

        # List of required fields
        required_fields = {
            "category_id": category_id,
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
               select recharge_id,amount,points,status,description,valid_days from vtpartner.recharge_plans_tbl where category_id=%s
                ORDER BY 
                    amount ASC
            """
            result = select_query(query,[category_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "recharge_id": row[0],
                    "amount": row[1],
                    "points": row[2],
                    "status": row[3],
                    "description": row[4],
                    "valid_days": row[5]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_jcb_crane_driver_current_recharge_details(request):
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
               select topup_id,recharge_id,allotted_points,used_points,remaining_points,negative_points,valid_till_date,status from vtpartner.jcb_crane_driver_topup_recharge_current_points_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "topup_id": row[0],
                    "recharge_id": row[1],
                    "allotted_points": row[2],
                    "used_points": row[3],
                    "remaining_points": row[4],
                    "negative_points": row[5],
                    "valid_till_date": row[6],
                    "status": row[7]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def get_jcb_crane_driver_recharge_history_details(request):
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
               select history_id,recharge_id,amount,allotted_points,date,valid_till_date,status,payment_method,payment_id,transaction_type,admin_id,last_recharge_negative_points from vtpartner.jcb_crane_driver_topup_recharge_history_tbl where driver_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "history_id": row[0],
                    "recharge_id": row[1],
                    "amount": row[2],
                    "allotted_points": row[3],
                    "date": row[4],
                    "valid_till_date": row[5],
                    "status": row[6],
                    "payment_method": row[7],
                    "payment_id": row[8],
                    "transaction_type": row[9],
                    "admin_id": row[10],
                    "last_recharge_negative_points": row[11],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def new_jcb_crane_driver_recharge(request):
    if request.method == "POST":
        data = json.loads(request.body)
        driver_id = data.get("driver_id")
        topup_id = data.get("topup_id")
        recharge_id = data.get("recharge_id")
        amount = data.get("amount")
        allotted_points = data.get("allotted_points")
        valid_till_date = data.get("valid_till_date")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        negative_points = data.get("previous_negative_points")
        last_validity_date = data.get("last_validity_date")

        # List of required fields
        required_fields = {
            "driver_id": driver_id,
            "topup_id": topup_id,
            "recharge_id": recharge_id,
            "amount": amount,
            "allotted_points": allotted_points,
            "valid_till_date": valid_till_date,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "previous_negative_points": negative_points,
            "last_validity_date": last_validity_date,
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        if negative_points > 0:
            allotted_points -= negative_points
        try:
            query = """
                insert into vtpartner.jcb_crane_driver_topup_recharge_history_tbl(driver_id,recharge_id,amount,allotted_points,valid_till_date,payment_method,payment_id,last_recharge_negative_points) values (%s,%s,%s,%s,%s,%s,%s,%s)
                """
            values = [
                    driver_id,
                    recharge_id,
                    amount,
                    allotted_points,
                    valid_till_date,
                    payment_method,
                    payment_id,
                    negative_points
                ]
            # Execute the query
            row_count = insert_query(query, values)
            
            #checking if recharge has been expired
            last_validity_date_obj = datetime.strptime(last_validity_date, "%Y-%m-%d")

            # Get the current date
            current_date = datetime.now()

            # Check if last_validity_date is greater than the current date
            isExpired = False
            if last_validity_date_obj > current_date:
                print("The last validity date is in the future.")
            else:
                isExpired = True
                print("The last validity date is today or has passed.")

            # Updating Booking History Table
            try:
                if negative_points > 0 or isExpired:
                    query = """
                    update vtpartner.jcb_crane_driver_topup_recharge_current_points_tbl set recharge_id=%s,allotted_points=%s,valid_till_date=%s,remaining_points=%s,negative_points='0',used_points='0' where topup_id=%s and driver_id=%s
                    """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            allotted_points,
                            topup_id,
                            driver_id
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)
                else:
                    query = """
                        INSERT INTO vtpartner.jcb_crane_driver_topup_recharge_current_points_tbl (recharge_id,allotted_points,valid_till_date,driver_id,remaining_points) VALUES (%s,%s,%s,%s,%s)
                        """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            driver_id,
                            allotted_points
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)

                
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def jcb_crane_driver_all_orders(request):
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
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,jcb_crane_driverstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,jcb_crane_driverstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,orders_tbl.ratings,orders_tbl.rating_description from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.jcb_crane_driverstbl,vtpartner.customers_tbl where jcb_crane_driverstbl.jcb_crane_driver_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and orders_tbl.driver_id=%s and  vehiclestbl.vehicle_id=jcb_crane_driverstbl.vehicle_id order by order_id desc
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "jcb_crane_driver_auth_token": str(row[27]),
                    "customer_name": str(row[28]),
                    "customers_auth_token": str(row[29]),
                    "pickup_address": str(row[30]),
                    "drop_address": str(row[31]),
                    "customer_mobile_no": str(row[32]),
                    "driver_mobile_no": str(row[33]),
                    "vehicle_id": str(row[34]),
                    "vehicle_name": str(row[35]),
                    "vehicle_image": str(row[36]),
                    "ratings": str(row[37]),
                    "rating_description": str(row[38]),
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def jcb_crane_driver_whole_year_earnings(request):
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
                SELECT
                months.month_index,
                COALESCE(SUM(e.amount), 0.0) AS total_earnings
            FROM (
                SELECT 1 AS month_index UNION ALL
                SELECT 2 UNION ALL
                SELECT 3 UNION ALL
                SELECT 4 UNION ALL
                SELECT 5 UNION ALL
                SELECT 6 UNION ALL
                SELECT 7 UNION ALL
                SELECT 8 UNION ALL
                SELECT 9 UNION ALL
                SELECT 10 UNION ALL
                SELECT 11 UNION ALL
                SELECT 12
            ) AS months
            LEFT JOIN vtpartner.jcb_crane_driver_earningstbl e
                ON EXTRACT(MONTH FROM e.earning_date) = months.month_index
                AND e.driver_id = %s
            GROUP BY months.month_index
            ORDER BY months.month_index;

            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            earning_details = [
                {
                    "month_index": row[0],
                    "total_earnings": row[1],
                   
                }
                for row in result
            ]

            return JsonResponse({"results": earning_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def jcb_crane_driver_todays_earnings(request):
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
            # Query to get today's earnings and rides count
            query = """
                SELECT COALESCE(SUM(amount), 0) AS todays_earnings, 
                       COUNT(*) AS todays_rides 
                FROM vtpartner.jcb_crane_driver_earningstbl 
                WHERE driver_id = %s AND earning_date = CURRENT_DATE;
            """
            result = select_query(query, [driver_id])  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Extract the first row from the result
            row = result[0]
            earning_details = {
                "todays_earnings": row[0],
                "todays_rides": row[1],
            }

            return JsonResponse({"results": [earning_details]}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


#Handy Man Agent Api's
@csrf_exempt
def handyman_login_view(request):
    if request.method == "POST":
        data = json.loads(request.body)
        mobile_no = data.get("mobile_no")

         # List of required fields
        required_fields = {
            "mobile_no": mobile_no,
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
                
                
        try:
            query = """
            SELECT handyman_id,name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,city_id FROM
            vtpartner.handymans_tbl WHERE mobile_no=%s
            """
            params = [mobile_no]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                try:
                    #Insert if not found
                    query = """
                        INSERT INTO vtpartner.handymans_tbl (
                            mobile_no
                        ) VALUES (%s) RETURNING handyman_id
                    """
                    values = [mobile_no]
                    new_result = insert_query(query, values)
                    print("new_result::",new_result)
                    if new_result:
                        print("new_result[0][0]::",new_result[0][0])
                        handyman_id = new_result[0][0]
                        response_value = [
                            {
                                "handyman_id":handyman_id
                            }
                        ]
                        return JsonResponse({"result": response_value}, status=200)
                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "handyman_id": row[0],
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
                    "full_address": row[13],
                    "city_id": row[14],
                    
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def handyman_registration(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        handyman_id = data.get("handyman_id")
        driver_first_name = data.get("driver_first_name")
        profile_pic = data.get("profile_pic")
        mobile_no = data.get("mobile_no")
        r_lat = data.get("r_lat")
        r_lng = data.get("r_lng")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")
        recent_online_pic = data.get("recent_online_pic")
        city_id = data.get("city_id")
        aadhar_no = data.get("aadhar_no")
        pan_card_no = data.get("pan_card_no")
        full_address = data.get("full_address")
        gender = data.get("gender")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
        sub_cat_id = data.get("sub_cat_id")
        service_id = data.get("service_id")
        
        
        
        
        
        # List of required fields
        required_fields = {
            "handyman_id":handyman_id,
            "driver_first_name":driver_first_name,
            "profile_pic":profile_pic,
            "mobile_no":mobile_no,
            "r_lat":r_lat,
            "r_lng":r_lng,
            "current_lat":current_lat,
            "current_lng":current_lng,
            "recent_online_pic":recent_online_pic,
            "city_id":city_id,
            "aadhar_no":aadhar_no,
            "pan_card_no":pan_card_no,
            "full_address":full_address,
            "gender":gender,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            "sub_cat_id":sub_cat_id,
            "service_id":service_id,
            
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
            UPDATE vtpartner.handymans_tbl 
            SET 
            name = %s,
            profile_pic = %s,
            mobile_no = %s,
            r_lat = %s,
            r_lng = %s,
            current_lat = %s,
            current_lng = %s,
            recent_online_pic = %s,
            category_id = %s,
            sub_cat_id = %s,
            service_id = %s,
            city_id = %s,
            aadhar_no = %s,
            pan_card_no = %s,
            full_address = %s,
            gender = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            pan_card_front = %s,
            pan_card_back = %s
            
            WHERE handyman_id=%s
        """
        values = [
            driver_first_name,
            profile_pic,
            mobile_no,
            r_lat,
            r_lng,
            r_lat,
            r_lng,
            recent_online_pic,
            '5',
            sub_cat_id,
            service_id,
            city_id,
            aadhar_no,
            pan_card_no,
            full_address,
            gender,
            aadhar_card_front,
            aadhar_card_back,
            pan_card_front,
            pan_card_back,
            handyman_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing add new faq query"}, status=500)

@csrf_exempt
def handyman_aadhar_details_update(request):
    try:
        data = json.loads(request.body)
        #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
        handyman_id = data.get("handyman_id")
        aadhar_no = data.get("aadhar_no")
        aadhar_card_front = data.get("aadhar_card_front")
        aadhar_card_back = data.get("aadhar_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "handyman_id":handyman_id,
            "aadhar_no":aadhar_no,
            "aadhar_card_front":aadhar_card_front,
            "aadhar_card_back":aadhar_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            aadhar_no = %s,
            aadhar_card_front = %s,
            aadhar_card_back = %s,
            WHERE handyman_id=%s
        """
        values = [
            aadhar_no,
            aadhar_card_front,
            aadhar_card_back,
            handyman_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def handyman_pan_card_details_update(request):
    try:
        data = json.loads(request.body)

        handyman_id = data.get("handyman_id")
        pan_card_no = data.get("pan_card_no")
        pan_card_front = data.get("pan_card_front")
        pan_card_back = data.get("pan_card_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "handyman_id":handyman_id,
            "pan_card_no":pan_card_no,
            "pan_card_front":pan_card_front,
            "pan_card_back":pan_card_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            pan_card_no = %s,
            pan_card_front = %s,
            pan_card_back = %s,
            WHERE handyman_id=%s
        """
        values = [
            pan_card_no,
            pan_card_front,
            pan_card_back,
            handyman_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def handyman_driving_license_details_update(request):
    try:
        data = json.loads(request.body)

        handyman_id = data.get("handyman_id")
        driving_license_no = data.get("driving_license_no")
        license_front = data.get("license_front")
        license_back = data.get("license_back")
       
        
        
        
        
        # List of required fields
        required_fields = {
            "handyman_id":handyman_id,
            "driving_license_no":driving_license_no,
            "license_front":license_front,
            "license_back":license_back,
            
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
            UPDATE vtpartner.customers_tbl 
            SET 
            driving_license_no = %s,
            license_front = %s,
            license_back = %s,
            WHERE handyman_id=%s
        """
        values = [
            driving_license_no,
            license_front,
            license_back,
            handyman_id
        ]
        row_count = update_query(query, values)

        # Send success response
        return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

    except Exception as err:
        print("Error executing query", err)
        return JsonResponse({"message": "Error executing update query"}, status=500)

@csrf_exempt
def handyman_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        handyman_id = data.get("handyman_id")

         # List of required fields
        required_fields = {
            "handyman_id": handyman_id,
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
                
                
        try:
            query = """
            select is_online,status,name,recent_online_pic,profile_pic,mobile_no from vtpartner.handymans_tbl where handyman_id=%s
            """
            params = [handyman_id]
            result = select_query(query, params)  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)
                                
            # Map the results to a list of dictionaries with meaningful keys
            response_value = [
                {
                    "is_online": row[0],
                    "status": row[1],  
                    "driver_first_name": row[2],  
                    "recent_online_pic": row[3],  
                    "profile_pic": row[4],  
                    "mobile_no": row[5],  
                }
                for row in result
            ]
            # Return customer response
            return JsonResponse({"results": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def handyman_update_online_status(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        handyman_id = data.get("handyman_id")
        recent_online_pic = data.get("recent_online_pic")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "status": status,
            "handyman_id": handyman_id,
            "lat": lat,
            "lng": lng
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

            if status == 1:
                # Include recent_online_pic in the query when status is 1
                query = """
                UPDATE vtpartner.handymans_tbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s,
                    recent_online_pic = %s
                WHERE handyman_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    recent_online_pic,
                    handyman_id
                ]
                

            else:
                # Exclude recent_online_pic when status is not 1
                query = """
                UPDATE vtpartner.handymans_tbl 
                SET 
                    is_online = %s,
                    current_lat = %s,
                    current_lng = %s
                WHERE handyman_id = %s
                """
                values = [
                    status,
                    lat,
                    lng,
                    handyman_id
                ]

            # Execute the query
            row_count = update_query(query, values)
            
            #insert record in attendance table
            query_insert = """
                    INSERT INTO vtpartner.handyman_attendance_tbl(driver_id, time, date, status) 
                    VALUES (%s, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE, %s)
                """

            insert_values = [
                handyman_id,
                status
            ]
            
            row_count = insert_query(query_insert,insert_values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def add_handyman_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        status = data.get("status")
        handyman_id = data.get("handyman_id")
        current_lat = data.get("current_lat")
        current_lng = data.get("current_lng")

         # List of required fields
        required_fields = {
            "status": status,
            "handyman_id": handyman_id,
            "current_lat": current_lat,
            "current_lng": current_lng,
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
                
                
        try:
            query = """
            INSERT INTO vtpartner.active_handymantbl 
            (handyman_id,current_lat,current_lng,current_status)
            VALUES (%s,%s,%s,%s) RETURNING active_id
            """
            values = [
                handyman_id,
                current_lat,
                current_lng,
                status,
            ]
            new_result = insert_query(query, values)
            print("new_result::",new_result)
            if new_result:
                print("new_result[0][0]::",new_result[0][0])
                active_id = new_result[0][0]
                response_value = [
                    {
                        "active_id":active_id
                    }
                ]
                # Send success response
                return JsonResponse({"result": response_value}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def delete_handyman_to_active_drivers_table(request):
    if request.method == "POST":
        data = json.loads(request.body)
        handyman_id = data.get("handyman_id")
        

         # List of required fields
        required_fields = {
            "handyman_id": handyman_id,
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
                
                
        try:
            query = """
            DELETE FROM vtpartner.active_handymantbl 
            WHERE handyman_id=%s
            """
            values = [
                handyman_id,
            ]
            row_count = delete_query(query, values)
            
            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def update_handymans_current_location(request):
    if request.method == "POST":
        data = json.loads(request.body)
        handyman_id = data.get("handyman_id")
        lat = data.get("lat")
        lng = data.get("lng")

        # List of required fields
        required_fields = {
            "handyman_id": handyman_id,
            "lat": lat,
            "lng": lng
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
                UPDATE vtpartner.active_handymantbl 
                SET 
                    current_lat = %s,
                    current_lng = %s
                WHERE handyman_id = %s
                """
            values = [
                    lat,
                    lng,
                    handyman_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt
def get_nearby_handymans(request):
    if request.method == "POST":
        data = json.loads(request.body)
        lat = data.get("lat")
        lng = data.get("lng")
        city_id = data.get("city_id")
        price_type = data.get("price_type", 1)
        radius_km = data.get("radius_km", 5)  # Radius in kilometers

        if lat is None or lng is None:
            return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

        try:
            # Haversine formula to calculate the distance in kilometers
            #SELECT main.active_id, main.handyman_id, main.current_lat, main.current_lng, 
#        main.entry_time, main.current_status, handymanstbl.driver_first_name,
#        handymanstbl.profile_pic, vehiclestbl.image AS vehicle_image, 
#        vehiclestbl.vehicle_name,weight,
#        (6371 * acos(
#            cos(radians(%s)) * cos(radians(main.current_lat)) *
#            cos(radians(main.current_lng) - radians(%s)) +
#            sin(radians(%s)) * sin(radians(main.current_lat))
#        )) AS distance
# FROM vtpartner.active_handymantbl AS main
# INNER JOIN (
#     SELECT handyman_id, MAX(entry_time) AS max_entry_time
#     FROM vtpartner.active_handymantbl
#     GROUP BY handyman_id
# ) AS latest ON main.handyman_id = latest.handyman_id
#               AND main.entry_time = latest.max_entry_time
# JOIN vtpartner.handymans_tbl ON main.handyman_id = handymanstbl.handyman_id
# JOIN vtpartner.vehiclestbl ON handymanstbl.vehicle_id = vehiclestbl.vehicle_id
# WHERE main.current_status = 1
#   AND (6371 * acos(
#          cos(radians(%s)) * cos(radians(main.current_lat)) *
#          cos(radians(main.current_lng) - radians(%s)) +
#          sin(radians(%s)) * sin(radians(main.current_lat))
#      )) <= %s
#   AND handymanstbl.category_id = vehiclestbl.category_id
#   AND handymanstbl.category_id = '1'
# ORDER BY distance;
#             """
#             values = [lat, lng, lat, lat, lng, lat, radius_km]

            query = """
            SELECT 
    main.active_id, 
    main.handyman_id, 
    main.current_lat, 
    main.current_lng, 
    main.entry_time, 
    main.current_status, 
    handymanstbl.driver_first_name,
    handymanstbl.profile_pic, 
    vehiclestbl.image AS vehicle_image, 
    vehiclestbl.vehicle_name,
    vehiclestbl.weight,
    vehicle_city_wise_price_tbl.starting_price_per_km,
    vehicle_city_wise_price_tbl.base_fare,
    vehiclestbl.vehicle_id,
    vehiclestbl.size_image,
    (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
    )) AS distance
FROM vtpartner.active_handymantbl AS main
INNER JOIN (
    SELECT handyman_id, MAX(entry_time) AS max_entry_time
    FROM vtpartner.active_handymantbl
    GROUP BY handyman_id
) AS latest ON main.handyman_id = latest.handyman_id
             AND main.entry_time = latest.max_entry_time
JOIN vtpartner.handymans_tbl ON main.handyman_id = handymanstbl.handyman_id
JOIN vtpartner.vehiclestbl ON handymanstbl.vehicle_id = vehiclestbl.vehicle_id
JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
WHERE main.current_status = 1
  AND (6371 * acos(
        cos(radians(%s)) * cos(radians(main.current_lat)) *
        cos(radians(main.current_lng) - radians(%s)) +
        sin(radians(%s)) * sin(radians(main.current_lat))
      )) <= %s
  AND handymanstbl.category_id = vehiclestbl.category_id
  AND handymanstbl.category_id = '1'
ORDER BY distance;

            """
            values = [lat, lng, lat,city_id,price_type, lat, lng, lat, radius_km]

            # Execute the query
            nearby_drivers = select_query(query, values)
            

            # Format response
            drivers_list = [
                {
                    "active_id": driver[0],
                    "handyman_id": driver[1],
                    "latitude": driver[2],
                    "longitude": driver[3],
                    "entry_time": driver[4],
                    "current_status": driver[5],
                    "driver_name": driver[6],
                    "driver_profile_pic": driver[7],
                    "vehicle_image": driver[8],
                    "vehicle_name": driver[9],
                    "weight": driver[10],
                    "starting_price_per_km": driver[11],
                    "base_fare": driver[12],
                    "vehicle_id": driver[13],
                    "size_image": driver[14],
                    "distance": driver[15]
                }
                for driver in nearby_drivers
            ]

            return JsonResponse({"nearby_drivers": drivers_list}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)




@csrf_exempt
def update_firebase_handyman_token(request):
    if request.method == "POST":
        data = json.loads(request.body)
        handyman_id = data.get("handyman_id")
        authToken = data.get("authToken")
        

        # List of required fields
        required_fields = {
            "handyman_id": handyman_id,
            "authToken": authToken,
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
                UPDATE vtpartner.handymans_tbl 
                SET 
                    authtoken = %s
                WHERE handyman_id = %s
                """
            values = [
                    authToken,
                    handyman_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def handyman_booking_details_for_ride_acceptance(request):
    if request.method == "POST":
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
            
        try:
            query = """
                select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,customer_name,customers_tbl.authtoken,pickup_address,drop_address from vtpartner.bookings_tbl,vtpartner.customers_tbl where customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "customer_name": row[28],
                    "customers_auth_token": row[29],
                    "pickup_address": row[30],
                    "drop_address": row[31],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def handyman_booking_accepted(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "server_token": server_token,
            "customer_id":customer_id
        
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
                select driver_id from vtpartner.bookings_tbl where booking_id=%s and driver_id!='-1'
            """
            result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

            if result == []:
                #Update booking status and driver assinged
                try:

                    query = """
                       update vtpartner.bookings_tbl set driver_id=%s ,booking_status='Driver Accepted' where booking_id=%s
                        """
                    values = [
                            driver_id,
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(query, values)
                    #Inserting record in booking_history table
                    try:

                        query = """
                           insert into vtpartner.bookings_history_tbl (status,booking_id) values ('Driver Accepted',%s)
                            """
                        values = [
                                booking_id
                            ]

                        # Execute the query
                        row_count = insert_query(query, values)
                        #Updating driver status to occupied
                        try:

                            query = """
                               update vtpartner.active_handymantbl set current_status='2' where handyman_id=%s
                                """
                            values = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query, values)



                            #get the customer auth token
                            auth_token = get_customer_auth_token(customer_id)
                            
                            #send Fcm notification to customer saying driver assigned
                            customer_data = {
                                'intent':'live_tracking',
                                'booking_id':str(booking_id)
                            }
                            sendFMCMsg(auth_token,'You have been assigned a driver','Driver Assigned',customer_data,server_token)

                            # Send success response
                            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                        
                        
                        

                    except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500)

                    

                except Exception as err:
                    print("Error executing updating booking status to accepted:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)

            # Checking if driver is assigned
            ret_driver_id = result[0][0]
            return JsonResponse({"message": "No Data Found"}, status=404)
            
        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def update_booking_status_handyman(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "booking_status": booking_status,
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
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP Verified":
                    body = "You're OTP is Verified Successfully!"
                    title = "OTP Verification"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                    # Update Pickup epoch here
                    update_pickup_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET pickup_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_pickup_epoch_query, values)
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                    # Update Drop epoch here
                    update_drop_epoch_query = """
                    UPDATE vtpartner.bookings_tbl SET drop_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
                    """
                    values = [
                            booking_id
                        ]

                    # Execute the query
                    row_count = update_query(update_drop_epoch_query, values)
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)
    
@csrf_exempt 
def generate_order_id_for_booking_id_handyman(request):
    if request.method == "POST":
        data = json.loads(request.body)
        booking_id = data.get("booking_id")
        driver_id = data.get("driver_id")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        booking_status = data.get("booking_status")
        server_token = data.get("server_token")
        customer_id = data.get("customer_id")
        total_amount = data.get("total_amount")
        

        # List of required fields
        required_fields = {
            "booking_id": booking_id,
            "driver_id": driver_id,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "booking_status": booking_status,
            "server_token": server_token,
            "customer_id": customer_id,
            "total_amount": total_amount,
            
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        #Updating the Status to Trip Ended
        
        try:

            query = """
                update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
                """
            values = [
                    booking_status,
                    booking_id
                ]

            # Execute the query
            row_count = update_query(query, values)

            # Updating Booking History Table
            try:

                query = """
                    insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
                    """
                values = [
                        booking_id,
                        booking_status
                    ]

                # Execute the query
                row_count = insert_query(query, values)

                # Send success response
                auth_token = get_customer_auth_token(customer_id)
                body = title = ""
                data_map = {}
                if booking_status == "Driver Arrived":
                    body = "Our agent has arrived at your pickup location"
                    title = "Agent Arrived"
                elif booking_status == "OTP verified":
                    body = "Your trip otp is verified"
                    title = "Trip OTP Verified"
                elif booking_status == "Start Trip":
                    body = "Trip has been started from your pickup location"
                    title = "Trip Started"
                elif booking_status == "Ongoing":
                    body = "Trip has been started from your pickup location"
                    title = "Ongoing"
                elif booking_status == "End Trip":
                    body = "Your package has been delivered successfully"
                    title = "Package Deliveried"
                sendFMCMsg(auth_token,body,title,data_map,server_token)
                #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
                
                #Generating Order ID
                try:
                    query = """
                    INSERT INTO vtpartner.orders_tbl (
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    )
                    SELECT 
                        customer_id, 
                        driver_id, 
                        pickup_lat, 
                        pickup_lng, 
                        destination_lat, 
                        destination_lng, 
                        distance, 
                        time, 
                        total_price, 
                        base_price, 
                        booking_timing, 
                        booking_date, 
                        booking_status, 
                        driver_arrival_time, 
                        otp, 
                        gst_amount, 
                        igst_amount, 
                        goods_type_id, 
                        payment_method, 
                        city_id, 
                        booking_id, 
                        sender_name, 
                        sender_number, 
                        receiver_name, 
                        receiver_number, 
                        pickup_address, 
                        drop_address,
                        pickup_time,
                        drop_time
                    FROM vtpartner.bookings_tbl
                    WHERE booking_id = %s
                    RETURNING order_id;
                    """

                    

                    # Execute the query
                    ret_result = insert_query2(query,[booking_id])
                    #get order_id from here
                    if ret_result!=None:
                        order_id = ret_result[0][0]
                        try:
                            query2 = """
                            update vtpartner.active_handymantbl set current_status='1' where handyman_id=%s
                            """
                            values2 = [
                                    driver_id
                                ]

                            # Execute the query
                            row_count = update_query(query2, values2)
                            #success
                            try:
                                query3 = """
                                update vtpartner.bookings_tbl set booking_completed='1' where booking_id=%s
                                """
                                values3 = [
                                        booking_id
                                    ]

                                # Execute the query
                                row_count = update_query(query3, values3)
                                
                                query_update = """
                                update vtpartner.orders_tbl set payment_method=%s,payment_id=%s where order_id=%s
                                """
                                values_update = [
                                        payment_method,
                                        payment_id,
                                        order_id
                                    ]

                                # Execute the query
                                row_count = update_query(query_update, values_update)
                                
                                #Adding the amount to driver earnings table
                                try:
                                    query4 = """
                                    insert into vtpartner.handyman_earningstbl(driver_id,amount,order_id,payment_id,payment_mode) values (%s,%s,%s,%s,%s)
                                    """
                                    values4 = [
                                            driver_id,
                                            total_amount,
                                            order_id,
                                            payment_id,
                                            payment_method
                                        ]

                                    # Execute the query
                                    row_count = insert_query(query4, values4)
                                    #success
                                    #Adding the amount to driver earnings table
                                    try:
                                        query5 = """
                                            UPDATE vtpartner.handyman_topup_recharge_current_points_tbl
                                            SET 
                                                used_points = used_points + %s,
                                                remaining_points = CASE 
                                                    WHEN remaining_points >= %s THEN remaining_points - %s
                                                    ELSE 0
                                                END,
                                                negative_points = CASE
                                                    WHEN remaining_points < %s THEN negative_points + (%s - remaining_points)
                                                    ELSE negative_points
                                                END,
                                                last_updated_time = date_part('epoch'::text, CURRENT_TIMESTAMP)
                                            WHERE driver_id = %s
                                        """
                                        values5 = [
                                                total_amount,  # %s for updating used_points
                                                total_amount,  # %s for checking remaining_points
                                                total_amount,  # %s for deducting from remaining_points
                                                total_amount,  # %s for checking if negative_points should be updated
                                                total_amount,  # %s for calculating difference for negative_points
                                                driver_id          # %s for identifying the driver
                                            ]

                                        # Execute the query
                                        row_count = insert_query(query5, values5)
                                        #success
                                        return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                                    except Exception as err:
                                        print("Error executing query:", err)
                                        return JsonResponse({"message": "An error occurred"}, status=500)
                                except Exception as err:
                                    print("Error executing query:", err)
                                    return JsonResponse({"message": "An error occurred"}, status=500)
                                
                                    #success
                                    #return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                            except Exception as err:
                                print("Error executing query:", err)
                                return JsonResponse({"message": "An error occurred"}, status=500)
                            
                            
                            # return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
                        except Exception as err:
                            print("Error executing query:", err)
                            return JsonResponse({"message": "An error occurred"}, status=500)
                    

                except Exception as err:
                    print("Error executing query:", err)
                    return JsonResponse({"message": "An error occurred"}, status=500)
        
            except Exception as err:
                        print("Error executing query:", err)
                        return JsonResponse({"message": "An error occurred"}, status=500) 
        

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_handyman_recharge_list(request):
    if request.method == "POST":
        data = json.loads(request.body)
        category_id = data.get("category_id")
        

        # List of required fields
        required_fields = {
            "category_id": category_id,
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
               select recharge_id,amount,points,status,description,valid_days from vtpartner.recharge_plans_tbl where category_id=%s
                ORDER BY 
                    amount ASC
            """
            result = select_query(query,[category_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "recharge_id": row[0],
                    "amount": row[1],
                    "points": row[2],
                    "status": row[3],
                    "description": row[4],
                    "valid_days": row[5]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def get_handyman_current_recharge_details(request):
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
               select topup_id,recharge_id,allotted_points,used_points,remaining_points,negative_points,valid_till_date,status from vtpartner.handyman_topup_recharge_current_points_tbl where handy_man_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "topup_id": row[0],
                    "recharge_id": row[1],
                    "allotted_points": row[2],
                    "used_points": row[3],
                    "remaining_points": row[4],
                    "negative_points": row[5],
                    "valid_till_date": row[6],
                    "status": row[7]
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def get_handyman_recharge_history_details(request):
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
               select history_id,recharge_id,amount,allotted_points,date,valid_till_date,status,payment_method,payment_id,transaction_type,admin_id,last_recharge_negative_points from vtpartner.handyman_topup_recharge_history_tbl where handy_man_id=%s
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            services_details = [
                {
                    "history_id": row[0],
                    "recharge_id": row[1],
                    "amount": row[2],
                    "allotted_points": row[3],
                    "date": row[4],
                    "valid_till_date": row[5],
                    "status": row[6],
                    "payment_method": row[7],
                    "payment_id": row[8],
                    "transaction_type": row[9],
                    "admin_id": row[10],
                    "last_recharge_negative_points": row[11],
                    
                }
                for row in result
            ]

            return JsonResponse({"results": services_details}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)


@csrf_exempt 
def new_handyman_recharge(request):
    if request.method == "POST":
        data = json.loads(request.body)
        driver_id = data.get("driver_id")
        topup_id = data.get("topup_id")
        recharge_id = data.get("recharge_id")
        amount = data.get("amount")
        allotted_points = data.get("allotted_points")
        valid_till_date = data.get("valid_till_date")
        payment_method = data.get("payment_method")
        payment_id = data.get("payment_id")
        negative_points = data.get("previous_negative_points")
        last_validity_date = data.get("last_validity_date")

        # List of required fields
        required_fields = {
            "driver_id": driver_id,
            "topup_id": topup_id,
            "recharge_id": recharge_id,
            "amount": amount,
            "allotted_points": allotted_points,
            "valid_till_date": valid_till_date,
            "payment_method": payment_method,
            "payment_id": payment_id,
            "previous_negative_points": negative_points,
            "last_validity_date": last_validity_date,
        }
        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        # If there are missing fields, return an error response
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        if negative_points > 0:
            allotted_points -= negative_points
        try:
            query = """
                insert into vtpartner.handyman_topup_recharge_history_tbl(handy_man_id,recharge_id,amount,allotted_points,valid_till_date,payment_method,payment_id,last_recharge_negative_points) values (%s,%s,%s,%s,%s,%s,%s,%s)
                """
            values = [
                    driver_id,
                    recharge_id,
                    amount,
                    allotted_points,
                    valid_till_date,
                    payment_method,
                    payment_id,
                    negative_points
                ]
            # Execute the query
            row_count = insert_query(query, values)
            
            #checking if recharge has been expired
            last_validity_date_obj = datetime.strptime(last_validity_date, "%Y-%m-%d")

            # Get the current date
            current_date = datetime.now()

            # Check if last_validity_date is greater than the current date
            isExpired = False
            if last_validity_date_obj > current_date:
                print("The last validity date is in the future.")
            else:
                isExpired = True
                print("The last validity date is today or has passed.")

            # Updating Booking History Table
            try:
                if negative_points > 0 or isExpired:
                    query = """
                    update vtpartner.handyman_topup_recharge_current_points_tbl set recharge_id=%s,allotted_points=%s,valid_till_date=%s,remaining_points=%s,negative_points='0',used_points='0' where topup_id=%s and handy_man_id=%s
                    """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            allotted_points,
                            topup_id,
                            driver_id
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)
                else:
                    query = """
                        INSERT INTO vtpartner.handyman_topup_recharge_current_points_tbl (recharge_id,allotted_points,valid_till_date,handy_man_id,remaining_points) VALUES (%s,%s,%s,%s,%s)
                        """
                    values = [
                            recharge_id,
                            allotted_points,
                            valid_till_date,
                            driver_id,
                            allotted_points
                        ]

                    # Execute the query
                    row_count = insert_query(query, values)

                
                return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

            except Exception as err:
                print("Error executing query:", err)
                return JsonResponse({"message": "An error occurred"}, status=500)
            #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "An error occurred"}, status=500)
        

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def handyman_all_orders(request):
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
                select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,handymanstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,handymanstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,orders_tbl.ratings,orders_tbl.rating_description from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.handymans_tbl,vtpartner.customers_tbl where handymanstbl.handyman_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and orders_tbl.driver_id=%s and  vehiclestbl.vehicle_id=handymanstbl.vehicle_id order by order_id desc
            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            booking_details = [
                {
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
                    "handyman_auth_token": str(row[27]),
                    "customer_name": str(row[28]),
                    "customers_auth_token": str(row[29]),
                    "pickup_address": str(row[30]),
                    "drop_address": str(row[31]),
                    "customer_mobile_no": str(row[32]),
                    "driver_mobile_no": str(row[33]),
                    "vehicle_id": str(row[34]),
                    "vehicle_name": str(row[35]),
                    "vehicle_image": str(row[36]),
                    "ratings": str(row[37]),
                    "rating_description": str(row[38]),
                }
                for row in result
            ]

            return JsonResponse({"results": booking_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt 
def handyman_whole_year_earnings(request):
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
                SELECT
                months.month_index,
                COALESCE(SUM(e.amount), 0.0) AS total_earnings
            FROM (
                SELECT 1 AS month_index UNION ALL
                SELECT 2 UNION ALL
                SELECT 3 UNION ALL
                SELECT 4 UNION ALL
                SELECT 5 UNION ALL
                SELECT 6 UNION ALL
                SELECT 7 UNION ALL
                SELECT 8 UNION ALL
                SELECT 9 UNION ALL
                SELECT 10 UNION ALL
                SELECT 11 UNION ALL
                SELECT 12
            ) AS months
            LEFT JOIN vtpartner.handyman_earningstbl e
                ON EXTRACT(MONTH FROM e.earning_date) = months.month_index
                AND e.driver_id = %s
            GROUP BY months.month_index
            ORDER BY months.month_index;

            """
            result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

            if result == []:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Map each row to a dictionary with appropriate keys
            earning_details = [
                {
                    "month_index": row[0],
                    "total_earnings": row[1],
                   
                }
                for row in result
            ]

            return JsonResponse({"results": earning_details}, status=200)


        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)

@csrf_exempt
def handyman_todays_earnings(request):
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
            # Query to get today's earnings and rides count
            query = """
                SELECT COALESCE(SUM(amount), 0) AS todays_earnings, 
                       COUNT(*) AS todays_rides 
                FROM vtpartner.handyman_earningstbl 
                WHERE handy_man_id = %s AND earning_date = CURRENT_DATE;
            """
            result = select_query(query, [driver_id])  # Assuming select_query is defined elsewhere

            if not result:
                return JsonResponse({"message": "No Data Found"}, status=404)

            # Extract the first row from the result
            row = result[0]
            earning_details = {
                "todays_earnings": row[0],
                "todays_rides": row[1],
            }

            return JsonResponse({"results": [earning_details]}, status=200)

        except Exception as err:
            print("Error executing query:", err)
            return JsonResponse({"message": "Internal Server Error"}, status=500)

    return JsonResponse({"message": "Method not allowed"}, status=405)
