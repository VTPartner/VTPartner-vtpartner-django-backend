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
        }

        # Check for missing fields
        missing_fields = check_missing_fields(required_fields)
        
        if missing_fields:
            return JsonResponse(
                {"message": f"Missing required fields: {', '.join(missing_fields)}"},
                status=400
            )
        
        try:
            # Insert record in the booking table
            query_insert = """
                INSERT INTO vtpartner.bookings_tbl (
                    customer_id, driver_id, pickup_lat, pickup_lng, destination_lat, destination_lng, 
                    distance, time, total_price, base_price, booking_timing, booking_date, 
                    otp, gst_amount, igst_amount, 
                    payment_method, city_id,sender_name,sender_number,receiver_name,receiver_number
                ) 
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE,  %s, %s, %s, 
                    %s, %s,%s, %s,%s, %s
                ) 
                RETURNING booking_id;
            """

            insert_values = [
                customer_id, driver_id, pickup_lat, pickup_lng, destination_lat, destination_lng, 
                distance, time, total_price, base_price, otp, 
                gst_amount, igst_amount, payment_method, city_id,sender_name,sender_number,receiver_name,receiver_number
            ]

            # Assuming insert_query is a function that runs the query
            new_result = insert_query(query_insert, insert_values)
            
            if new_result:
                booking_id = new_result[0][0]  # Extracting booking_id from the result
                response_value = [{"booking_id": booking_id}]

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
            select is_online,status,driver_first_name,recent_online_pic from vtpartner.goods_driverstbl where goods_driver_id=%s
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
                    "recent_online_pic": row[3]  
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
            
            # Send success response
            return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

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
        try:
            query = """
            select guide_id,guide_line from vtpartner.guide_lines_tbl where category_id='1'
            """
            
            result = select_query(query)

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
