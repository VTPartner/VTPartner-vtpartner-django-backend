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

# Utility function to check for missing fields
def check_missing_fields(fields):
    missing_fields = [field for field, value in fields.items() if not value]
    return missing_fields if missing_fields else None

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

