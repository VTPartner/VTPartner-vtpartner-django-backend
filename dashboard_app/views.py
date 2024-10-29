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

# Create your views here.
@csrf_exempt
def login_view(request):
    if request.method == "POST":
        email = request.POST.get('email')
        password = request.POST.get('password')
        
        # Use parameterized query to prevent SQL injection
        query = "SELECT admin_id, admin_name, email, password FROM vtpartner.admintbl WHERE email = %s AND password = %s"
        user_data = execute_raw_query_fetch_one(query, (email, password))  # Pass parameters as a tuple
        
        if user_data:
            # User is authorized
            print('User is Authorized')
            request.session['usrname'] = user_data[0]
            request.session['username'] = user_data[1]
            request.session['password'] = user_data[2]
            request.session['userid'] = user_data[3]
            request.session['is_logged_in'] = True
            
            # Setting the session to expire after one day (86400 seconds)
            request.session.set_expiry(86400)
            print('All Session Data Saved') 
            
            # Create response data
            response_data = {
                'token': 'your_generated_jwt_token_here',  # You need to generate a JWT token
                'user': {
                    'id': user_data[0],
                    'name': user_data[1],
                    'email': user_data[2],
                },
            }
            return JsonResponse(response_data, status=200)

        else:
            return JsonResponse({'message': 'Invalid credentials, please try again'}, status=401)

    # If not a POST request, you can return a different response
    return JsonResponse({'message': 'Method not allowed'}, status=405)
    
    
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
