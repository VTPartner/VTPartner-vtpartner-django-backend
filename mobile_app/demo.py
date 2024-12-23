# #Handy Man Agent Api's
# @csrf_exempt
# def handyman_login_view(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         mobile_no = data.get("mobile_no")

#          # List of required fields
#         required_fields = {
#             "mobile_no": mobile_no,
#         }
#         # Check for missing fields
#          # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#             {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#             status=400
#         )
                
                
#         try:
#             query = """
#             SELECT handyman_id,driver_first_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,city_id FROM
#             vtpartner.handymanstbl WHERE mobile_no=%s
#             """
#             params = [mobile_no]
#             result = select_query(query, params)  # Assuming select_query is defined elsewhere

#             if result == []:
#                 try:
#                     #Insert if not found
#                     query = """
#                         INSERT INTO vtpartner.handymanstbl (
#                             mobile_no
#                         ) VALUES (%s) RETURNING handyman_id
#                     """
#                     values = [mobile_no]
#                     new_result = insert_query(query, values)
#                     print("new_result::",new_result)
#                     if new_result:
#                         print("new_result[0][0]::",new_result[0][0])
#                         handyman_id = new_result[0][0]
#                         response_value = [
#                             {
#                                 "handyman_id":handyman_id
#                             }
#                         ]
#                         return JsonResponse({"result": response_value}, status=200)
#                 except Exception as err:
#                     print("Error executing query:", err)
#                     return JsonResponse({"message": "An error occurred"}, status=500)
                
#             # Map the results to a list of dictionaries with meaningful keys
#             response_value = [
#                 {
#                     "handyman_id": row[0],
#                     "driver_first_name": row[1],
#                     "profile_pic": row[2],
#                     "is_online": row[3],
#                     "ratings": row[4],
#                     "mobile_no": row[5],
#                     "registration_date": row[6],
#                     "time": row[7],
#                     "r_lat": row[8],
#                     "r_lng": row[9],
#                     "current_lat": row[10],
#                     "current_lng": row[11],
#                     "status": row[12],
#                     "full_address": row[13],
#                     "city_id": row[14],
                    
#                 }
#                 for row in result
#             ]
#             # Return customer response
#             return JsonResponse({"results": response_value}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt
# def handyman_registration(request):
#     try:
#         data = json.loads(request.body)
#         #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
#         handyman_id = data.get("handyman_id")
#         driver_first_name = data.get("driver_first_name")
#         profile_pic = data.get("profile_pic")
#         mobile_no = data.get("mobile_no")
#         r_lat = data.get("r_lat")
#         r_lng = data.get("r_lng")
#         current_lat = data.get("current_lat")
#         current_lng = data.get("current_lng")
#         recent_online_pic = data.get("recent_online_pic")
#         vehicle_id = data.get("vehicle_id")
#         city_id = data.get("city_id")
#         aadhar_no = data.get("aadhar_no")
#         pan_card_no = data.get("pan_card_no")
#         full_address = data.get("full_address")
#         gender = data.get("gender")
#         aadhar_card_front = data.get("aadhar_card_front")
#         aadhar_card_back = data.get("aadhar_card_back")
#         pan_card_front = data.get("pan_card_front")
#         pan_card_back = data.get("pan_card_back")
#         license_front = data.get("license_front")
#         license_back = data.get("license_back")
#         insurance_image = data.get("insurance_image")
#         noc_image = data.get("noc_image")
#         pollution_certificate_image = data.get("pollution_certificate_image")
#         rc_image = data.get("rc_image")
#         vehicle_image = data.get("vehicle_image")
#         vehicle_plate_image = data.get("vehicle_plate_image")
#         driving_license_no = data.get("driving_license_no")
#         vehicle_plate_no = data.get("vehicle_plate_no")
#         rc_no = data.get("rc_no")
#         insurance_no = data.get("insurance_no")
#         noc_no = data.get("noc_no")
#         vehicle_fuel_type = data.get("vehicle_fuel_type")
#         owner_name = data.get("owner_name")
#         owner_mobile_no = data.get("owner_mobile_no")
#         owner_photo_url = data.get("owner_photo_url")
#         owner_address = data.get("owner_address")
#         owner_city_name = data.get("owner_city_name")
        
        
        
        
#         # List of required fields
#         required_fields = {
#             "handyman_id":handyman_id,
#             "driver_first_name":driver_first_name,
#             "profile_pic":profile_pic,
#             "mobile_no":mobile_no,
#             "r_lat":r_lat,
#             "r_lng":r_lng,
#             "current_lat":current_lat,
#             "current_lng":current_lng,
#             "recent_online_pic":recent_online_pic,
#             "vehicle_id":vehicle_id,
#             "city_id":city_id,
#             "aadhar_no":aadhar_no,
#             "pan_card_no":pan_card_no,
#             "full_address":full_address,
#             "gender":gender,
#             "aadhar_card_front":aadhar_card_front,
#             "aadhar_card_back":aadhar_card_back,
#             "pan_card_front":pan_card_front,
#             "pan_card_back":pan_card_back,
#             "license_front":license_front,
#             "license_back":license_back,
#             "insurance_image":insurance_image,
#             "noc_image":noc_image,
#             "pollution_certificate_image":pollution_certificate_image,
#             "rc_image":rc_image,
#             "vehicle_image":vehicle_image,
#             "vehicle_plate_image":vehicle_plate_image,
#             "driving_license_no":driving_license_no,
#             "vehicle_plate_no":vehicle_plate_no,
#             "rc_no":rc_no,
#             "insurance_no":insurance_no,
#             "noc_no":noc_no,
#             "vehicle_fuel_type":vehicle_fuel_type,
#             "owner_name":owner_name,
#             "owner_mobile_no":owner_mobile_no,
#             "owner_photo_url":owner_photo_url,
#             "owner_address":owner_address,
#             "owner_city_name":owner_city_name,
#         }

#         # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)

#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
#         # Check if owner exists by mobile number
#         owner_id = None
#         if owner_name and owner_mobile_no:
#             try:
#                 # Check if owner already exists based on mobile number
#                 check_owner_query = "SELECT owner_id FROM vtpartner.owner_tbl WHERE owner_mobile_no = %s"
#                 owner_result = select_query(check_owner_query, [owner_mobile_no])
#                 if owner_result:
#                     # Owner exists, get the existing owner ID
#                     owner_id = owner_result[0][0]
#                 else:
#                     # Insert new owner if not found
#                     insert_owner_query = """
#                         INSERT INTO vtpartner.owner_tbl (
#                             owner_name, owner_mobile_no,  city_name, address, profile_photo
#                         ) VALUES (%s, %s, %s,  %s, %s) RETURNING owner_id
#                     """
#                     owner_values = [owner_name, owner_mobile_no,  owner_city_name, owner_address, owner_photo_url]
#                     new_owner_result = insert_query(insert_owner_query, owner_values)
#                     if new_owner_result:
#                         owner_id = new_owner_result[0][0]
#                     else:
#                         raise Exception("Failed to retrieve owner ID from insert operation")
#             except Exception as error:
#                 print("Owner error::", error)
#         print("owner_id::",owner_id)
        
#         query = """
#             UPDATE vtpartner.handymanstbl 
#             SET 
#             driver_first_name = %s,
#             profile_pic = %s,
#             mobile_no = %s,
#             r_lat = %s,
#             r_lng = %s,
#             current_lat = %s,
#             current_lng = %s,
#             recent_online_pic = %s,
#             category_id = %s,
#             vehicle_id = %s,
#             city_id = %s,
#             aadhar_no = %s,
#             pan_card_no = %s,
#             full_address = %s,
#             gender = %s,
#             owner_id = %s,
#             aadhar_card_front = %s,
#             aadhar_card_back = %s,
#             pan_card_front = %s,
#             pan_card_back = %s,
#             license_front = %s,
#             license_back = %s,
#             insurance_image = %s,
#             noc_image = %s,
#             pollution_certificate_image = %s,
#             rc_image = %s,
#             vehicle_image = %s,
#             vehicle_plate_image = %s,
#             driving_license_no = %s,
#             vehicle_plate_no = %s,
#             rc_no = %s,
#             insurance_no = %s,
#             noc_no = %s,
#             vehicle_fuel_type = %s
#             WHERE handyman_id=%s
#         """
#         values = [
#             driver_first_name,
#             profile_pic,
#             mobile_no,
#             r_lat,
#             r_lng,
#             r_lat,
#             r_lng,
#             recent_online_pic,
#             '2',
#             vehicle_id,
#             city_id,
#             aadhar_no,
#             pan_card_no,
#             full_address,
#             gender,
#             owner_id,
#             aadhar_card_front,
#             aadhar_card_back,
#             pan_card_front,
#             pan_card_back,
#             license_front,
#             license_back,
#             insurance_image,
#             noc_image,
#             pollution_certificate_image,
#             rc_image,
#             vehicle_image,
#             vehicle_plate_image,
#             driving_license_no,
#             vehicle_plate_no,
#             rc_no,
#             insurance_no,
#             noc_no,
#             vehicle_fuel_type,
#             handyman_id
#         ]
#         row_count = update_query(query, values)

#         # Send success response
#         return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#     except Exception as err:
#         print("Error executing query", err)
#         return JsonResponse({"message": "Error executing add new faq query"}, status=500)

# @csrf_exempt
# def handyman_aadhar_details_update(request):
#     try:
#         data = json.loads(request.body)
#         #customer_id,customer_name,profile_pic,is_online,ratings,mobile_no,registration_date,time,r_lat,r_lng,current_lat,current_lng,status,full_address,email
#         handyman_id = data.get("handyman_id")
#         aadhar_no = data.get("aadhar_no")
#         aadhar_card_front = data.get("aadhar_card_front")
#         aadhar_card_back = data.get("aadhar_card_back")
       
        
        
        
        
#         # List of required fields
#         required_fields = {
#             "handyman_id":handyman_id,
#             "aadhar_no":aadhar_no,
#             "aadhar_card_front":aadhar_card_front,
#             "aadhar_card_back":aadhar_card_back,
            
#         }

#         # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)

#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )

        
#         query = """
#             UPDATE vtpartner.customers_tbl 
#             SET 
#             aadhar_no = %s,
#             aadhar_card_front = %s,
#             aadhar_card_back = %s,
#             WHERE handyman_id=%s
#         """
#         values = [
#             aadhar_no,
#             aadhar_card_front,
#             aadhar_card_back,
#             handyman_id
#         ]
#         row_count = update_query(query, values)

#         # Send success response
#         return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#     except Exception as err:
#         print("Error executing query", err)
#         return JsonResponse({"message": "Error executing update query"}, status=500)

# @csrf_exempt
# def handyman_pan_card_details_update(request):
#     try:
#         data = json.loads(request.body)

#         handyman_id = data.get("handyman_id")
#         pan_card_no = data.get("pan_card_no")
#         pan_card_front = data.get("pan_card_front")
#         pan_card_back = data.get("pan_card_back")
       
        
        
        
        
#         # List of required fields
#         required_fields = {
#             "handyman_id":handyman_id,
#             "pan_card_no":pan_card_no,
#             "pan_card_front":pan_card_front,
#             "pan_card_back":pan_card_back,
            
#         }

#         # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)

#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )

        
#         query = """
#             UPDATE vtpartner.customers_tbl 
#             SET 
#             pan_card_no = %s,
#             pan_card_front = %s,
#             pan_card_back = %s,
#             WHERE handyman_id=%s
#         """
#         values = [
#             pan_card_no,
#             pan_card_front,
#             pan_card_back,
#             handyman_id
#         ]
#         row_count = update_query(query, values)

#         # Send success response
#         return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#     except Exception as err:
#         print("Error executing query", err)
#         return JsonResponse({"message": "Error executing update query"}, status=500)

# @csrf_exempt
# def handyman_driving_license_details_update(request):
#     try:
#         data = json.loads(request.body)

#         handyman_id = data.get("handyman_id")
#         driving_license_no = data.get("driving_license_no")
#         license_front = data.get("license_front")
#         license_back = data.get("license_back")
       
        
        
        
        
#         # List of required fields
#         required_fields = {
#             "handyman_id":handyman_id,
#             "driving_license_no":driving_license_no,
#             "license_front":license_front,
#             "license_back":license_back,
            
#         }

#         # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)

#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )

        
#         query = """
#             UPDATE vtpartner.customers_tbl 
#             SET 
#             driving_license_no = %s,
#             license_front = %s,
#             license_back = %s,
#             WHERE handyman_id=%s
#         """
#         values = [
#             driving_license_no,
#             license_front,
#             license_back,
#             handyman_id
#         ]
#         row_count = update_query(query, values)

#         # Send success response
#         return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#     except Exception as err:
#         print("Error executing query", err)
#         return JsonResponse({"message": "Error executing update query"}, status=500)

# @csrf_exempt
# def handyman_online_status(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         handyman_id = data.get("handyman_id")

#          # List of required fields
#         required_fields = {
#             "handyman_id": handyman_id,
#         }
#         # Check for missing fields
#          # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#             {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#             status=400
#         )
                
                
#         try:
#             query = """
#             select is_online,status,driver_first_name,recent_online_pic,profile_pic,mobile_no from vtpartner.handymanstbl where handyman_id=%s
#             """
#             params = [handyman_id]
#             result = select_query(query, params)  # Assuming select_query is defined elsewhere

#             if result == []:
#                 return JsonResponse({"message": "No Data Found"}, status=404)
                                
#             # Map the results to a list of dictionaries with meaningful keys
#             response_value = [
#                 {
#                     "is_online": row[0],
#                     "status": row[1],  
#                     "driver_first_name": row[2],  
#                     "recent_online_pic": row[3],  
#                     "profile_pic": row[4],  
#                     "mobile_no": row[5],  
#                 }
#                 for row in result
#             ]
#             # Return customer response
#             return JsonResponse({"results": response_value}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt
# def handyman_update_online_status(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         status = data.get("status")
#         handyman_id = data.get("handyman_id")
#         recent_online_pic = data.get("recent_online_pic")
#         lat = data.get("lat")
#         lng = data.get("lng")

#         # List of required fields
#         required_fields = {
#             "status": status,
#             "handyman_id": handyman_id,
#             "lat": lat,
#             "lng": lng
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
        
#         try:

#             if status == 1:
#                 # Include recent_online_pic in the query when status is 1
#                 query = """
#                 UPDATE vtpartner.handymanstbl 
#                 SET 
#                     is_online = %s,
#                     current_lat = %s,
#                     current_lng = %s,
#                     recent_online_pic = %s
#                 WHERE handyman_id = %s
#                 """
#                 values = [
#                     status,
#                     lat,
#                     lng,
#                     recent_online_pic,
#                     handyman_id
#                 ]
                

#             else:
#                 # Exclude recent_online_pic when status is not 1
#                 query = """
#                 UPDATE vtpartner.handymanstbl 
#                 SET 
#                     is_online = %s,
#                     current_lat = %s,
#                     current_lng = %s
#                 WHERE handyman_id = %s
#                 """
#                 values = [
#                     status,
#                     lat,
#                     lng,
#                     handyman_id
#                 ]

#             # Execute the query
#             row_count = update_query(query, values)
            
#             #insert record in attendance table
#             query_insert = """
#                     INSERT INTO vtpartner.handyman_attendance_tbl(driver_id, time, date, status) 
#                     VALUES (%s, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP), CURRENT_DATE, %s)
#                 """

#             insert_values = [
#                 handyman_id,
#                 status
#             ]
            
#             row_count = insert_query(query_insert,insert_values)

#             # Send success response
#             return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt
# def add_handyman_to_active_drivers_table(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         status = data.get("status")
#         handyman_id = data.get("handyman_id")
#         current_lat = data.get("current_lat")
#         current_lng = data.get("current_lng")

#          # List of required fields
#         required_fields = {
#             "status": status,
#             "handyman_id": handyman_id,
#             "current_lat": current_lat,
#             "current_lng": current_lng,
#         }
#         # Check for missing fields
#          # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#             {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#             status=400
#         )
                
                
#         try:
#             query = """
#             INSERT INTO vtpartner.active_handymantbl 
#             (handyman_id,current_lat,current_lng,current_status)
#             VALUES (%s,%s,%s,%s) RETURNING active_id
#             """
#             values = [
#                 handyman_id,
#                 current_lat,
#                 current_lng,
#                 status,
#             ]
#             new_result = insert_query(query, values)
#             print("new_result::",new_result)
#             if new_result:
#                 print("new_result[0][0]::",new_result[0][0])
#                 active_id = new_result[0][0]
#                 response_value = [
#                     {
#                         "active_id":active_id
#                     }
#                 ]
#                 # Send success response
#                 return JsonResponse({"result": response_value}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt
# def delete_handyman_to_active_drivers_table(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         handyman_id = data.get("handyman_id")
        

#          # List of required fields
#         required_fields = {
#             "handyman_id": handyman_id,
#         }
#         # Check for missing fields
#          # Use the utility function to check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#             {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#             status=400
#         )
                
                
#         try:
#             query = """
#             DELETE FROM vtpartner.active_handymantbl 
#             WHERE handyman_id=%s
#             """
#             values = [
#                 handyman_id,
#             ]
#             row_count = delete_query(query, values)
            
#             # Send success response
#             return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt
# def update_handymans_current_location(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         handyman_id = data.get("handyman_id")
#         lat = data.get("lat")
#         lng = data.get("lng")

#         # List of required fields
#         required_fields = {
#             "handyman_id": handyman_id,
#             "lat": lat,
#             "lng": lng
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
        
#         try:

#             query = """
#                 UPDATE vtpartner.active_handymantbl 
#                 SET 
#                     current_lat = %s,
#                     current_lng = %s
#                 WHERE handyman_id = %s
#                 """
#             values = [
#                     lat,
#                     lng,
#                     handyman_id
#                 ]

#             # Execute the query
#             row_count = update_query(query, values)

#             # Send success response
            
#             return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)


# @csrf_exempt
# def get_nearby_handymans(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         lat = data.get("lat")
#         lng = data.get("lng")
#         city_id = data.get("city_id")
#         price_type = data.get("price_type", 1)
#         radius_km = data.get("radius_km", 5)  # Radius in kilometers

#         if lat is None or lng is None:
#             return JsonResponse({"message": "Latitude and Longitude are required"}, status=400)

#         try:
#             # Haversine formula to calculate the distance in kilometers
#             #SELECT main.active_id, main.handyman_id, main.current_lat, main.current_lng, 
# #        main.entry_time, main.current_status, handymanstbl.driver_first_name,
# #        handymanstbl.profile_pic, vehiclestbl.image AS vehicle_image, 
# #        vehiclestbl.vehicle_name,weight,
# #        (6371 * acos(
# #            cos(radians(%s)) * cos(radians(main.current_lat)) *
# #            cos(radians(main.current_lng) - radians(%s)) +
# #            sin(radians(%s)) * sin(radians(main.current_lat))
# #        )) AS distance
# # FROM vtpartner.active_handymantbl AS main
# # INNER JOIN (
# #     SELECT handyman_id, MAX(entry_time) AS max_entry_time
# #     FROM vtpartner.active_handymantbl
# #     GROUP BY handyman_id
# # ) AS latest ON main.handyman_id = latest.handyman_id
# #               AND main.entry_time = latest.max_entry_time
# # JOIN vtpartner.handymanstbl ON main.handyman_id = handymanstbl.handyman_id
# # JOIN vtpartner.vehiclestbl ON handymanstbl.vehicle_id = vehiclestbl.vehicle_id
# # WHERE main.current_status = 1
# #   AND (6371 * acos(
# #          cos(radians(%s)) * cos(radians(main.current_lat)) *
# #          cos(radians(main.current_lng) - radians(%s)) +
# #          sin(radians(%s)) * sin(radians(main.current_lat))
# #      )) <= %s
# #   AND handymanstbl.category_id = vehiclestbl.category_id
# #   AND handymanstbl.category_id = '1'
# # ORDER BY distance;
# #             """
# #             values = [lat, lng, lat, lat, lng, lat, radius_km]

#             query = """
#             SELECT 
#     main.active_id, 
#     main.handyman_id, 
#     main.current_lat, 
#     main.current_lng, 
#     main.entry_time, 
#     main.current_status, 
#     handymanstbl.driver_first_name,
#     handymanstbl.profile_pic, 
#     vehiclestbl.image AS vehicle_image, 
#     vehiclestbl.vehicle_name,
#     vehiclestbl.weight,
#     vehicle_city_wise_price_tbl.starting_price_per_km,
#     vehicle_city_wise_price_tbl.base_fare,
#     vehiclestbl.vehicle_id,
#     vehiclestbl.size_image,
#     (6371 * acos(
#         cos(radians(%s)) * cos(radians(main.current_lat)) *
#         cos(radians(main.current_lng) - radians(%s)) +
#         sin(radians(%s)) * sin(radians(main.current_lat))
#     )) AS distance
# FROM vtpartner.active_handymantbl AS main
# INNER JOIN (
#     SELECT handyman_id, MAX(entry_time) AS max_entry_time
#     FROM vtpartner.active_handymantbl
#     GROUP BY handyman_id
# ) AS latest ON main.handyman_id = latest.handyman_id
#              AND main.entry_time = latest.max_entry_time
# JOIN vtpartner.handymanstbl ON main.handyman_id = handymanstbl.handyman_id
# JOIN vtpartner.vehiclestbl ON handymanstbl.vehicle_id = vehiclestbl.vehicle_id
# JOIN vtpartner.vehicle_city_wise_price_tbl ON vehiclestbl.vehicle_id = vehicle_city_wise_price_tbl.vehicle_id
# AND vehicle_city_wise_price_tbl.city_id = %s  AND vehicle_city_wise_price_tbl.price_type_id=%s
# WHERE main.current_status = 1
#   AND (6371 * acos(
#         cos(radians(%s)) * cos(radians(main.current_lat)) *
#         cos(radians(main.current_lng) - radians(%s)) +
#         sin(radians(%s)) * sin(radians(main.current_lat))
#       )) <= %s
#   AND handymanstbl.category_id = vehiclestbl.category_id
#   AND handymanstbl.category_id = '1'
# ORDER BY distance;

#             """
#             values = [lat, lng, lat,city_id,price_type, lat, lng, lat, radius_km]

#             # Execute the query
#             nearby_drivers = select_query(query, values)
            

#             # Format response
#             drivers_list = [
#                 {
#                     "active_id": driver[0],
#                     "handyman_id": driver[1],
#                     "latitude": driver[2],
#                     "longitude": driver[3],
#                     "entry_time": driver[4],
#                     "current_status": driver[5],
#                     "driver_name": driver[6],
#                     "driver_profile_pic": driver[7],
#                     "vehicle_image": driver[8],
#                     "vehicle_name": driver[9],
#                     "weight": driver[10],
#                     "starting_price_per_km": driver[11],
#                     "base_fare": driver[12],
#                     "vehicle_id": driver[13],
#                     "size_image": driver[14],
#                     "distance": driver[15]
#                 }
#                 for driver in nearby_drivers
#             ]

#             return JsonResponse({"nearby_drivers": drivers_list}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)




# @csrf_exempt
# def update_firebase_handyman_token(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         handyman_id = data.get("handyman_id")
#         authToken = data.get("authToken")
        

#         # List of required fields
#         required_fields = {
#             "handyman_id": handyman_id,
#             "authToken": authToken,
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
        
#         try:

#             query = """
#                 UPDATE vtpartner.handymanstbl 
#                 SET 
#                     authtoken = %s
#                 WHERE handyman_id = %s
#                 """
#             values = [
#                     authToken,
#                     handyman_id
#                 ]

#             # Execute the query
#             row_count = update_query(query, values)

#             # Send success response
#             return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)


# @csrf_exempt 
# def cab_booking_details_for_ride_acceptance(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         booking_id = data.get("booking_id")
        
        

#         # List of required fields
#         required_fields = {
#             "booking_id": booking_id,
        
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             query = """
#                 select booking_id,bookings_tbl.customer_id,bookings_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,bookings_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,bookings_tbl.city_id,cancelled_reason,cancel_time,order_id,sender_name,sender_number,receiver_name,receiver_number,customer_name,customers_tbl.authtoken,pickup_address,drop_address from vtpartner.bookings_tbl,vtpartner.customers_tbl where customers_tbl.customer_id=bookings_tbl.customer_id and booking_id=%s
#             """
#             result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

#             if result == []:
#                 return JsonResponse({"message": "No Data Found"}, status=404)

#             # Map each row to a dictionary with appropriate keys
#             booking_details = [
#                 {
#                     "booking_id": row[0],
#                     "customer_id": row[1],
#                     "driver_id": row[2],
#                     "pickup_lat": row[3],
#                     "pickup_lng": row[4],
#                     "destination_lat": row[5],
#                     "destination_lng": row[6],
#                     "distance": row[7],
#                     "total_time": row[8],
#                     "total_price": row[9],
#                     "base_price": row[10],
#                     "booking_timing": row[11],
#                     "booking_date": row[12],
#                     "booking_status": row[13],
#                     "driver_arrival_time": row[14],
#                     "otp": row[15],
#                     "gst_amount": row[16],
#                     "igst_amount": row[17],
#                     "goods_type_id": row[18],
#                     "payment_method": row[19],
#                     "city_id": row[20],
#                     "cancelled_reason": row[21],
#                     "cancel_time": row[22],
#                     "order_id": row[23],
#                     "sender_name": row[24],
#                     "sender_number": row[25],
#                     "receiver_name": row[26],
#                     "receiver_number": row[27],
#                     "customer_name": row[28],
#                     "customers_auth_token": row[29],
#                     "pickup_address": row[30],
#                     "drop_address": row[31],
                    
#                 }
#                 for row in result
#             ]

#             return JsonResponse({"results": booking_details}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)


# @csrf_exempt 
# def handyman_booking_accepted(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         booking_id = data.get("booking_id")
#         driver_id = data.get("driver_id")
#         server_token = data.get("server_token")
#         customer_id = data.get("customer_id")
        
        

#         # List of required fields
#         required_fields = {
#             "booking_id": booking_id,
#             "driver_id": driver_id,
#             "server_token": server_token,
#             "customer_id":customer_id
        
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             query = """
#                 select driver_id from vtpartner.bookings_tbl where booking_id=%s and driver_id!='-1'
#             """
#             result = select_query(query,[booking_id])  # Assuming select_query is defined elsewhere

#             if result == []:
#                 #Update booking status and driver assinged
#                 try:

#                     query = """
#                        update vtpartner.bookings_tbl set driver_id=%s ,booking_status='Driver Accepted' where booking_id=%s
#                         """
#                     values = [
#                             driver_id,
#                             booking_id
#                         ]

#                     # Execute the query
#                     row_count = update_query(query, values)
#                     #Inserting record in booking_history table
#                     try:

#                         query = """
#                            insert into vtpartner.bookings_history_tbl (status,booking_id) values ('Driver Accepted',%s)
#                             """
#                         values = [
#                                 booking_id
#                             ]

#                         # Execute the query
#                         row_count = insert_query(query, values)
#                         #Updating driver status to occupied
#                         try:

#                             query = """
#                                update vtpartner.active_handymantbl set current_status='2' where handyman_id=%s
#                                 """
#                             values = [
#                                     driver_id
#                                 ]

#                             # Execute the query
#                             row_count = update_query(query, values)



#                             #get the customer auth token
#                             auth_token = get_customer_auth_token(customer_id)
                            
#                             #send Fcm notification to customer saying driver assigned
#                             customer_data = {
#                                 'intent':'live_tracking',
#                                 'booking_id':str(booking_id)
#                             }
#                             sendFMCMsg(auth_token,'You have been assigned a driver','Driver Assigned',customer_data,server_token)

#                             # Send success response
#                             return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#                         except Exception as err:
#                             print("Error executing query:", err)
#                             return JsonResponse({"message": "An error occurred"}, status=500)
                        
                        
                        

#                     except Exception as err:
#                         print("Error executing query:", err)
#                         return JsonResponse({"message": "An error occurred"}, status=500)

                    

#                 except Exception as err:
#                     print("Error executing updating booking status to accepted:", err)
#                     return JsonResponse({"message": "An error occurred"}, status=500)

#             # Checking if driver is assigned
#             ret_driver_id = result[0][0]
#             return JsonResponse({"message": "No Data Found"}, status=404)
            
#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)


# @csrf_exempt 
# def update_booking_status_handyman(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         booking_id = data.get("booking_id")
#         booking_status = data.get("booking_status")
#         server_token = data.get("server_token")
#         customer_id = data.get("customer_id")

#         # List of required fields
#         required_fields = {
#             "booking_id": booking_id,
#             "booking_status": booking_status,
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:

#             query = """
#                 update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
#                 """
#             values = [
#                     booking_status,
#                     booking_id
#                 ]

#             # Execute the query
#             row_count = update_query(query, values)

#             # Updating Booking History Table
#             try:

#                 query = """
#                     insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
#                     """
#                 values = [
#                         booking_id,
#                         booking_status
#                     ]

#                 # Execute the query
#                 row_count = insert_query(query, values)

#                 # Send success response
#                 auth_token = get_customer_auth_token(customer_id)
#                 body = title = ""
#                 data_map = {}
#                 if booking_status == "Driver Arrived":
#                     body = "Our agent has arrived at your pickup location"
#                     title = "Agent Arrived"
#                 elif booking_status == "OTP Verified":
#                     body = "You're OTP is Verified Successfully!"
#                     title = "OTP Verification"
#                 elif booking_status == "Start Trip":
#                     body = "Trip has been started from your pickup location"
#                     title = "Trip Started"
#                     # Update Pickup epoch here
#                     update_pickup_epoch_query = """
#                     UPDATE vtpartner.bookings_tbl SET pickup_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
#                     """
#                     values = [
#                             booking_id
#                         ]

#                     # Execute the query
#                     row_count = update_query(update_pickup_epoch_query, values)
#                 elif booking_status == "End Trip":
#                     body = "Your package has been delivered successfully"
#                     title = "Package Deliveried"
#                     # Update Drop epoch here
#                     update_drop_epoch_query = """
#                     UPDATE vtpartner.bookings_tbl SET drop_time=EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) where booking_id=%s
#                     """
#                     values = [
#                             booking_id
#                         ]

#                     # Execute the query
#                     row_count = update_query(update_drop_epoch_query, values)
#                 sendFMCMsg(auth_token,body,title,data_map,server_token)
#                 return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#             except Exception as err:
#                 print("Error executing query:", err)
#                 return JsonResponse({"message": "An error occurred"}, status=500)
#             #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)
        

#     return JsonResponse({"message": "Method not allowed"}, status=405)
    
# @csrf_exempt 
# def generate_order_id_for_booking_id_handyman(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         booking_id = data.get("booking_id")
#         driver_id = data.get("driver_id")
#         payment_method = data.get("payment_method")
#         payment_id = data.get("payment_id")
#         booking_status = data.get("booking_status")
#         server_token = data.get("server_token")
#         customer_id = data.get("customer_id")
#         total_amount = data.get("total_amount")
        

#         # List of required fields
#         required_fields = {
#             "booking_id": booking_id,
#             "driver_id": driver_id,
#             "payment_method": payment_method,
#             "payment_id": payment_id,
#             "booking_status": booking_status,
#             "server_token": server_token,
#             "customer_id": customer_id,
#             "total_amount": total_amount,
            
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
        
#         #Updating the Status to Trip Ended
        
#         try:

#             query = """
#                 update vtpartner.bookings_tbl set booking_status=%s where booking_id=%s
#                 """
#             values = [
#                     booking_status,
#                     booking_id
#                 ]

#             # Execute the query
#             row_count = update_query(query, values)

#             # Updating Booking History Table
#             try:

#                 query = """
#                     insert into vtpartner.bookings_history_tbl(booking_id,status) values (%s,%s)
#                     """
#                 values = [
#                         booking_id,
#                         booking_status
#                     ]

#                 # Execute the query
#                 row_count = insert_query(query, values)

#                 # Send success response
#                 auth_token = get_customer_auth_token(customer_id)
#                 body = title = ""
#                 data_map = {}
#                 if booking_status == "Driver Arrived":
#                     body = "Our agent has arrived at your pickup location"
#                     title = "Agent Arrived"
#                 elif booking_status == "OTP verified":
#                     body = "Your trip otp is verified"
#                     title = "Trip OTP Verified"
#                 elif booking_status == "Start Trip":
#                     body = "Trip has been started from your pickup location"
#                     title = "Trip Started"
#                 elif booking_status == "Ongoing":
#                     body = "Trip has been started from your pickup location"
#                     title = "Ongoing"
#                 elif booking_status == "End Trip":
#                     body = "Your package has been delivered successfully"
#                     title = "Package Deliveried"
#                 sendFMCMsg(auth_token,body,title,data_map,server_token)
#                 #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)
                
#                 #Generating Order ID
#                 try:
#                     query = """
#                     INSERT INTO vtpartner.orders_tbl (
#                         customer_id, 
#                         driver_id, 
#                         pickup_lat, 
#                         pickup_lng, 
#                         destination_lat, 
#                         destination_lng, 
#                         distance, 
#                         time, 
#                         total_price, 
#                         base_price, 
#                         booking_timing, 
#                         booking_date, 
#                         booking_status, 
#                         driver_arrival_time, 
#                         otp, 
#                         gst_amount, 
#                         igst_amount, 
#                         goods_type_id, 
#                         payment_method, 
#                         city_id, 
#                         booking_id, 
#                         sender_name, 
#                         sender_number, 
#                         receiver_name, 
#                         receiver_number, 
#                         pickup_address, 
#                         drop_address,
#                         pickup_time,
#                         drop_time
#                     )
#                     SELECT 
#                         customer_id, 
#                         driver_id, 
#                         pickup_lat, 
#                         pickup_lng, 
#                         destination_lat, 
#                         destination_lng, 
#                         distance, 
#                         time, 
#                         total_price, 
#                         base_price, 
#                         booking_timing, 
#                         booking_date, 
#                         booking_status, 
#                         driver_arrival_time, 
#                         otp, 
#                         gst_amount, 
#                         igst_amount, 
#                         goods_type_id, 
#                         payment_method, 
#                         city_id, 
#                         booking_id, 
#                         sender_name, 
#                         sender_number, 
#                         receiver_name, 
#                         receiver_number, 
#                         pickup_address, 
#                         drop_address,
#                         pickup_time,
#                         drop_time
#                     FROM vtpartner.bookings_tbl
#                     WHERE booking_id = %s
#                     RETURNING order_id;
#                     """

                    

#                     # Execute the query
#                     ret_result = insert_query2(query,[booking_id])
#                     #get order_id from here
#                     if ret_result!=None:
#                         order_id = ret_result[0][0]
#                         try:
#                             query2 = """
#                             update vtpartner.active_handymantbl set current_status='1' where handyman_id=%s
#                             """
#                             values2 = [
#                                     driver_id
#                                 ]

#                             # Execute the query
#                             row_count = update_query(query2, values2)
#                             #success
#                             try:
#                                 query3 = """
#                                 update vtpartner.bookings_tbl set booking_completed='1' where booking_id=%s
#                                 """
#                                 values3 = [
#                                         booking_id
#                                     ]

#                                 # Execute the query
#                                 row_count = update_query(query3, values3)
                                
#                                 query_update = """
#                                 update vtpartner.orders_tbl set payment_method=%s,payment_id=%s where order_id=%s
#                                 """
#                                 values_update = [
#                                         payment_method,
#                                         payment_id,
#                                         order_id
#                                     ]

#                                 # Execute the query
#                                 row_count = update_query(query_update, values_update)
                                
#                                 #Adding the amount to driver earnings table
#                                 try:
#                                     query4 = """
#                                     insert into vtpartner.handyman_earningstbl(driver_id,amount,order_id,payment_id,payment_mode) values (%s,%s,%s,%s,%s)
#                                     """
#                                     values4 = [
#                                             driver_id,
#                                             total_amount,
#                                             order_id,
#                                             payment_id,
#                                             payment_method
#                                         ]

#                                     # Execute the query
#                                     row_count = insert_query(query4, values4)
#                                     #success
#                                     #Adding the amount to driver earnings table
#                                     try:
#                                         query5 = """
#                                             UPDATE vtpartner.handyman_topup_recharge_current_points_tbl
#                                             SET 
#                                                 used_points = used_points + %s,
#                                                 remaining_points = CASE 
#                                                     WHEN remaining_points >= %s THEN remaining_points - %s
#                                                     ELSE 0
#                                                 END,
#                                                 negative_points = CASE
#                                                     WHEN remaining_points < %s THEN negative_points + (%s - remaining_points)
#                                                     ELSE negative_points
#                                                 END,
#                                                 last_updated_time = date_part('epoch'::text, CURRENT_TIMESTAMP)
#                                             WHERE driver_id = %s
#                                         """
#                                         values5 = [
#                                                 total_amount,  # %s for updating used_points
#                                                 total_amount,  # %s for checking remaining_points
#                                                 total_amount,  # %s for deducting from remaining_points
#                                                 total_amount,  # %s for checking if negative_points should be updated
#                                                 total_amount,  # %s for calculating difference for negative_points
#                                                 driver_id          # %s for identifying the driver
#                                             ]

#                                         # Execute the query
#                                         row_count = insert_query(query5, values5)
#                                         #success
#                                         return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
#                                     except Exception as err:
#                                         print("Error executing query:", err)
#                                         return JsonResponse({"message": "An error occurred"}, status=500)
#                                 except Exception as err:
#                                     print("Error executing query:", err)
#                                     return JsonResponse({"message": "An error occurred"}, status=500)
                                
#                                     #success
#                                     #return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
#                             except Exception as err:
#                                 print("Error executing query:", err)
#                                 return JsonResponse({"message": "An error occurred"}, status=500)
                            
                            
#                             # return JsonResponse({"message": f"{ret_result} row(s) updated","order_id":order_id}, status=200)
#                         except Exception as err:
#                             print("Error executing query:", err)
#                             return JsonResponse({"message": "An error occurred"}, status=500)
                    

#                 except Exception as err:
#                     print("Error executing query:", err)
#                     return JsonResponse({"message": "An error occurred"}, status=500)
        
#             except Exception as err:
#                         print("Error executing query:", err)
#                         return JsonResponse({"message": "An error occurred"}, status=500) 
        

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)
        

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt 
# def get_handyman_recharge_list(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         category_id = data.get("category_id")
        

#         # List of required fields
#         required_fields = {
#             "category_id": category_id,
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             query = """
#                select recharge_id,amount,points,status,description,valid_days from vtpartner.recharge_plans_tbl where category_id=%s
#                 ORDER BY 
#                     amount ASC
#             """
#             result = select_query(query,[category_id])  # Assuming select_query is defined elsewhere

#             if result == []:
#                 return JsonResponse({"message": "No Data Found"}, status=404)

#             # Map each row to a dictionary with appropriate keys
#             services_details = [
#                 {
#                     "recharge_id": row[0],
#                     "amount": row[1],
#                     "points": row[2],
#                     "status": row[3],
#                     "description": row[4],
#                     "valid_days": row[5]
                    
#                 }
#                 for row in result
#             ]

#             return JsonResponse({"results": services_details}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt 
# def get_handyman_current_recharge_details(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         driver_id = data.get("driver_id")
        

#         # List of required fields
#         required_fields = {
#             "driver_id": driver_id,
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             query = """
#                select topup_id,recharge_id,allotted_points,used_points,remaining_points,negative_points,valid_till_date,status from vtpartner.handyman_topup_recharge_current_points_tbl where driver_id=%s
#             """
#             result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

#             if result == []:
#                 return JsonResponse({"message": "No Data Found"}, status=404)

#             # Map each row to a dictionary with appropriate keys
#             services_details = [
#                 {
#                     "topup_id": row[0],
#                     "recharge_id": row[1],
#                     "allotted_points": row[2],
#                     "used_points": row[3],
#                     "remaining_points": row[4],
#                     "negative_points": row[5],
#                     "valid_till_date": row[6],
#                     "status": row[7]
                    
#                 }
#                 for row in result
#             ]

#             return JsonResponse({"results": services_details}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)


# @csrf_exempt 
# def get_handyman_recharge_history_details(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         driver_id = data.get("driver_id")
        

#         # List of required fields
#         required_fields = {
#             "driver_id": driver_id,
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             query = """
#                select history_id,recharge_id,amount,allotted_points,date,valid_till_date,status,payment_method,payment_id,transaction_type,admin_id,last_recharge_negative_points from vtpartner.handyman_topup_recharge_history_tbl where driver_id=%s
#             """
#             result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

#             if result == []:
#                 return JsonResponse({"message": "No Data Found"}, status=404)

#             # Map each row to a dictionary with appropriate keys
#             services_details = [
#                 {
#                     "history_id": row[0],
#                     "recharge_id": row[1],
#                     "amount": row[2],
#                     "allotted_points": row[3],
#                     "date": row[4],
#                     "valid_till_date": row[5],
#                     "status": row[6],
#                     "payment_method": row[7],
#                     "payment_id": row[8],
#                     "transaction_type": row[9],
#                     "admin_id": row[10],
#                     "last_recharge_negative_points": row[11],
                    
#                 }
#                 for row in result
#             ]

#             return JsonResponse({"results": services_details}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)


# @csrf_exempt 
# def new_handyman_recharge(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         driver_id = data.get("driver_id")
#         topup_id = data.get("topup_id")
#         recharge_id = data.get("recharge_id")
#         amount = data.get("amount")
#         allotted_points = data.get("allotted_points")
#         valid_till_date = data.get("valid_till_date")
#         payment_method = data.get("payment_method")
#         payment_id = data.get("payment_id")
#         negative_points = data.get("previous_negative_points")
#         last_validity_date = data.get("last_validity_date")

#         # List of required fields
#         required_fields = {
#             "driver_id": driver_id,
#             "topup_id": topup_id,
#             "recharge_id": recharge_id,
#             "amount": amount,
#             "allotted_points": allotted_points,
#             "valid_till_date": valid_till_date,
#             "payment_method": payment_method,
#             "payment_id": payment_id,
#             "previous_negative_points": negative_points,
#             "last_validity_date": last_validity_date,
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
#         if negative_points > 0:
#             allotted_points -= negative_points
#         try:
#             query = """
#                 insert into vtpartner.handyman_topup_recharge_history_tbl(driver_id,recharge_id,amount,allotted_points,valid_till_date,payment_method,payment_id,last_recharge_negative_points) values (%s,%s,%s,%s,%s,%s,%s,%s)
#                 """
#             values = [
#                     driver_id,
#                     recharge_id,
#                     amount,
#                     allotted_points,
#                     valid_till_date,
#                     payment_method,
#                     payment_id,
#                     negative_points
#                 ]
#             # Execute the query
#             row_count = insert_query(query, values)
            
#             #checking if recharge has been expired
#             last_validity_date_obj = datetime.strptime(last_validity_date, "%Y-%m-%d")

#             # Get the current date
#             current_date = datetime.now()

#             # Check if last_validity_date is greater than the current date
#             isExpired = False
#             if last_validity_date_obj > current_date:
#                 print("The last validity date is in the future.")
#             else:
#                 isExpired = True
#                 print("The last validity date is today or has passed.")

#             # Updating Booking History Table
#             try:
#                 if negative_points > 0 or isExpired:
#                     query = """
#                     update vtpartner.handyman_topup_recharge_current_points_tbl set recharge_id=%s,allotted_points=%s,valid_till_date=%s,remaining_points=%s,negative_points='0',used_points='0' where topup_id=%s and driver_id=%s
#                     """
#                     values = [
#                             recharge_id,
#                             allotted_points,
#                             valid_till_date,
#                             allotted_points,
#                             topup_id,
#                             driver_id
#                         ]

#                     # Execute the query
#                     row_count = insert_query(query, values)
#                 else:
#                     query = """
#                         INSERT INTO vtpartner.handyman_topup_recharge_current_points_tbl (recharge_id,allotted_points,valid_till_date,driver_id,remaining_points) VALUES (%s,%s,%s,%s,%s)
#                         """
#                     values = [
#                             recharge_id,
#                             allotted_points,
#                             valid_till_date,
#                             driver_id,
#                             allotted_points
#                         ]

#                     # Execute the query
#                     row_count = insert_query(query, values)

                
#                 return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#             except Exception as err:
#                 print("Error executing query:", err)
#                 return JsonResponse({"message": "An error occurred"}, status=500)
#             #return JsonResponse({"message": f"{row_count} row(s) updated"}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "An error occurred"}, status=500)
        

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt 
# def handyman_all_orders(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         driver_id = data.get("driver_id")
        
        

#         # List of required fields
#         required_fields = {
#             "driver_id": driver_id,
        
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             query = """
#                 select booking_id,orders_tbl.customer_id,orders_tbl.driver_id,pickup_lat,pickup_lng,destination_lat,destination_lng,distance,orders_tbl.time,total_price,base_price,booking_timing,booking_date,booking_status,driver_arrival_time,otp,gst_amount,igst_amount,goods_type_id,payment_method,orders_tbl.city_id,order_id,sender_name,sender_number,receiver_name,receiver_number,driver_first_name,handymanstbl.authtoken,customer_name,customers_tbl.authtoken,pickup_address,drop_address,customers_tbl.mobile_no,handymanstbl.mobile_no,vehiclestbl.vehicle_id,vehiclestbl.vehicle_name,vehiclestbl.image,orders_tbl.ratings,orders_tbl.rating_description from vtpartner.vehiclestbl,vtpartner.orders_tbl,vtpartner.handymanstbl,vtpartner.customers_tbl where handymanstbl.handyman_id=orders_tbl.driver_id and customers_tbl.customer_id=orders_tbl.customer_id and orders_tbl.driver_id=%s and  vehiclestbl.vehicle_id=handymanstbl.vehicle_id order by order_id desc
#             """
#             result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

#             if result == []:
#                 return JsonResponse({"message": "No Data Found"}, status=404)

#             # Map each row to a dictionary with appropriate keys
#             booking_details = [
#                 {
#                     "booking_id": str(row[0]),
#                     "customer_id": str(row[1]),
#                     "driver_id": str(row[2]),
#                     "pickup_lat": str(row[3]),
#                     "pickup_lng": str(row[4]),
#                     "destination_lat": str(row[5]),
#                     "destination_lng": str(row[6]),
#                     "distance": str(row[7]),
#                     "total_time": str(row[8]),
#                     "total_price": str(row[9]),
#                     "base_price": str(row[10]),
#                     "booking_timing": str(row[11]),
#                     "booking_date": str(row[12]),
#                     "booking_status": str(row[13]),
#                     "driver_arrival_time": str(row[14]),
#                     "otp": str(row[15]),
#                     "gst_amount": str(row[16]),
#                     "igst_amount": str(row[17]),
#                     "goods_type_id": str(row[18]),
#                     "payment_method": str(row[19]),
#                     "city_id": str(row[20]),
#                     "order_id": str(row[21]),
#                     "sender_name": str(row[22]),
#                     "sender_number": str(row[23]),
#                     "receiver_name": str(row[24]),
#                     "receiver_number": str(row[25]),
#                     "driver_first_name": str(row[26]),
#                     "handyman_auth_token": str(row[27]),
#                     "customer_name": str(row[28]),
#                     "customers_auth_token": str(row[29]),
#                     "pickup_address": str(row[30]),
#                     "drop_address": str(row[31]),
#                     "customer_mobile_no": str(row[32]),
#                     "driver_mobile_no": str(row[33]),
#                     "vehicle_id": str(row[34]),
#                     "vehicle_name": str(row[35]),
#                     "vehicle_image": str(row[36]),
#                     "ratings": str(row[37]),
#                     "rating_description": str(row[38]),
#                 }
#                 for row in result
#             ]

#             return JsonResponse({"results": booking_details}, status=200)


#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt 
# def handyman_whole_year_earnings(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         driver_id = data.get("driver_id")
        
        

#         # List of required fields
#         required_fields = {
#             "driver_id": driver_id,
        
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             query = """
#                 SELECT
#                 months.month_index,
#                 COALESCE(SUM(e.amount), 0.0) AS total_earnings
#             FROM (
#                 SELECT 1 AS month_index UNION ALL
#                 SELECT 2 UNION ALL
#                 SELECT 3 UNION ALL
#                 SELECT 4 UNION ALL
#                 SELECT 5 UNION ALL
#                 SELECT 6 UNION ALL
#                 SELECT 7 UNION ALL
#                 SELECT 8 UNION ALL
#                 SELECT 9 UNION ALL
#                 SELECT 10 UNION ALL
#                 SELECT 11 UNION ALL
#                 SELECT 12
#             ) AS months
#             LEFT JOIN vtpartner.handyman_earningstbl e
#                 ON EXTRACT(MONTH FROM e.earning_date) = months.month_index
#                 AND e.driver_id = %s
#             GROUP BY months.month_index
#             ORDER BY months.month_index;

#             """
#             result = select_query(query,[driver_id])  # Assuming select_query is defined elsewhere

#             if result == []:
#                 return JsonResponse({"message": "No Data Found"}, status=404)

#             # Map each row to a dictionary with appropriate keys
#             earning_details = [
#                 {
#                     "month_index": row[0],
#                     "total_earnings": row[1],
                   
#                 }
#                 for row in result
#             ]

#             return JsonResponse({"results": earning_details}, status=200)


#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)

# @csrf_exempt
# def handyman_todays_earnings(request):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         driver_id = data.get("driver_id")

#         # List of required fields
#         required_fields = {
#             "driver_id": driver_id,
#         }
#         # Check for missing fields
#         missing_fields = check_missing_fields(required_fields)
        
#         # If there are missing fields, return an error response
#         if missing_fields:
#             return JsonResponse(
#                 {"message": f"Missing required fields: {', '.join(missing_fields)}"},
#                 status=400
#             )
            
#         try:
#             # Query to get today's earnings and rides count
#             query = """
#                 SELECT COALESCE(SUM(amount), 0) AS todays_earnings, 
#                        COUNT(*) AS todays_rides 
#                 FROM vtpartner.handyman_earningstbl 
#                 WHERE driver_id = %s AND earning_date = CURRENT_DATE;
#             """
#             result = select_query(query, [driver_id])  # Assuming select_query is defined elsewhere

#             if not result:
#                 return JsonResponse({"message": "No Data Found"}, status=404)

#             # Extract the first row from the result
#             row = result[0]
#             earning_details = {
#                 "todays_earnings": row[0],
#                 "todays_rides": row[1],
#             }

#             return JsonResponse({"results": [earning_details]}, status=200)

#         except Exception as err:
#             print("Error executing query:", err)
#             return JsonResponse({"message": "Internal Server Error"}, status=500)

#     return JsonResponse({"message": "Method not allowed"}, status=405)
