from django.urls import path
from . import views

app_name = 'vt_partner'

urlpatterns = [
    #Send OTP 
    path('send_otp',views.send_otp,name='send_otp'),
    #To get Agent App Request Token HTTp FCM
    path('get_agent_app_firebase_access_token',views.get_agent_app_firebase_access_token,name='get_agent_app_firebase_access_token'),
    #Send OTP 
    path('get_customer_app_firebase_access_token',views.get_customer_app_firebase_access_token,name='get_customer_app_firebase_access_token'),
    #Upload Images
    path('upload',views.upload_image,name='upload'),
    #Customer Saved addresses
    path('add_or_update_customer_address',views.add_or_update_customer_address,name='add_or_update_customer_address'),
    #Customer Login
    path('login',views.login_view,name='login'),
    #New Customer Registration
    path('customer_registration',views.customer_registration,name='customer_registration'),
    #All Coupons
    path('all_coupons',views.all_coupons,name='all_coupons'),
    #Get Customer Details
    path('customer_details',views.customer_details,name='customer_details'),
    #Get Customer Reward Points Details
    path('customer_reward_points_details',views.customer_reward_points_details,name='customer_reward_points_details'),
    #Update Customer Details
    path('update_customer_details',views.update_customer_details,name='update_customer_details'),
    #All Saved Addresses
    path('all_saved_addresses',views.all_saved_addresses,name='all_saved_addresses'),
    #All Services
    path('all_services',views.all_services,name='all_services'),
    #All Cities
    path('all_cities',views.all_cities,name='all_cities'),
    #All Vehicles
    path('all_vehicles',views.all_vehicles,name='all_vehicles'),
    #All Vehicles with category id , price
    path('all_vehicles_with_price_details',views.all_vehicles_with_price_details,name='all_vehicles_with_price_details'),
    #Allowed PinCodes
    path('allowed_pin_code',views.allowed_pin_code,name='allowed_pin_code'),
    #Calculate Distance between 2 place IDs
    path('distance',views.distance,name='distance'),
    #Goods types 
    path('get_all_goods_types',views.get_all_goods_types,name='get_all_goods_types'),
    #GuideLines according to category
    path('get_all_guide_lines',views.get_all_guide_lines,name='get_all_guide_lines'),
    #New Goods Delivery Booking
    path('new_goods_delivery_booking',views.new_goods_delivery_booking,name='new_goods_delivery_booking'),
    #Search Near by Goods Driver for vehicle id to send notification to accept booking
    path('search_nearby_drivers',views.search_nearby_drivers,name='search_nearby_drivers'),
    #Update Customer Firebase Token
    path('update_firebase_customer_token',views.update_firebase_customer_token,name='update_firebase_customer_token'),
    #Tracking the Booking
    path('booking_details_live_track',views.booking_details_live_track,name='booking_details_live_track'),
    #Tracking the Booking
    path('get_goods_driver_current_booking_detail',views.get_goods_driver_current_booking_detail,name='get_goods_driver_current_booking_detail'),
    #Customers All Bookings
    path('customers_all_bookings',views.customers_all_bookings,name='customers_all_bookings'),
    #Customers All Cab Bookings
    path('customers_all_cab_bookings',views.customers_all_cab_bookings,name='customers_all_cab_bookings'),
    #Customers All Other Driver Bookings
    path('customers_all_other_driver_bookings',views.customers_all_other_driver_bookings,name='customers_all_other_driver_bookings'),
    #Customers All JCB Crane Bookings
    path('customers_all_jcb_crane_bookings',views.customers_all_jcb_crane_bookings,name='customers_all_jcb_crane_bookings'),
    #Customers All Handymans Bookings
    path('customers_all_handyman_bookings',views.customers_all_handyman_bookings,name='customers_all_handyman_bookings'),
    path('get_cab_driver_current_booking_detail',views.get_cab_driver_current_booking_detail,name='get_cab_driver_current_booking_detail'),
    #Customers All Orders
    path('customers_all_orders',views.customers_all_orders,name='customers_all_orders'),
    #Customers All Cab Orders
    path('customers_all_cab_orders',views.customers_all_cab_orders,name='customers_all_cab_orders'),
    #Customers All JCB Crane Orders
    path('customers_all_jcb_crane_orders',views.customers_all_jcb_crane_orders,name='customers_all_jcb_crane_orders'),
    #Customers All Other Drivers Orders
    path('customers_all_other_driver_orders',views.customers_all_other_driver_orders,name='customers_all_other_driver_orders'),
    #Customers All Handymans Orders
    path('customers_all_handyman_orders',views.customers_all_handyman_orders,name='customers_all_handyman_orders'),
    #Get Order Details
    path('goods_order_details',views.goods_order_details,name='goods_order_details'),
    #Get Cab Order Details
    path('cab_order_details',views.cab_order_details,name='cab_order_details'),
    #Get JCB Crane Order Details
    path('jcb_crane_order_details',views.jcb_crane_order_details,name='jcb_crane_order_details'),
    #Get Other Driver Order Details
    path('other_driver_order_details',views.other_driver_order_details,name='other_driver_order_details'),
    #Get Other HandyMan Order Details
    path('handyman_order_details',views.handyman_order_details,name='handyman_order_details'),
    #Cancel Booking
    path('cancel_booking',views.cancel_booking,name='cancel_booking'),
    #Cancel Cab Booking
    path('cancel_cab_booking',views.cancel_cab_booking,name='cancel_cab_booking'),
    #Save Order Review
    path('save_order_ratings',views.save_order_ratings,name='save_order_ratings'),
    #Save Cab Order Review
    path('save_cab_order_ratings',views.save_cab_order_ratings,name='save_cab_order_ratings'),
    #Save Cab Order Review
    path('save_jcb_crane_order_ratings',views.save_jcb_crane_order_ratings,name='save_jcb_crane_order_ratings'),
    #Save Cab Order Review
    path('save_other_driver_order_ratings',views.save_other_driver_order_ratings,name='save_other_driver_order_ratings'),
    #Save Cab Order Review
    path('save_handyman_order_ratings',views.save_handyman_order_ratings,name='save_handyman_order_ratings'),
    #Goods Driver Live Location Tracking
    path('goods_driver_current_location',views.goods_driver_current_location,name='goods_driver_current_location'),
    #Cab Driver Live Location Tracking
    path('cab_driver_current_location',views.cab_driver_current_location,name='cab_driver_current_location'),
    # Other Driver Live Location Tracking
    path('other_driver_current_location',views.other_driver_current_location,name='other_driver_current_location'),
    # JCB Crane Driver Live Location Tracking
    path('jcb_crane_driver_current_location',views.jcb_crane_driver_current_location,name='jcb_crane_driver_current_location'),
    # Handy Man Agent Live Location Tracking
    path('handyman_agent_current_location',views.handyman_agent_current_location,name='handyman_agent_current_location'),
    #Getting all sub categories such as Plumber , LVM
    path('get_all_sub_categories',views.get_all_sub_categories,name='get_all_sub_categories'),
    #Getting all sub services such as Wireman
    path('get_all_sub_services',views.get_all_sub_services,name='get_all_sub_services'),
    #Generate New Booking Id For Goods Delivery booking with send notification to all driver for specific vehicle id
    path('generate_new_goods_drivers_booking_id_get_nearby_drivers_with_fcm_token',views.generate_new_goods_drivers_booking_id_get_nearby_drivers_with_fcm_token,name='generate_new_goods_drivers_booking_id_get_nearby_drivers_with_fcm_token'),
    #Getting customer details
    path('get_customer_details',views.get_customer_details,name='get_customer_details'),
    #Update customer details
    path('update_customer_details',views.update_customer_details,name='update_customer_details'),
    #Update customer details
    path('get_peak_hour_prices',views.get_peak_hour_prices,name='get_peak_hour_prices'),
    
    
    
    
    
    
    
    
    
    #Goods Driver Api's URLs
    #Login
    path('get_goods_driver_details',views.get_goods_driver_details,name='get_goods_driver_details'),
    #Login
    path('update_goods_driver_details',views.update_goods_driver_details,name='update_goods_driver_details'),
    #Login
    path('goods_driver_login',views.goods_driver_login_view,name='goods_driver_login'),
    #Goods Driver Verified all details to show edit option
    path('get_goods_driver_details',views.get_goods_driver_details,name='get_goods_driver_details'),
    #Registration 
    path('goods_driver_registration',views.goods_driver_registration,name='goods_driver_registration'),
    #Status Verification 
    path('goods_driver_online_status',views.goods_driver_online_status,name='goods_driver_online_status'),
    #Update Online Status Verification 
    path('goods_driver_update_online_status',views.goods_driver_update_online_status,name='goods_driver_update_online_status'),
    #Insert new record in active goods driver table  
    path('add_new_active_goods_driver',views.add_goods_driver_to_active_drivers_table,name='add_new_active_goods_driver'),
    #Delete record in active goods driver table once driver wants to go offline
    path('delete_active_goods_driver',views.delete_goods_driver_to_active_drivers_table,name='delete_active_goods_driver'),
    #Update Drivers Current Location until he goes offline
    path('update_goods_drivers_current_location',views.update_goods_drivers_current_location,name='update_goods_drivers_current_location'),
    #Get Near By Drivers
    path('get_nearby_drivers',views.get_nearby_drivers,name='get_nearby_drivers'),
    #Update Goods Driver Firebase Token
    path('update_firebase_goods_driver_token',views.update_firebase_goods_driver_token,name='update_firebase_goods_driver_token'),
    #Booking Details for ride acceptance
    path('booking_details_for_ride_acceptance',views.booking_details_for_ride_acceptance,name='booking_details_for_ride_acceptance'),
    #Booking accepted
    path('goods_driver_booking_accepted',views.goods_driver_booking_accepted,name='goods_driver_booking_accepted'),
    #Update Booking Status Done by Goods Driver
    path('update_booking_status_driver',views.update_booking_status_driver,name='update_booking_status_driver'),
    #Generate Order Id after successful delivery completed
    path('generate_order_id_for_booking_id_goods_driver',views.generate_order_id_for_booking_id_goods_driver,name='generate_order_id_for_booking_id_goods_driver'),
    #Get Goods Driver Current Recharge and points details
    path('get_goods_driver_current_recharge_details',views.get_goods_driver_current_recharge_details,name='get_goods_driver_current_recharge_details'),
    #Get Goods Driver Recharge history details
    path('get_goods_driver_recharge_history_details',views.get_goods_driver_recharge_history_details,name='get_goods_driver_recharge_history_details'),
    #Get Goods Driver Recharge List 
    path('get_goods_driver_recharge_list',views.get_goods_driver_recharge_list,name='get_goods_driver_recharge_list'),
    #Get Goods Driver New Recharge Plan List 
    path('get_goods_driver_new_recharge_plans_list',views.get_goods_driver_new_recharge_plans_list,name='get_goods_driver_new_recharge_plans_list'),
    #Get Goods Driver New Recharge Plan History List 
    path('get_goods_driver_new_recharge_plan_history_list',views.get_goods_driver_new_recharge_plan_history_list,name='get_goods_driver_new_recharge_plan_history_list'),
    #Insert the Goods Driver New Recharge
    path('new_goods_driver_recharge',views.new_goods_driver_recharge,name='new_goods_driver_recharge'),
    #Insert the Goods Driver New Recharge Plans
    path('new_goods_driver_new_recharge_plan',views.new_goods_driver_new_recharge_plan,name='new_goods_driver_new_recharge_plan'),
    #My All Rides
    path('goods_driver_all_orders',views.goods_driver_all_orders,name='goods_driver_all_orders'),
    #My Whole Years Earnings
    path('goods_driver_whole_year_earnings',views.goods_driver_whole_year_earnings,name='goods_driver_whole_year_earnings'),
    #My All Rides
    path('goods_driver_todays_earnings',views.goods_driver_todays_earnings,name='goods_driver_todays_earnings'),
    #getting goods driver current new day wise recharge details
    path('goods_driver_current_new_recharge_details',views.goods_driver_current_new_recharge_details,name='goods_driver_current_new_recharge_details'),
    #getting goods driver faq 
    path('get_faqs_by_category',views.get_faqs_by_category,name='get_faqs_by_category'),
    #getting all banners
    path('get_all_banners',views.get_all_banners,name='get_all_banners'),
    
    
    #Cab Driver Api's URLs
    #Login
    path('cab_driver_login',views.cab_driver_login_view,name='cab_driver_login'),
    #Registration 
    path('cab_driver_registration',views.cab_driver_registration,name='cab_driver_registration'),
    #Status Verification 
    path('cab_driver_online_status',views.cab_driver_online_status,name='cab_driver_online_status'),
    #Update Online Status Verification 
    path('cab_driver_update_online_status',views.cab_driver_update_online_status,name='cab_driver_update_online_status'),
    #Insert new record in active goods driver table  
    path('add_new_active_cab_driver',views.add_cab_driver_to_active_drivers_table,name='add_new_active_cab_driver'),
    #Delete record in active goods driver table once driver wants to go offline
    path('delete_active_cab_driver',views.delete_cab_driver_to_active_drivers_table,name='delete_active_cab_driver'),
    #Update Drivers Current Location until he goes offline
    path('update_cab_drivers_current_location',views.update_cab_drivers_current_location,name='update_cab_drivers_current_location'),
    #Get Near By Cab Drivers
    path('get_nearby_cab_drivers',views.get_nearby_cab_drivers,name='get_nearby_cab_drivers'),
    #Update Goods Driver Firebase Token
    path('update_firebase_cab_driver_token',views.update_firebase_cab_driver_token,name='update_firebase_cab_driver_token'),
    #Booking Details for ride acceptance
    path('cab_booking_details_for_ride_acceptance',views.cab_booking_details_for_ride_acceptance,name='cab_booking_details_for_ride_acceptance'),
    #Booking accepted
    path('cab_driver_booking_accepted',views.cab_driver_booking_accepted,name='cab_driver_booking_accepted'),
    #Update Booking Status Done by Goods Driver
    path('update_booking_status_cab_driver',views.update_booking_status_cab_driver,name='update_booking_status_cab_driver'),
    #Generate Order Id after successful delivery completed
    path('generate_order_id_for_booking_id_cab_driver',views.generate_order_id_for_booking_id_cab_driver,name='generate_order_id_for_booking_id_cab_driver'),
    #Get Goods Driver Current Recharge and points details
    path('get_cab_driver_current_recharge_details',views.get_cab_driver_current_recharge_details,name='get_cab_driver_current_recharge_details'),
    #Get Goods Driver Recharge history details
    path('get_cab_driver_recharge_history_details',views.get_cab_driver_recharge_history_details,name='get_cab_driver_recharge_history_details'),
    #Get Goods Driver Recharge List 
    path('get_cab_driver_recharge_list',views.get_cab_driver_recharge_list,name='get_cab_driver_recharge_list'),
    #Insert the Goods Driver New Recharge
    path('new_cab_driver_recharge',views.new_cab_driver_recharge,name='new_cab_driver_recharge'),
    #My All Rides
    path('cab_driver_all_orders',views.cab_driver_all_orders,name='cab_driver_all_orders'),
    #My Whole Years Earnings
    path('cab_driver_whole_year_earnings',views.cab_driver_whole_year_earnings,name='cab_driver_whole_year_earnings'),
    #My All Rides
    path('cab_driver_todays_earnings',views.cab_driver_todays_earnings,name='cab_driver_todays_earnings'),
    #All Cab Drivers Nearby
    path('generate_new_cab_drivers_booking_id_get_nearby_drivers_with_fcm_token',views.generate_new_cab_drivers_booking_id_get_nearby_drivers_with_fcm_token,name='generate_new_cab_drivers_booking_id_get_nearby_drivers_with_fcm_token'),
    #All Cab Drivers Nearby
    path('cab_booking_details_live_track',views.cab_booking_details_live_track,name='cab_booking_details_live_track'),
    
    #Other Drivers Api's URLs
    #Login
    path('other_driver_login',views.other_driver_login_view,name='other_driver_login'),
    #Registration 
    path('other_driver_registration',views.other_driver_registration,name='other_driver_registration'),
    #Status Verification 
    path('other_driver_online_status',views.other_driver_online_status,name='other_driver_online_status'),
    #Update Online Status Verification 
    path('other_driver_update_online_status',views.other_driver_update_online_status,name='other_driver_update_online_status'),
    #Insert new record in active goods driver table  
    path('add_new_active_other_driver',views.add_other_driver_to_active_drivers_table,name='add_new_active_other_driver'),
    #Delete record in active goods driver table once driver wants to go offline
    path('delete_active_other_driver',views.delete_other_driver_to_active_drivers_table,name='delete_active_other_driver'),
    #Update Drivers Current Location until he goes offline
    path('update_other_drivers_current_location',views.update_other_drivers_current_location,name='update_other_drivers_current_location'),
    #Get Near By Cab Drivers
    path('get_nearby_other_drivers',views.get_nearby_other_drivers,name='get_nearby_other_drivers'),
    #Update Goods Driver Firebase Token
    path('update_firebase_other_driver_token',views.update_firebase_other_driver_token,name='update_firebase_other_driver_token'),
    #Booking Details for ride acceptance
    path('other_driver_booking_details_for_ride_acceptance',views.other_driver_booking_details_for_ride_acceptance,name='other_driver_booking_details_for_ride_acceptance'),
    #Booking accepted
    path('other_driver_booking_accepted',views.other_driver_booking_accepted,name='other_driver_booking_accepted'),
    #Update Booking Status Done by Goods Driver
    path('update_booking_status_other_driver',views.update_booking_status_other_driver,name='update_booking_status_other_driver'),
    #Generate Order Id after successful delivery completed
    path('generate_order_id_for_booking_id_other_driver',views.generate_order_id_for_booking_id_other_driver,name='generate_order_id_for_booking_id_other_driver'),
    #Get Goods Driver Current Recharge and points details
    path('get_other_driver_current_recharge_details',views.get_other_driver_current_recharge_details,name='get_other_driver_current_recharge_details'),
    #Get Goods Driver Recharge history details
    path('get_other_driver_recharge_history_details',views.get_other_driver_recharge_history_details,name='get_other_driver_recharge_history_details'),
    #Get Goods Driver Recharge List 
    path('get_other_driver_recharge_list',views.get_other_driver_recharge_list,name='get_other_driver_recharge_list'),
    #Insert the Goods Driver New Recharge
    path('new_other_driver_recharge',views.new_other_driver_recharge,name='new_other_driver_recharge'),
    #My All Rides
    path('other_driver_all_orders',views.other_driver_all_orders,name='other_driver_all_orders'),
    #My All Rides
    path('handyman_agent_all_orders',views.handyman_agent_all_orders,name='handyman_agent_all_orders'),
    #My Whole Years Earnings
    path('other_driver_whole_year_earnings',views.other_driver_whole_year_earnings,name='other_driver_whole_year_earnings'),
    #My All Rides
    path('other_driver_todays_earnings',views.other_driver_todays_earnings,name='other_driver_todays_earnings'),
    #My All Rides
    path('cancel_other_driver_booking',views.cancel_other_driver_booking,name='cancel_other_driver_booking'),
    #My All Rides
    path('other_driver_booking_details_live_track',views.other_driver_booking_details_live_track,name='other_driver_booking_details_live_track'),
    #Generating new booking id and sending notifications to nearby agents
    path('generate_new_other_driver_booking_id_get_nearby_agents_with_fcm_token',views.generate_new_other_driver_booking_id_get_nearby_agents_with_fcm_token,name='generate_new_other_driver_booking_id_get_nearby_agents_with_fcm_token'),
    
    #JCB Crane Drivers Api's URLs
    #Login
    path('jcb_crane_driver_login',views.jcb_crane_driver_login_view,name='jcb_crane_driver_login'),
    #Registration 
    path('jcb_crane_driver_registration',views.jcb_crane_driver_registration,name='jcb_crane_driver_registration'),
    #Status Verification 
    path('jcb_crane_driver_online_status',views.jcb_crane_driver_online_status,name='jcb_crane_driver_online_status'),
    #Update Online Status Verification 
    path('jcb_crane_driver_update_online_status',views.jcb_crane_driver_update_online_status,name='jcb_crane_driver_update_online_status'),
    #Insert new record in active goods driver table  
    path('add_new_active_jcb_crane_driver',views.add_jcb_crane_driver_to_active_drivers_table,name='add_new_active_jcb_crane_driver'),
    #Delete record in active goods driver table once driver wants to go offline
    path('delete_active_jcb_crane_driver',views.delete_jcb_crane_driver_to_active_drivers_table,name='delete_active_jcb_crane_driver'),
    #Update Drivers Current Location until he goes offline
    path('update_jcb_crane_drivers_current_location',views.update_jcb_crane_drivers_current_location,name='update_jcb_crane_drivers_current_location'),
    #Get Near By Cab Drivers
    path('get_nearby_jcb_crane_drivers',views.get_nearby_jcb_crane_drivers,name='get_nearby_jcb_crane_drivers'),
    #Update Goods Driver Firebase Token
    path('update_firebase_jcb_crane_driver_token',views.update_firebase_jcb_crane_driver_token,name='update_firebase_jcb_crane_driver_token'),
    #Booking Details for ride acceptance
    path('jcb_crane_booking_details_for_ride_acceptance',views.jcb_crane_booking_details_for_ride_acceptance,name='jcb_crane_booking_details_for_ride_acceptance'),
    #Booking accepted
    path('jcb_crane_driver_booking_accepted',views.jcb_crane_driver_booking_accepted,name='jcb_crane_driver_booking_accepted'),
    #Update Booking Status Done by Goods Driver
    path('update_booking_status_jcb_crane_driver',views.update_booking_status_jcb_crane_driver,name='update_booking_status_jcb_crane_driver'),
    #Generate Order Id after successful delivery completed
    path('generate_order_id_for_booking_id_jcb_crane_driver',views.generate_order_id_for_booking_id_jcb_crane_driver,name='generate_order_id_for_booking_id_jcb_crane_driver'),
    #Get Goods Driver Current Recharge and points details
    path('get_jcb_crane_driver_current_recharge_details',views.get_jcb_crane_driver_current_recharge_details,name='get_jcb_crane_driver_current_recharge_details'),
    #Get Goods Driver Recharge history details
    path('get_jcb_crane_driver_recharge_history_details',views.get_jcb_crane_driver_recharge_history_details,name='get_jcb_crane_driver_recharge_history_details'),
    #Get Goods Driver Recharge List 
    path('get_jcb_crane_driver_recharge_list',views.get_jcb_crane_driver_recharge_list,name='get_jcb_crane_driver_recharge_list'),
    #Insert the Goods Driver New Recharge
    path('new_jcb_crane_driver_recharge',views.new_jcb_crane_driver_recharge,name='new_jcb_crane_driver_recharge'),
    #My All Rides
    path('cancel_jcb_crane_driver_booking',views.cancel_jcb_crane_driver_booking,name='cancel_jcb_crane_driver_booking'),
    #My All Rides
    path('jcb_crane_driver_all_orders',views.jcb_crane_driver_all_orders,name='jcb_crane_driver_all_orders'),
    #My Whole Years Earnings
    path('jcb_crane_driver_whole_year_earnings',views.jcb_crane_driver_whole_year_earnings,name='jcb_crane_driver_whole_year_earnings'),
    #My All Rides
    path('jcb_crane_driver_todays_earnings',views.jcb_crane_driver_todays_earnings,name='jcb_crane_driver_todays_earnings'),
    #My All Rides
    path('jcb_crane_driver_booking_details_live_track',views.jcb_crane_driver_booking_details_live_track,name='jcb_crane_driver_booking_details_live_track'),
    #My Near by agents and generating booking id
    path('generate_new_jcb_crane_booking_id_get_nearby_agents_with_fcm_token',views.generate_new_jcb_crane_booking_id_get_nearby_agents_with_fcm_token,name='generate_new_jcb_crane_booking_id_get_nearby_agents_with_fcm_token'),
    
    
    #Handy Man Agents Api's URLs
    #Login
    path('handyman_login',views.handyman_login_view,name='handyman_login'),
    #Registration 
    path('handyman_registration',views.handyman_registration,name='handyman_registration'),
    #Status Verification 
    path('handyman_online_status',views.handyman_online_status,name='handyman_online_status'),
    #Update Online Status Verification 
    path('handyman_update_online_status',views.handyman_update_online_status,name='handyman_update_online_status'),
    #Insert new record in active goods driver table  
    path('add_new_active_handyman',views.add_handyman_to_active_drivers_table,name='add_new_active_handyman'),
    #Delete record in active goods driver table once driver wants to go offline
    path('delete_active_handyman',views.delete_handyman_to_active_drivers_table,name='delete_active_handyman'),
    #Update Drivers Current Location until he goes offline
    path('update_handymans_current_location',views.update_handymans_current_location,name='update_handymans_current_location'),
    #Get Near By Cab Drivers
    path('get_nearby_handymans',views.get_nearby_handymans,name='get_nearby_handymans'),
    #Update Goods Driver Firebase Token
    path('update_firebase_handyman_token',views.update_firebase_handyman_token,name='update_firebase_handyman_token'),
    #Booking Details for ride acceptance
    path('handyman_agent_booking_details_for_ride_acceptance',views.handyman_agent_booking_details_for_ride_acceptance,name='handyman_agent_booking_details_for_ride_acceptance'),
    #Booking accepted
    path('handyman_booking_accepted',views.handyman_booking_accepted,name='handyman_booking_accepted'),
    #Update Booking Status Done by Goods Driver
    path('update_booking_status_handyman',views.update_booking_status_handyman,name='update_booking_status_handyman'),
    #Generate Order Id after successful delivery completed
    path('generate_order_id_for_booking_id_handyman',views.generate_order_id_for_booking_id_handyman,name='generate_order_id_for_booking_id_handyman'),
    #Get Goods Driver Current Recharge and points details
    path('get_handyman_current_recharge_details',views.get_handyman_current_recharge_details,name='get_handyman_current_recharge_details'),
    #Get Goods Driver Recharge history details
    path('get_handyman_recharge_history_details',views.get_handyman_recharge_history_details,name='get_handyman_recharge_history_details'),
    #Get Goods Driver Recharge List 
    path('get_handyman_recharge_list',views.get_handyman_recharge_list,name='get_handyman_recharge_list'),
    #Insert the Goods Driver New Recharge
    path('new_handyman_recharge',views.new_handyman_recharge,name='new_handyman_recharge'),
    #Insert the Goods Driver New Recharge
    path('cancel_handyman_agent_booking',views.cancel_handyman_agent_booking,name='cancel_handyman_agent_booking'),
    #My All Rides
    path('handyman_all_orders',views.handyman_all_orders,name='handyman_all_orders'),
    #My Whole Years Earnings
    path('handyman_whole_year_earnings',views.handyman_whole_year_earnings,name='handyman_whole_year_earnings'),
    #My All Rides
    path('handyman_todays_earnings',views.handyman_todays_earnings,name='handyman_todays_earnings'),
    #My All Rides
    path('handyman_agent_booking_details_live_track',views.handyman_agent_booking_details_live_track,name='handyman_agent_booking_details_live_track'),
    #Generating Booking ID and searching nearby handyman agents
    path('generate_new_handyman_booking_id_get_nearby_agents_with_fcm_token',views.generate_new_handyman_booking_id_get_nearby_agents_with_fcm_token,name='generate_new_handyman_booking_id_get_nearby_agents_with_fcm_token'),
]