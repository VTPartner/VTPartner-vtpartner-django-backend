from django.urls import path
from . import views

app_name = 'vt_partner'

urlpatterns = [
    #Upload Images
    path('upload',views.upload_image,name='upload'),
    #Customer Login
    path('login',views.login_view,name='login'),
    #New Customer Registration
    path('customer_registration',views.customer_registration,name='customer_registration'),
    #All Services
    path('all_services',views.all_services,name='all_services'),
    #All Cities
    path('all_cities',views.all_cities,name='all_cities'),
    #All Vehicles
    path('all_vehicles',views.all_vehicles,name='all_vehicles'),
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
    #Customers All Bookings
    path('customers_all_bookings',views.customers_all_bookings,name='customers_all_bookings'),
    #Customers All Orders
    path('customers_all_orders',views.customers_all_orders,name='customers_all_orders'),
    #Get Order Details
    path('goods_order_details',views.goods_order_details,name='goods_order_details'),
    #Save Order Review
    path('save_order_ratings',views.save_order_ratings,name='save_order_ratings'),
    #Goods Driver Live Location Tracking
    path('goods_driver_current_location',views.goods_driver_current_location,name='goods_driver_current_location'),
    
    
    
    
    
    
    
    #Goods Driver Api's URLs
    #Login
    path('goods_driver_login',views.goods_driver_login_view,name='goods_driver_login'),
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
]