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
#Get Near By Drivers
path('get_nearby_cab_drivers',views.get_nearby_cab_drivers,name='get_nearby_cab_drivers'),
#Update Goods Driver Firebase Token
path('update_firebase_cab_driver_token',views.update_firebase_cab_driver_token,name='update_firebase_cab_driver_token'),
#Booking Details for ride acceptance
path('booking_details_for_ride_acceptance',views.booking_details_for_ride_acceptance,name='booking_details_for_ride_acceptance'),
#Booking accepted
path('cab_driver_booking_accepted',views.cab_driver_booking_accepted,name='cab_driver_booking_accepted'),
#Update Booking Status Done by Goods Driver
path('update_booking_status_driver',views.update_booking_status_driver,name='update_booking_status_driver'),
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