from django.urls import path
from . import views

app_name = 'dashboard_app'

urlpatterns = [
    #Admin Login
    path('login',views.login_view,name='login'),
    #Admin Login
    path('upload',views.upload_image,name='upload'),
    #Admin Login
    path('all_branches',views.all_branches,name='all_branches'),
    #Admin Login
    path('all_allowed_cities',views.all_allowed_cities,name='all_allowed_cities'),
    #Admin Login
    path('update_allowed_city',views.update_allowed_city,name='update_allowed_city'),
    #Admin Login
    path('add_new_allowed_city',views.add_new_allowed_city,name='add_new_allowed_city'),
    #Admin Login
    path('all_allowed_pincodes',views.all_allowed_pincodes,name='all_allowed_pincodes'),
    #Admin Login
    path('add_new_pincode',views.add_new_pincode,name='add_new_pincode'),
    #Admin Login
    path('edit_pincode',views.edit_pincode,name='edit_pincode'),
    #Admin Login
    path('service_types',views.service_types,name='service_types'),
    #Admin Login
    path('all_services',views.all_services,name='all_services'),
    #Admin Login
    path('add_service',views.add_service,name='add_service'),
    #Admin Login
    path('edit_service',views.edit_service,name='edit_service'),
    #Admin Login
    path('vehicle_types',views.vehicle_types,name='vehicle_types'),
    #Admin Login
    path('all_vehicles',views.all_vehicles,name='all_vehicles'),
    #Admin Login
    path('add_vehicle',views.add_vehicle,name='add_vehicle'),
    #Admin Login
    path('edit_vehicle',views.edit_vehicle,name='edit_vehicle'),
    #Admin Login
    path('vehicle_prices',views.vehicle_prices,name='vehicle_prices'),
    #Admin Login
    path('vehicle_price_types',views.vehicle_price_types,name='vehicle_price_types'),
    #Admin Login
    path('add_vehicle_price',views.add_vehicle_price,name='add_vehicle_price'),
    #Admin Login
    path('edit_vehicle_price',views.edit_vehicle_price,name='edit_vehicle_price'),
    #Getting All Customers
    path('get_all_customers',views.get_all_customers,name='get_all_customers'),
    #Getting Peak Hours Prices
    path('get_peak_hour_prices',views.get_peak_hour_prices,name='get_peak_hour_prices'),
    #Editing peak hour prices
    path('edit_peak_hour_price',views.edit_peak_hour_price,name='edit_peak_hour_price'),
    #Adding peak hour prices
    path('add_peak_hour_price',views.add_peak_hour_price,name='add_peak_hour_price'),
    #Adding peak hour prices 
    path('get_all_banners',views.get_all_banners,name='get_all_banners'),
    #Adding peak hour prices
    path('delete_banner',views.delete_banner,name='delete_banner'),
    #Adding peak hour prices
    path('edit_banner',views.edit_banner,name='edit_banner'),
    #Adding peak hour prices
    path('add_banner',views.add_banner,name='add_banner'),
    
    #Adding peak hour prices
    path('add_coupon',views.add_coupon,name='add_coupon'),
    #Adding peak hour prices
    path('edit_coupon',views.edit_coupon,name='edit_coupon'),
    #Adding peak hour prices
    path('delete_coupon',views.delete_coupon,name='delete_coupon'),
    #Adding peak hour prices
    path('get_all_coupons',views.get_all_coupons,name='get_all_coupons'),
    
    #Reports
    path('get_orders_report',views.get_orders_report,name='get_orders_report'),
    
    #Admin Login
    path('all_sub_categories',views.all_sub_categories,name='all_sub_categories'),
    #Admin Login
    path('add_sub_category',views.add_sub_category,name='add_sub_category'),
    #Admin Login
    path('edit_sub_category',views.edit_sub_category,name='edit_sub_category'),
    #Admin Login
    path('all_other_services',views.all_other_services,name='all_other_services'),
    #Admin Login
    path('add_other_service',views.add_other_service,name='add_other_service'),
    #Admin Login
    path('edit_other_service',views.edit_other_service,name='edit_other_service'),
    #Admin Login
    path('all_enquiries',views.all_enquiries,name='all_enquiries'),
    #Admin Login
    path('all_gallery_images',views.all_gallery_images,name='all_gallery_images'),
    #Admin Login
    path('add_gallery_image',views.add_gallery_image,name='add_gallery_image'),
    #Admin Login
    path('edit_gallery_image',views.edit_gallery_image,name='edit_gallery_image'),
    #Admin Login
    path('enquiries_all',views.enquiries_all,name='enquiries_all'),
    #Admin Login
    path('register_agent',views.register_agent,name='register_agent'),
    #Admin Login
    path('check_driver_existence',views.check_driver_existence,name='check_driver_existence'),
    #Admin Login
    path('check_handyman_existence',views.check_handyman_existence,name='check_handyman_existence'),
    #Admin Login
    path('all_goods_drivers',views.all_goods_drivers,name='all_goods_drivers'),
    #Admin Login
    path('all_cab_drivers',views.all_cab_drivers,name='all_cab_drivers'),
    #Admin Login
    path('all_jcb_crane_drivers',views.all_jcb_crane_drivers,name='all_jcb_crane_drivers'),
    #Admin Login
    path('all_handy_man',views.all_handy_man,name='all_handy_man'),
    #Admin Login
    path('all_drivers',views.all_drivers,name='all_drivers'),
    #Other Drivers Service Edit
    path('edit_other_driver_details',views.edit_other_driver_details,name='edit_other_driver_details'),
    #Other Drivers Service Add
    path('add_other_driver_details',views.add_other_driver_details,name='add_other_driver_details'),
    #Admin Login
    path('edit_driver_details',views.edit_driver_details,name='edit_driver_details'),
    #Admin Login
    path('add_driver_details',views.add_driver_details,name='add_driver_details'),
    #Handyman Details
    path('edit_handyman_details',views.edit_handyman_details,name='edit_handyman_details'),
    #ADD New Handyman Details
    path('add_new_handyman_details',views.add_new_handyman_details,name='add_new_handyman_details'),
    #Admin Login
    path('all_faqs',views.all_faqs,name='all_faqs'),
    #Admin Login
    path('add_new_faq',views.add_new_faq,name='add_new_faq'),
    #Admin Login
    path('edit_new_faq',views.edit_new_faq,name='edit_new_faq'),
    #Admin Login
    path('all_estimations',views.all_estimations,name='all_estimations'),
    #Admin Login
    path('delete_estimation',views.delete_estimation,name='delete_estimation'),
    #Admin Login
    path('delete_enquiry',views.delete_enquiry,name='delete_enquiry'),
    #Admin Login
    path('update_other_driver_status',views.update_other_driver_status,name='update_other_driver_status'),
    #Admin Login
    path('update_handyman_status',views.update_handyman_status,name='update_handyman_status'),
    #Admin Login
    path('update_jcb_crane_driver_status',views.update_jcb_crane_driver_status,name='update_jcb_crane_driver_status'),
    #Admin Login
    path('update_cab_driver_status',views.update_cab_driver_status,name='update_cab_driver_status'),
    #Admin Login
    path('update_goods_driver_status',views.update_goods_driver_status,name='update_goods_driver_status'),
    # Admin Login
    path('get_total_goods_drivers_verified_with_count',views.get_total_goods_drivers_verified_with_count,name='get_total_goods_drivers_verified_with_count'),
    # Admin Login
    path('get_total_goods_drivers_un_verified_with_count',views.get_total_goods_drivers_un_verified_with_count,name='get_total_goods_drivers_un_verified_with_count'),
    # Admin Login
    path('get_total_goods_drivers_blocked_with_count',views.get_total_goods_drivers_blocked_with_count,name='get_total_goods_drivers_blocked_with_count'),
    # Admin Login
    path('get_total_goods_drivers_rejected_with_count',views.get_total_goods_drivers_rejected_with_count,name='get_total_goods_drivers_rejected_with_count'),
    # Admin Login
    path('get_total_goods_drivers_orders_and_earnings',views.get_total_goods_drivers_orders_and_earnings,name='get_total_goods_drivers_orders_and_earnings'),
    # Admin Login
    path('get_goods_drivers_today_earnings',views.get_goods_drivers_today_earnings,name='get_goods_drivers_today_earnings'),
    # Admin Login
    path('get_goods_drivers_current_month_earnings',views.get_goods_drivers_current_month_earnings,name='get_goods_drivers_current_month_earnings'),
    # Admin Login
    path('get_goods_all_ongoing_bookings_details',views.get_goods_all_ongoing_bookings_details,name='get_goods_all_ongoing_bookings_details'),
    # Admin Login
    path('get_goods_all_cancelled_bookings_details',views.get_goods_all_cancelled_bookings_details,name='get_goods_all_cancelled_bookings_details'),
    # Admin Login
    path('get_goods_all_completed_orders_details',views.get_goods_all_completed_orders_details,name='get_goods_all_completed_orders_details'),
    # Admin Login
    path('get_goods_booking_detail_with_id',views.get_goods_booking_detail_with_id,name='get_goods_booking_detail_with_id'),
    # Admin Login
    path('get_goods_order_detail_with_id',views.get_goods_order_detail_with_id,name='get_goods_order_detail_with_id'),
    # Admin Login
    path('get_goods_booking_detail_history_with_id',views.get_goods_booking_detail_history_with_id,name='get_goods_booking_detail_history_with_id'),
    #Goods Driver Live Location Tracking
    path('goods_driver_current_location',views.goods_driver_current_location,name='goods_driver_current_location'),
    #Goods Driver All Live Location Tracking
    path('get_all_goods_driver_online_current_location',views.get_all_goods_driver_online_current_location,name='get_all_goods_driver_online_current_location'),
    #Goods Driver All verified not verified
    path('get_total_goods_drivers_with_count',views.get_total_goods_drivers_with_count,name='get_total_goods_drivers_with_count'),
    #Get Goods Driver Details
    path('get_goods_driver_details',views.get_goods_driver_details,name='get_goods_driver_details'),
    #Check Goods Driver Status
    path('check_driver_status',views.check_driver_status,name='check_driver_status'),
    #Change Goods Driver Status from Online to Offline and vice versa
    path('toggle_driver_online_status',views.toggle_driver_online_status,name='toggle_driver_online_status'),
    #
    path('get_recharge_plans',views.get_recharge_plans,name='get_recharge_plans'),
    #
    path('add_recharge_plan',views.add_recharge_plan,name='add_recharge_plan'),
    #
    path('update_recharge_plan',views.update_recharge_plan,name='update_recharge_plan'),
    #
    path('get_all_goods_types',views.get_all_goods_types,name='get_all_goods_types'),
    #
    path('add_goods_type',views.add_goods_type,name='add_goods_type'),
    #
    path('update_goods_type',views.update_goods_type,name='update_goods_type'),
    #
    path('get_offline_drivers',views.get_offline_drivers,name='get_offline_drivers'),
    #Admin Login
    # path('login',views.login_view,name='login'),
]