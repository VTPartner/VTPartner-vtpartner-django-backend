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
    path('edit_driver_details',views.edit_driver_details,name='edit_driver_details'),
    #Admin Login
    path('add_driver_details',views.add_driver_details,name='add_driver_details'),
    #Admin Login
    path('edit_handyman_details',views.edit_handyman_details,name='edit_handyman_details'),
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
    # path('login',views.login_view,name='login'),
]