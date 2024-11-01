from django.urls import path
from . import views

app_name = 'website_app'

urlpatterns = [
    #All Services
    path('all_services',views.all_services,name='all_services'),
    #All Allowed Cities
    path('all_allowed_cities',views.all_allowed_cities,name='all_allowed_cities'),
    #All Vehicles
    path('all_vehicles',views.all_vehicles,name='all_vehicles'),
    #All Services
    path('all_vehicles_with_price',views.all_vehicles_with_price,name='all_vehicles_with_price'),
    #All SUB Categories
    path('all_sub_categories',views.all_sub_categories,name='all_sub_categories'),
    #All Other Services
    path('all_other_services',views.all_other_services,name='all_other_services'),
    #All Gallery Images Delivery
    path('all_delivery_gallery_images',views.all_delivery_gallery_images,name='all_delivery_gallery_images'),
    #All Service Gallery Images
    path('all_services_gallery_images',views.all_services_gallery_images,name='all_services_gallery_images'),
    #Add New Enquiry
    path('add_new_enquiry',views.add_new_enquiry,name='add_new_enquiry'),
    #Add New Drivers Enquiry
    path('add_new_drivers_enquiry',views.add_new_drivers_enquiry,name='add_new_drivers_enquiry'),
    #Distance Calculation
    path('distance',views.distance,name='distance'),
    #Fare Results
    path('fare_result',views.fare_result,name='fare_result'),
    #Estimation request for Goods , driver 
    path('add_new_estimation_request',views.add_new_estimation_request,name='add_new_estimation_request'),
    #Check Pincode is allowed or not 
    path('check_allowed_pincode',views.check_allowed_pincode,name='check_allowed_pincode'),
    #All Services
    # path('',views.all_services,name='all_services'),
    
]