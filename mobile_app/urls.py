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
    
    
    
    
    
    
    
    #Goods Driver Api's URLs
    #Login
    path('goods_driver_login',views.goods_driver_login_view,name='goods_driver_login'),
    #Registration 
    path('goods_driver_registration',views.goods_driver_registration,name='goods_driver_registration'),
    #Status Verification 
    path('goods_driver_online_status',views.goods_driver_online_status,name='goods_driver_online_status'),
]