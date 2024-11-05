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
]