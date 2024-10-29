from django.urls import path
from . import views

app_name = 'dashboard_app'

urlpatterns = [
    #Admin Login
    path('login',views.login_view,name='login'),
]