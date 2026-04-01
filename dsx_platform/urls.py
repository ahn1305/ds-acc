"""
URL configuration for dsx_platform project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path
from ingestion.views import UploadView
from ui.views import upload_page
from ui.views import home_page, informatica_upload_page
from ingestion.views import BatchDetailView, InformaticaBatchDetailView, InformaticaUploadView


urlpatterns = [
    path("admin/", admin.site.urls),
    
    path('', home_page),
    path('dsx/',upload_page),            # UI
    path('upload/', UploadView.as_view()),  # API
    path("batch/<int:batch_id>/", BatchDetailView.as_view()),

    # =========================
    # INFORMATICA ROUTES
    # =========================
    path("informatica/upload/", InformaticaUploadView.as_view(), name="informatica-upload"),
    path("informatica/batch/<int:batch_id>/", InformaticaBatchDetailView.as_view(), name="informatica-batch-detail"),
    path('informatica/', informatica_upload_page),


]
