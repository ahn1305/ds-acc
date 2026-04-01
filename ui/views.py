from django.shortcuts import render

def upload_page(request):
    return render(request, "upload.html")

def home_page(request):
    return render(request, "home.html")



def informatica_upload_page(request):
    return render(request, "upload2.html")