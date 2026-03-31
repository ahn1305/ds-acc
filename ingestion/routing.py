# ingestion/routing.py

from django.urls import path
from .consumer import BatchConsumer

websocket_urlpatterns = [
    path("ws/batch/<int:batch_id>/", BatchConsumer.as_asgi()),
]

