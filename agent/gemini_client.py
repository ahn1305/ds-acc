from google import genai
from django.conf import settings

client = genai.Client(api_key=settings.GEMINI_API_KEY)

def call_gemini(prompt):
    response = client.models.generate_content(
        model="gemini-3.1-flash-lite-preview",   # fast + cheap
        contents=prompt
    )

    return response.text
