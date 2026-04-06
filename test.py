
import requests

BOT_TOKEN = "8734644918:AAHgTR1eD4PRcpwnWahYrDjDrygaVobcpDU"
CHAT_ID = "6195310717"

url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

try:
    response = requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": "🔥 Lolo Music conectado correctamente"
        },
        timeout=30
    )
    print("Status code:", response.status_code)
    print("Response text:", response.text)
except Exception as e:
    print("Error:", e)