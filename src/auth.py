import asyncio
import os
import qrcode
from dotenv import load_dotenv
from telethon import TelegramClient
from config import Config

load_dotenv()

SESSION = os.getenv(
    "TELEGRAM_SESSION_NAME",
    os.path.join(str(Config.VAR_ROOT_PATH), "sessions", "session_name"),
)
os.makedirs(os.path.dirname(SESSION), exist_ok=True)

async def main():
    client = TelegramClient(
        SESSION,
        int(os.getenv("TELEGRAM_API_ID")),
        os.getenv("TELEGRAM_API_HASH"),
    )
    await client.connect()
    if not await client.is_user_authorized():
        qr = await client.qr_login()
        done = False
        while not done:
            code = qrcode.QRCode()
            code.add_data(qr.url)
            code.print_ascii(invert=True)
            print("Скануй: Telegram -> Налаштування -> Пристрої -> Підключити пристрій")
            try:
                await qr.wait(20)
                done = True
            except asyncio.TimeoutError:
                await qr.recreate()
    me = await client.get_me()
    print("OK, увійшов як:", me.first_name)
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())