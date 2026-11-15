import os
import sqlite3
from config import Config

def analyze_results():
    print("=== АНАЛІЗ ЗБІРУ TELEGRAM SNOWBALL ===")
    
    db_path = Config.DATABASE_PATH
    if not os.path.exists(db_path):
        print(f"Базу даних не знайдено за шляхом: {db_path}")
        print("Можливо, дані зберігаються у файлах результатів або інфраструктура використовує інше сховище.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Приклад підрахунку загальної кількості унікальних каналів у базі
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(T"Знайдено таблиць у базі: {len(tables)}")
        
        # Виведемо загальну статистику, якщо таблиця каналів існує
        # (Назва таблиці може залежати від вашої реалізації SQLite-реєстру)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for table_name in cursor.fetchall():
            t_name = table_name[0]
            cursor.execute(f"SELECT COUNT(*) FROM {t_name}")
            count = cursor.fetchone()[0]
            print(f"Таблиця '{t_name}': {count} записів")
            
    except Exception as e:
        print(f"Подібність при аналізі бази даних: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_results()