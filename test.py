import mysql.connector

# Connect string cho MySQL
connect_string = "mysql+mysqlconnector://iart:Hoang22041999@3.18.29.6:3306/iart"

# Phân tích connect string
import re
pattern = r"mysql\+mysqlconnector:\/\/(.*?):(.*?)@(.*?):(.*?)\/(.*)"
match = re.match(pattern, connect_string)
if match:
    user = match.group(1)
    password = match.group(2)
    host = match.group(3)
    port = match.group(4)
    database = match.group(5)

    # Kết nối tới MySQL database
    conn = mysql.connector.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )

    if conn.is_connected():
        print("Kết nối thành công tới MySQL database")
    
    # Thực hiện một số hoạt động với database
    cursor = conn.cursor()
    cursor.execute("SELECT DATABASE();")
    db_name = cursor.fetchone()
    print(f"Đang sử dụng database: {db_name}")

    # Đóng kết nối
    cursor.close()
    conn.close()
else:
    print("Connect string không hợp lệ")
