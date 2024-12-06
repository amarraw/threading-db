import sqlite3
import threading

# Database setup functions
def create_users_table():
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    ''')
    conn.commit()
    conn.close()

def create_products_table():
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL
        )
    ''')
    conn.commit()
    conn.close()

def create_orders_table():
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            quantity INTEGER
        )
    ''')
    conn.commit()
    conn.close()

# Insert functions
def insert_users():
    users_data = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com"),
        (4, "David", "david@example.com"),
        (5, "Eve", "eve@example.com"),
        (6, "Frank", "frank@example.com"),
        (7, "Grace", "grace@example.com"),
        (8, "Alice", "alice@example.com"),
        (9, "Henry", "henry@example.com"),
        (10, "", "jane@example.com")
    ]
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO Users (id, name, email) VALUES (?, ?, ?)', users_data)
    conn.commit()
    conn.close()

def insert_products():
    products_data = [
        (1, "Laptop", 1000.00),
        (2, "Smartphone", 700.00),
        (3, "Headphones", 150.00),
        (4, "Monitor", 300.00),
        (5, "Keyboard", 50.00),
        (6, "Mouse", 30.00),
        (7, "Laptop", 1000.00),
        (8, "Smartwatch", 250.00),
        (9, "Gaming Chair", 500.00),
        (10, "Earbuds", -50.00)
    ]
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO Products (id, name, price) VALUES (?, ?, ?)', products_data)
    conn.commit()
    conn.close()

def insert_orders():
    orders_data = [
        (1, 1, 1, 2),
        (2, 2, 2, 1),
        (3, 3, 3, 5),
        (4, 4, 4, 1),
        (5, 5, 5, 3),
        (6, 6, 6, 4),
        (7, 7, 7, 2),
        (8, 8, 8, 0),
        (9, 9, 1, -1),
        (10, 10, 11, 2)
    ]
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO Orders (id, user_id, product_id, quantity) VALUES (?, ?, ?, ?)', orders_data)
    conn.commit()
    conn.close()

# Threading setup
def threaded_inserts():
    threads = []
    functions = [insert_users, insert_products, insert_orders]

    for func in functions:
        thread = threading.Thread(target=func)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

# Display results
def display_users():
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Users')
    rows = cursor.fetchall()
    print("\nUsers Table:")
    for row in rows:
        print(row)
    conn.close()

def display_products():
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Products')
    rows = cursor.fetchall()
    print("\nProducts Table:")
    for row in rows:
        print(row)
    conn.close()

def display_orders():
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Orders')
    rows = cursor.fetchall()
    print("\nOrders Table:")
    for row in rows:
        print(row)
    conn.close()

def display_all():
    display_users()
    display_products()
    display_orders()

# Main
if __name__ == "__main__":
    create_users_table()
    create_products_table()
    create_orders_table()
    threaded_inserts()
    print("\nInsertions completed successfully!")
    

    display_all()