import mysql.connector


def guardar_en_mysql(datos):
    conexion = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Amaamama12345.",
        database="scraping"
    )
    cursor = conexion.cursor()

    sql = """
    INSERT INTO resultados
    (categoria, nombre, sexo, grado, tipo_proyecto, titulo_proyecto, anio)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for fila in datos:
        valores = (
            fila["categoria"],
            fila["nombre"],
            fila["sexo"],
            fila["grado"],
            fila["tipo_proyecto"],
            fila["titulo_proyecto"],
            fila["anio"]
        )
        cursor.execute(sql, valores)

    conexion.commit()
    cursor.close()
    conexion.close()
    print("✅ Datos guardados en MySQL correctamente")