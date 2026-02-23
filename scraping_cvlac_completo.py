import html
import requests  #sirve para hacer solicitudes HTTP
import csv  # sirve para escribir archivos CSV
import re # busca patrones en el texto y extraer información específica y reducir espacios extra
import time
from bs4 import BeautifulSoup
import os
import unicodedata

from conexion_sql import guardar_en_mysql

URL = ""

#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001671425" #Profe Saray
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001685519" #Jaime Blanco Lopez
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0000113761" #Addriana
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001740020" #Edna Conde
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001473049" #Ana Cristina Zuniga
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0000003029" #Javier Cordoba
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001006690" #Jhon niño
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0000674400&mostrar=produccion" #Walter arboleda
#URL = "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001413648" #Wilson Arana

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CO,es;q=0.9",
    "Referer": "https://scienti.minciencias.gov.co/",
    "Connection": "keep-alive"
}
# Nota: El sitio de CVLAC puede bloquear solicitudes si detecta tráfico sospechoso.
# Por ende se usa el headers para simular un navegador real
# y se implementan reintentos con espera entre ellos.

archivo_csv = "cv_datos_generales.csv"

# Borra el archivo si ya existe
if os.path.exists(archivo_csv):
    os.remove(archivo_csv)

def quitar_tildes(texto):
    if not texto:
        return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    return texto
def limpiar(texto):
    if not texto:
        return ""
    texto = texto.replace("\xa0", " ")
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()
# La función limpiar se encarga de eliminar espacios extra y caracteres
# no deseados del texto extraído.
def limpiar_titulo(titulo):
    """
    Quita tildes, espacios, comillas y reemplaza comas internas por punto y coma.
    """
    if not titulo:
        return ""
    titulo = quitar_tildes(titulo)        # Quita tildes
    titulo = titulo.replace('"', '')      # Quita comillas dobles
    titulo = titulo.replace("'", '')      # Quita comillas simples
    titulo = titulo.replace(",", ";")     # Reemplaza comas internas
    titulo = titulo.strip()               # Quita espacios al inicio y al final
    return titulo
def limpiar_categoria(texto):
    if not texto:
        return ""

    match = re.search(
        r"(Investigador\s+(Junior|Asociado|Senior))",
        texto,
        re.IGNORECASE
    )

    if match:
        return match.group(1).title()

    return texto.strip()

def limpiar_tipo_trabajo(texto):
    if not texto:
        return ""

    texto = limpiar(texto)

    # Nos quedamos SOLO con lo que está después del "de"
    if "-" in texto:
        texto = texto.split("de", 1)[1].strip()

    # Aseguramos formato uniforme
    texto = texto.lower()

    # Construimos el texto final
    return f"Trabajo dirigido de {texto}"

def limpiar_tipo_consultoria(texto):
    texto = limpiar(texto)

    if "-" in texto:
        texto = texto.split("-", 1)[1].strip()

    return texto


def obtener_html():
    session = requests.Session()
    session.headers.update(HEADERS)

    for intento in range(5):
        print(f"Intento {intento + 1} de conexión...")
        response = session.get(URL, timeout=30)

        if response.status_code == 200:

            # 🔥 Decodificar manualmente desde bytes
            html = response.content.decode("latin-1")

            return html

        print(f"Servidor respondió {response.status_code}, esperando...")
        time.sleep(5)

    raise Exception("No fue posible acceder a CVLAC (bloqueo del servidor)")
# La función obtener_html intenta obtener el HTML de la página con reintentos y espera

#================================================
# EXTRAER DATOS GENERALES
#================================================
def extraer_datos_generales(soup):
    datos = {
        "categoria": "No categorizado",  # Valor por defecto
        "nombre": "",
        "sexo": ""
    }

    # 1️⃣ Buscar el ancla
    anchor = soup.find("a", {"name": "datos_generales"})
    if not anchor:
        print("No se encontró el ancla datos_generales")
        return datos

    # 2️⃣ La tabla está justo después del ancla
    tabla = anchor.find_next("table")
    if not tabla:
        print("No se encontró la tabla de datos generales")
        return datos

    # 3️⃣ Recorrer filas
    for fila in tabla.find_all("tr"):
        columnas = fila.find_all("td")
        if len(columnas) == 2:
            campo = limpiar(columnas[0].get_text())
            valor = limpiar(columnas[1].get_text())

            if campo == "Categoría":
                categoria_limpia = limpiar_categoria(valor)
                if categoria_limpia:  # Si existe una categoría válida
                    datos["categoria"] = categoria_limpia
                # Si no, se queda "No categorizado"
            elif campo == "Nombre":
                datos["nombre"] = valor
            elif campo == "Sexo":
                datos["sexo"] = valor

    return datos


#================================================
# EXTRAER ÚLTIMA FORMACIÓN ACADÉMICA
#================================================
def extraer_ultima_formacion_academica(soup):
    formacion = {
        "UltimaFormacionAcademica": ""
    }

    # 1️⃣ Buscar el ancla
    anchor = soup.find("a", {"name": "formacion_acad"})
    if not anchor:
        print("No se encontró el ancla formacion_acad")
        return formacion

    # 2️⃣ Buscar la tabla de formación académica
    tabla = anchor.find_next("table")
    if not tabla:
        print("No se encontró la tabla de formación académica")
        return formacion

    # 3️⃣ Buscar el primer <b> (nivel académico)
    bold = tabla.find("b")
    if not bold:
        print("No se encontró el nivel académico")
        return formacion

    texto_nivel = bold.get_text(strip=True)

    # 4️⃣ Quedarse solo con "Maestría"
    formacion["UltimaFormacionAcademica"] = texto_nivel.split("/")[0].strip()

    return formacion

#================================================
# EXTRAER TRABAJOS DIRIGIDOS
#================================================
def extraer_trabajos_dirigidos(soup):
    

    resultados = []

    anchor = soup.find("a", {"name": "trabajos_dirigi"})
    if not anchor:
        print("No se encontró la sección trabajos dirigidos")
        return resultados

    contenedor = anchor.find_parent("td")

    tipo_trabajo_actual = ""

    # Recorremos en orden todo lo que hay dentro del contenedor
    for elemento in contenedor.find_all(["b", "blockquote"], recursive=True):

        # 1️⃣ Si es un <b>, actualizamos el tipo de trabajo
        if elemento.name == "b":
            texto_b = limpiar(elemento.get_text())

            if "trabajos dirigidos/tutorías" in texto_b.lower():
                tipo_trabajo_actual = limpiar_tipo_trabajo(texto_b)
            else:
                tipo_trabajo_actual = ""

        # 2️⃣ Si es un <blockquote>, es un trabajo
        elif elemento.name == "blockquote" and tipo_trabajo_actual:
            texto = limpiar(elemento.get_text(" "))

            # 🔹 Separar por la PRIMERA coma (autor / título)
            partes = texto.split(",", 1)

            if len(partes) < 2:
                continue  # si no hay coma, ignorar

            texto_sin_autor = partes[1].strip()

            # 🔹 Año
            año_match = re.search(r"\b(20\d{2})\b", texto_sin_autor)
            año = año_match.group(1) if año_match else ""

            # 🔹 Cortar antes de "Estado:"
            estado_match = re.search(r"^(.*?)(?=Estado:)", texto_sin_autor, re.IGNORECASE)

            if estado_match:
                titulo = estado_match.group(1).strip(" ,")
            else:
                titulo = texto_sin_autor.strip(" ,")

            resultados.append({
                "NodoHijo": tipo_trabajo_actual,
                "Titulo_proyecto": titulo,
                "año": año
            })
    print(f"✅ Total TRABAJOS DIRIGIDOS: {len(resultados)}")
    return resultados

#================================================
# EXTRAER CONSULTORÍAS
#================================================
def extraer_consultorias(soup):
    

    resultados = []

    # 1️⃣ Buscar el encabezado exacto
    h3 = soup.find("h3", id="trabajos_tec")
    if not h3:
        print("⚠️ No se encontró el h3 de Consultorías")
        return resultados

    # 2️⃣ Buscar el <b> del tipo de consultoría
    tipo_b = h3.find_next("b")
    if not tipo_b:
        print("⚠️ No se encontró el tipo de consultoría")
        return resultados

    tipo_actual = limpiar_tipo_consultoria(tipo_b.get_text())

    # 3️⃣ Recorrer TODOS los blockquote siguientes
    for block in tipo_b.find_all_next("blockquote"):
        texto = limpiar(block.get_text(" "))

        # Cortar cuando aparezca otra sección
        if block.find_previous("h3") != h3:
            break

        # 🟢 Año: buscar el año que aparece después de "En: <país>", ignorando comas/espacios extra
        anio_match = re.search(r"En:\s*[A-Za-z\s]+(?:,\s*)*,\s*(\d{4})", texto)
        anio = anio_match.group(1) if anio_match else ""

        # 🟢 Título: todo hasta "Nombre comercial"
        hasta_nombre = re.search(r"^(.*?)(?=Nombre comercial)", texto, re.IGNORECASE)
        titulo = ""
        if hasta_nombre:
            texto_hasta_nombre = hasta_nombre.group(1).strip()

            # Buscar la última coma que tenga mayúscula a la derecha
            match_coma = list(re.finditer(r",\s*(?=[A-Z])", texto_hasta_nombre))
            if match_coma:
                ultima_coma = match_coma[-1].end()  # posición final de la coma
                titulo = texto_hasta_nombre[ultima_coma:].strip(" ,")
            else:
                titulo = texto_hasta_nombre.strip(" ,")  # si no hay, tomar todo

        resultados.append({
            "NodoHijo": tipo_actual,
            "Titulo_proyecto": titulo,
            "año": anio
        })
    print(f"✅ Total CONSULTORÍAS: {len(resultados)}")
    return resultados


#================================================
# EXTRAER EVENTOS CIENTÍFICOS
#================================================
def extraer_eventos(soup):
    

    resultados = []

    anchor = soup.find("a", {"name": "evento"})
    if not anchor:
        print("⚠️ No se encontró la sección de eventos")
        return resultados

    contenedor = anchor.find_parent("td")

    # 🔹 Cada evento empieza con un <b> numérico (1,2,3...)
    for b in contenedor.find_all("b"):

        if not b.get_text(strip=True).isdigit():
            continue

        td_evento = b.find_parent("td")
        if not td_evento:
            continue

        texto = limpiar(td_evento.get_text(" "))

        # 🟢 Nombre del evento
        nombre_match = re.search(
            r"Nombre del evento:\s*(.*?)(?=Tipo de evento:|Ámbito:|Realizado el:)",
            texto,
            re.IGNORECASE
        )
        nombre_evento = nombre_match.group(1).strip() if nombre_match else ""

        # 🟢 Año
        anio_match = re.search(r"\b(19|20)\d{2}\b", texto)
        anio = anio_match.group() if anio_match else ""

        if nombre_evento:
            resultados.append({
                "NodoHijo": "Evento científico",
                "Titulo_proyecto": nombre_evento,
                "año": anio
            })
    print(f"✅ Total EVENTOS CIENTÍFICOS: {len(resultados)}")
    return resultados

#================================================
# EXTRAER FORTALECIMIENTO O SOLUCIÓN DE ASUNTOS DE INTERÉS
#================================================
def extraer_apropiacion_social(soup):
    

    resultados = []

    # 1️⃣ Buscar la sección
    h3 = soup.find("h3", string=re.compile(
        r"Fortalecimiento o solución de asuntos de interés social",
        re.IGNORECASE
    ))

    if not h3:
        print("⚠️ No se encontró la sección de Apropiación Social")
        return resultados

    # 2️⃣ Buscar todos los <b> después del h3 hasta otro h3
    for elem in h3.find_all_next():

        # 🛑 cortar si empieza otra sección
        if elem.name == "h3":
            break

        # 🎯 detectar cada nodo hijo (<b>)
        if elem.name == "b" and "Apropiación social del conocimiento" in elem.get_text():

            texto_b = limpiar(elem.get_text())

            # 🔹 NodoHijo después del guion
            nodo_hijo = ""
            if "-" in texto_b:
                nodo_hijo = texto_b.split("-", 1)[1].strip()

            # 🔹 Buscar el blockquote siguiente (producto)
            blockquote = elem.find_next("blockquote")
            if not blockquote:
                continue

            titulo = ""
            anio = ""

            children = list(blockquote.children)

            for i, child in enumerate(children):

                # Nombre del producto
                if getattr(child, "name", None) == "i" and "Nombre del producto" in child.get_text():
                    if i + 1 < len(children):
                        titulo = limpiar(children[i + 1])

                # Año
                if getattr(child, "name", None) == "i" and "Fecha de presentación" in child.get_text():
                    if i + 1 < len(children):
                        texto_fecha = limpiar(children[i + 1])
                        anio_match = re.search(r"\b(19|20)\d{2}\b", texto_fecha)
                        if anio_match:
                            anio = anio_match.group()

            if titulo:
                resultados.append({
                    "NodoHijo": nodo_hijo,
                    "Titulo_producto": titulo,
                    "año": anio
                })
    print(f"✅ Total APROPIACIÓN SOCIAL: {len(resultados)}")
    return resultados

#================================================
# EXTRAER GENERACIÓN DE INSUMOS DE POLÍTICA PÚBLICA Y NORMATIVIDAD
#================================================
def extraer_apropiacion_normatividad(soup):
    

    resultados = []

    # 1️⃣ Buscar la sección
    h3 = soup.find("h3", string=re.compile(
        r"Generación de insumos de política pública y normatividad",
        re.IGNORECASE
    ))

    if not h3:
        print("⚠️ No se encontró la sección de Generación de insumos de política pública y normatividad")
        return resultados

    # 2️⃣ Buscar todos los <b> después del h3 hasta otro h3
    for elem in h3.find_all_next():

        # 🛑 cortar si empieza otra sección
        if elem.name == "h3":
            break

        # 🎯 detectar cada nodo hijo (<b>)
        if elem.name == "b" and "Apropiación social del conocimiento" in elem.get_text():

            texto_b = limpiar(elem.get_text())

            # 🔹 NodoHijo después del guion
            nodo_hijo = ""
            if "-" in texto_b:
                nodo_hijo = texto_b.split("-", 1)[1].strip()

            # 🔹 Buscar el blockquote siguiente (producto)
            blockquote = elem.find_next("blockquote")
            if not blockquote:
                continue

            titulo = ""
            anio = ""

            children = list(blockquote.children)

            for i, child in enumerate(children):

                # Nombre del producto
                if getattr(child, "name", None) == "i" and "Nombre del producto" in child.get_text():
                    if i + 1 < len(children):
                        titulo = limpiar(children[i + 1])

                # Año
                if getattr(child, "name", None) == "i" and "Fecha de presentación" in child.get_text():
                    if i + 1 < len(children):
                        texto_fecha = limpiar(children[i + 1])
                        anio_match = re.search(r"\b(19|20)\d{2}\b", texto_fecha)
                        if anio_match:
                            anio = anio_match.group()

            if titulo:
                resultados.append({
                    "NodoHijo": nodo_hijo,
                    "Titulo_producto": titulo,
                    "año": anio
                })
    print(f"✅ Total APROPIACIÓN NORMATIVIDAD: {len(resultados)}")
    return resultados

def extraer_apropiacion_cadenas_productivas(soup):
    

    resultados = []

    # 1️⃣ Buscar la sección
    h3 = soup.find("h3", string=re.compile(
        r"Fortalecimiento de cadenas productivas",
        re.IGNORECASE
    ))

    if not h3:
        print("⚠️ No se encontró la sección de Fortalecimiento de cadenas productivas")
        return resultados

    # 2️⃣ Buscar todos los <b> después del h3 hasta otro h3
    for elem in h3.find_all_next():

        # 🛑 cortar si empieza otra sección
        if elem.name == "h3":
            break

        # 🎯 detectar cada nodo hijo (<b>)
        if elem.name == "b" and "Apropiación social del conocimiento" in elem.get_text():

            texto_b = limpiar(elem.get_text())

            # 🔹 NodoHijo después del guion
            nodo_hijo = ""
            if "-" in texto_b:
                nodo_hijo = texto_b.split("-", 1)[1].strip()

            # 🔹 Buscar el blockquote siguiente (producto)
            blockquote = elem.find_next("blockquote")
            if not blockquote:
                continue

            titulo = ""
            anio = ""

            children = list(blockquote.children)

            for i, child in enumerate(children):

                # Nombre del producto
                if getattr(child, "name", None) == "i" and "Nombre del producto" in child.get_text():
                    if i + 1 < len(children):
                        titulo = limpiar(children[i + 1])

                # Año
                if getattr(child, "name", None) == "i" and "Fecha de presentación" in child.get_text():
                    if i + 1 < len(children):
                        texto_fecha = limpiar(children[i + 1])
                        anio_match = re.search(r"\b(19|20)\d{2}\b", texto_fecha)
                        if anio_match:
                            anio = anio_match.group()

            if titulo:
                resultados.append({
                    "NodoHijo": nodo_hijo,
                    "Titulo_producto": titulo,
                    "año": anio
                })
    print(f"✅ Total FORTALECIMIENTO DE CADENAS PRODUCTIVAS: {len(resultados)}")
    return resultados

def extraer_produccion_contenido_transmedia(soup):
    

    resultados = []

    # 1️⃣ Buscar la sección
    h3 = soup.find(
        "h3",
        string=re.compile(
            r"Producción de estrategias y contenidos transmedia",
            re.IGNORECASE
        )
    )

    if not h3:
        print("⚠️ No se encontró la sección de Producción de estrategias y contenidos transmedia")
        return resultados

    # 2️⃣ Recorrer elementos hasta otro h3
    for elem in h3.find_all_next():

        if elem.name == "h3":
            break

        # 3️⃣ Detectar el <b> correcto
        if elem.name == "b" and "producción de estrategias y contenidos transmedia" in elem.get_text().lower():

            texto_b = limpiar(elem.get_text())

            # 🔹 NodoHijo = después del guion
            nodo_hijo = ""
            if "-" in texto_b:
                nodo_hijo = texto_b.split("-", 1)[1].strip()

            # 4️⃣ Buscar el blockquote siguiente
            blockquote = elem.find_next("blockquote")
            if not blockquote:
                continue

            titulo = ""
            anio = ""

            # 5️⃣ Recorrer los <i> del blockquote
            for i_tag in blockquote.find_all("i"):

                texto_i = limpiar(i_tag.get_text()).lower()

                # 🔹 Nombre del producto
                if "nombre del producto" in texto_i:
                    siguiente = i_tag.next_sibling
                    if siguiente:
                        titulo = limpiar(str(siguiente))

                # 🔹 Año
                if "fecha de presentación" in texto_i:
                    siguiente = i_tag.next_sibling
                    if siguiente:
                        año_match = re.search(r"\b(19|20)\d{2}\b", str(siguiente))
                        if año_match:
                            anio = año_match.group()

            if titulo:
                resultados.append({
                    "NodoHijo": nodo_hijo,
                    "Titulo_producto": titulo,
                    "año": anio
                })
    print(f"✅ Total PRODUCCIÓN DE ESTRATEGIAS Y CONTENIDOS TRANSMEDIA: {len(resultados)}")
    return resultados


#================================================
# EXTRAER DESARROLLOS WEB
#================================================

def extraer_desarrollos_web(soup):
    

    resultados = []

    # 1️⃣ Buscar la sección
    h3 = soup.find("h3", string=re.compile(
        r"Desarrollos web",
        re.IGNORECASE
    ))

    if not h3:
        print("⚠️ No se encontró la sección Desarrollos web")
        return resultados

    # 2️⃣ Recorrer elementos hasta otro h3
    for elem in h3.find_all_next():

        if elem.name == "h3":
            break

        # 🎯 Detectar el <b>
        if elem.name == "b" and "Divulgación pública de la ciencia" in elem.get_text():

            texto_b = limpiar(elem.get_text())

            nodo_hijo = ""

            # 🔹 Tomar texto después del guion
            if "-" in texto_b:
                parte = texto_b.split("-", 1)[1].strip()

                # 🔹 Cortar antes de los dos puntos
                if ":" in parte:
                    nodo_hijo = parte.split(":", 1)[0].strip()
                else:
                    nodo_hijo = parte.strip()

            # 🔹 Buscar blockquote
            blockquote = elem.find_next("blockquote")
            if not blockquote:
                continue

            titulo = ""
            anio = ""

            children = list(blockquote.children)

            for i, child in enumerate(children):

                # Nombre del producto
                if getattr(child, "name", None) == "i" and "Nombre del producto" in child.get_text():
                    if i + 1 < len(children):
                        titulo = limpiar(children[i + 1])

                # Año
                if getattr(child, "name", None) == "i" and "Fecha de presentación" in child.get_text():
                    if i + 1 < len(children):
                        texto_fecha = limpiar(children[i + 1])
                        anio_match = re.search(r"\b(19|20)\d{2}\b", texto_fecha)
                        if anio_match:
                            anio = anio_match.group()

            if titulo:
                resultados.append({
                    "NodoHijo": nodo_hijo,
                    "Titulo_producto": titulo,
                    "año": anio
                })
    print(f"✅ Total DESARROLLOS WEB: {len(resultados)}")
    return resultados

#================================================
# EXTRAER ARTÍCULOS
#================================================
def extraer_articulos(soup):
    

    resultados = []

    # 1️⃣ Buscar ancla de artículos
    anchor = soup.find("a", {"name": "articulos"})
    if not anchor:
        print("⚠️ No se encontró la sección de artículos")
        return resultados

    contenedor = anchor.find_parent("td")

    # 2️⃣ Iterar cada blockquote (cada artículo o grupo de artículos)
    for block in contenedor.find_all("blockquote", recursive=True):
        texto = limpiar(block.get_text(" "))

        # 🟢 Extraer todos los títulos entre comillas simples o dobles
        # Esto captura: "Título" o ""Título""
        titulos = re.findall(r'"{1,2}\s*(.*?)\s*"{1,2}', texto)

        # Extraemos el año
        parte_antes_doi = texto.split("DOI")[0]
        anios = re.findall(r"\b(?:19|20)\d{2}\b", parte_antes_doi)
        anio = anios[-1] if anios else ""

        # Agregar todos los títulos encontrados
        for titulo in titulos:
            if titulo.strip():  # Evitar títulos vacíos
                resultados.append({
                    "NodoHijo": "Artículo",
                    "Titulo_proyecto": titulo.strip(),
                    "año": anio
                })
    print(f"✅ Total ARTÍCULOS: {len(resultados)}")
    return resultados

#================================================
# EXTRAER LIBROS
#================================================
def extraer_libros(soup):
    resultados = []

    # 1️⃣ Buscar el h3 que diga exactamente "Libros"
    h3_libros = soup.find("h3", string=re.compile(r"^Libros$", re.I))
    if not h3_libros:
        return resultados

    # 2️⃣ Subir a la tabla que contiene esa sección
    tabla_libros = h3_libros.find_parent("table")
    if not tabla_libros:
        return resultados

    # 3️⃣ Buscar todos los <li> dentro de esa tabla
    items = tabla_libros.find_all("li")

    for li in items:
        b_tag = li.find("b")
        if not b_tag:
            continue

        texto_categoria = b_tag.get_text(" ", strip=True)

        # Validar estructura tipo: Producción bibliográfica - Libro - ...
        if texto_categoria.count("-") < 2:
            continue

        partes = [p.strip() for p in texto_categoria.split("-")]
        nodo_hijo = partes[1]

        # 4️⃣ Buscar el blockquote siguiente
        block = li.find_next("blockquote")

        # Asegurar que el blockquote pertenece a esta tabla
        if not block or block.find_parent("table") != tabla_libros:
            continue

        texto_block = block.get_text(" ", strip=True)

        # 5️⃣ Extraer título entre comillas
        match_titulo = re.search(r'"([^"]+)"', texto_block)
        if not match_titulo:
            continue

        titulo = match_titulo.group(1).strip()

        # 6️⃣ Extraer año
        match_anio = re.search(r'\b(19|20)\d{2}\b', texto_block)
        anio = match_anio.group(0) if match_anio else None

        resultados.append({
            "NodoHijo": nodo_hijo,
            "Titulo_proyecto": titulo,
            "año": anio
        })
    print(f"✅ Total LIBROS: {len(resultados)}")
    return resultados


#================================================
# EXTRAER CAPÍTULOS DE LIBRO
#================================================
def extraer_capitulos_libro(soup):
    

    resultados = []

    # 1️⃣ Buscar el ancla
    anchor = soup.find("a", {"name": "capitulos"})
    if not anchor:
        print("⚠️ No se encontró la sección de capítulos de libro")
        return resultados

    contenedor = anchor.find_parent("td")

    nodo_hijo = "Capítulos de libro"

    # 2️⃣ Cada capítulo está en un <blockquote>
    for block in contenedor.find_all("blockquote", recursive=True):

        texto = limpiar(block.get_text(" "))

        # 🟢 Título (entre comillas)
        titulo_match = re.search(
            r"\"(.*?)\"",
            texto
        )
        titulo = titulo_match.group(1).strip() if titulo_match else ""

        # 🟢 Año
        anio_match = re.search(r"\b(19|20)\d{2}\b", texto)
        anio = anio_match.group() if anio_match else ""

        if titulo:
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_proyecto": titulo,
                "año": anio
            })
    print(f"✅ Total CAPÍTULOS DE LIBRO: {len(resultados)}")
    return resultados

#================================================
# EXTRAER INNOVACIONES DE GESTIÓN EMPRESARIAL
#================================================
def extraer_innovaciones_gestion_empresarial(soup):
    

    resultados = []

    # 1️⃣ Buscar todos los <b> que indiquen la sección
    for b in soup.find_all("b"):
        if "Producción técnica - Innovaciones generadas de producción empresarial" in b.get_text():
            # NodoHijo: lo que está después del primer guion
            nodo_hijo = b.get_text().split("-", 1)[1].strip()

            # 2️⃣ Buscar el blockquote siguiente
            block = b.find_parent("td").find_next("blockquote")
            if not block:
                continue

            texto = limpiar(block.get_text(" "))

            # 3️⃣ Título: todo hasta "Nombre comercial"
            hasta_nombre = re.search(r"^(.*?)(?=Nombre comercial)", texto, re.IGNORECASE)
            titulo = ""
            if hasta_nombre:
                texto_hasta_nombre = hasta_nombre.group(1).strip()

                # Buscar la última coma que tenga mayúscula a la derecha
                match_coma = list(re.finditer(r",\s*(?=[A-Z])", texto_hasta_nombre))
                if match_coma:
                    ultima_coma = match_coma[-1].end()
                    titulo = texto_hasta_nombre[ultima_coma:].strip(" ,")
                else:
                    titulo = texto_hasta_nombre.strip(" ,")

            # 4️⃣ Año: buscar "En: <país>" y luego el año, ignorando comas y espacios extra
            anio_match = re.search(r"En:\s*[A-Za-z\s]+(?:,\s*)*,\s*(\d{4})", texto)
            anio = anio_match.group(1) if anio_match else ""

            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_proyecto": titulo,
                "año": anio
            })
    print(f"✅ Total INNOVACIONES DE GESTIÓN EMPRESARIAL: {len(resultados)}")
    return resultados
#================================================
# EXTRAER DOCUMENTOS DE TRABAJO
#================================================
def extraer_documentos_trabajo(soup):
    

    resultados = []

    # 1️⃣ Buscar sección "Documentos de trabajo"
    h3 = soup.find("h3", string=re.compile(r"Documentos de trabajo", re.IGNORECASE))
    if not h3:
        print("⚠️ No se encontró la sección Documentos de trabajo")
        return resultados

    # 2️⃣ Recorrer todos los blockquote dentro de la sección hasta otro h3
    for blockquote in h3.find_all_next("blockquote"):
        if blockquote.find_previous("h3") != h3:
            break  # salió de la sección

        texto_block = limpiar(blockquote.get_text(" "))

        # 3️⃣ NodoHijo: buscar el <b> más cercano antes del blockquote
        b_antes = blockquote.find_previous("b")
        nodo_hijo = ""
        if b_antes:
            texto_b = limpiar(b_antes.get_text())
            if "-" in texto_b:
                nodo_hijo = texto_b.split("-", 1)[1].strip()
                nodo_hijo = re.sub(r"\(.*?\)", "", nodo_hijo).strip()

        # 4️⃣ Título entre comillas
        titulo_match = re.search(r'"([^"]+)"', texto_block)
        if not titulo_match:
            continue  # ignorar blockquote sin título
        titulo = titulo_match.group(1).strip()

        # 5️⃣ Año: primero En: <país>, si no fallback a cualquier año
        anio_match = re.search(r"En:\s*[A-Za-z\s]+(?:,\s*)*,\s*(\d{4})", texto_block)
        if anio_match:
            anio = anio_match.group(1)
        else:
            anio_match = re.search(r"\b(19|20)\d{2}\b", texto_block)
            anio = anio_match.group() if anio_match else ""

        # 6️⃣ Evitar duplicados: comparar NodoHijo + título + año
        if not any(r["NodoHijo"] == nodo_hijo and r["Titulo_documento"] == titulo and r["año"] == anio for r in resultados):
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_documento": titulo,
                "año": anio
            })
    
    print(f"✅ Total DOCUMENTOS DE TRABAJO: {len(resultados)}")
    return resultados

#================================================
# EXTRAER PATENTES
#================================================
def extraer_patentes(soup):
    

    resultados = []

    anchor = soup.find("a", {"name": "patentes"})
    if not anchor:
        print("No se encontró la sección Patentes")
        return resultados

    contenedor = anchor.find_parent("td")

    nodo_hijo = "Patente"

    for blockquote in contenedor.find_all("blockquote"):

        texto = limpiar(blockquote.get_text(" "))

        # 🔹 TÍTULO: después del "-" hasta la primera coma
        titulo = ""
        titulo_match = re.search(r"-\s*([^,]+)", texto)
        if titulo_match:
            titulo = titulo_match.group(1).strip()

        # 🔹 AÑO: desde fecha YYYY-MM-DD
        anio = ""
        anio_match = re.search(r"\b(19|20)\d{2}(?=-\d{2}-\d{2})", texto)
        if anio_match:
            anio = anio_match.group(0)

        if titulo:
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_patente": titulo,
                "año": anio
            })

    print(f"✅ Total PATENTES: {len(resultados)}")
    return resultados

#================================================
# EXTRAER SECRETOS EMPRESARIALES
#================================================
def extraer_secretos_empresariales(soup ):
    

    resultados = []

    anchor = soup.find("a", {"name": "secretos"})
    if not anchor:
        print("No se encontró la sección Secretos empresariales")
        return resultados

    contenedor = anchor.find_parent("td")

    nodo_hijo = "Secreto empresarial"

    # Recorremos SOLO los <b> dentro del contenedor
    for b in contenedor.find_all("b"):

        titulo = limpiar(b.get_text())

        if titulo:
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_secreto": titulo,
                "año": ""
            })
    print(f"✅ Total SECRETOS EMPRESARIALES: {len(resultados)}")
    return resultados

#================================================
# EXTRAER SOFTWARE
#================================================
def extraer_software(soup):
    

    resultados = []

    anchor = soup.find("a", {"name": "software"})
    if not anchor:
        print("⚠️ No se encontró la sección de software")
        return resultados

    contenedor = anchor.find_parent("td")
    nodo_hijo = "Software"

    for block in contenedor.find_all("blockquote", recursive=True):

        texto = limpiar(block.get_text(" "))

        # ✅ EXTRAER TÍTULO: lo que está antes de ", Nombre comercial"
        titulo_match = re.search(
            r"([^,]+)(?=,\s*Nombre comercial)",
            texto,
            re.IGNORECASE
        )
        titulo = titulo_match.group(1).strip() if titulo_match else ""

        # ✅ EXTRAER AÑO
        anio_match = re.search(r"\b(19|20)\d{2}\b", texto)
        anio = anio_match.group() if anio_match else ""

        if titulo:
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_proyecto": titulo,
                "año": anio
            })
    print(f"✅ Total SOFTWARE: {len(resultados)}")
    return resultados


#================================================
# EXTRAER PROTOTIPOS INDUSTRIALES
#================================================
def extraer_prototipos_industriales(soup):
    
    resultados = []

    # 1️⃣ Buscar la sección Prototipos
    h3 = soup.find("h3", string=re.compile(r"Prototipos", re.IGNORECASE))
    if not h3:
        print("⚠️ No se encontró la sección Prototipos")
        return resultados

    # 2️⃣ Contenedor general
    contenedor = h3.find_parent("table")

    nodo_hijo = "Prototipo industrial"

    # 3️⃣ Recorremos todos los <b> de prototipo industrial
    for b in contenedor.find_all("b"):

        texto_b = limpiar(b.get_text())

        if "Prototipo - Industrial" not in texto_b:
            continue

        # 4️⃣ El blockquote SIEMPRE está en el siguiente <tr>
        tr = b.find_parent("tr")
        siguiente_tr = tr.find_next_sibling("tr")
        if not siguiente_tr:
            continue

        blockquote = siguiente_tr.find("blockquote")
        if not blockquote:
            continue

        texto = blockquote.get_text(" ", strip=True)

        # 5️⃣ TÍTULO → antes de "Nombre comercial:"
        parte_util = texto.split("Nombre comercial:")[0]
        fragmentos = [f.strip() for f in parte_util.split(",") if f.strip()]
        titulo = fragmentos[-1] if fragmentos else ""

        # 6️⃣ AÑO
        anio_match = re.search(r",\s*(19|20)\d{2}\s*,", texto)
        anio = anio_match.group(0).replace(",", "").strip() if anio_match else ""

        if titulo:
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_prototipo": limpiar(titulo),
                "año": anio
            })
    print(f"✅ Total PROTOTIPOS INDUSTRIALES: {len(resultados)}")
    return resultados

#================================================
# EXTRAER INNOVACIÓN DE PROCESO O PROCEDIMIENTO
#================================================

def extraer_innovacion_procesos(soup):
    

    resultados = []

    # 1️⃣ Buscar el h3 exacto
    h3 = soup.find("h3", string=re.compile(
        r"Innovación de proceso o procedimiento", re.IGNORECASE
    ))

    if not h3:
        print("⚠️ No se encontró el h3 de Innovación de proceso o procedimiento")
        return resultados

    # 2️⃣ Recorrer hasta el siguiente h3
    for elem in h3.find_all_next():

        if elem.name == "h3":
            break

        if elem.name != "blockquote":
            continue

        texto = limpiar(elem.get_text(" "))

        # ✅ TÍTULO: texto antes de ", Nombre comercial"
        titulo_match = re.search(
            r"([^,]+)(?=,\s*Nombre comercial)",
            texto,
            re.IGNORECASE
        )
        titulo = titulo_match.group(1).strip() if titulo_match else ""

        # ✅ AÑO
        anio_match = re.search(r"\b(19|20)\d{2}\b", texto)
        anio = anio_match.group() if anio_match else ""

        if titulo:
            resultados.append({
                "NodoHijo": "Innovación de proceso o procedimiento",
                "Titulo_proyecto": titulo,
                "año": anio
            })
    print(f"✅ Total INNOVACIÓN DE PROCESOS O PROCEDIMIENTOS: {len(resultados)}")
    return resultados

#================================================
# EXTRAER INFORMES TÉCNICOS
#================================================

def extraer_informes_tecnicos(soup):

    resultados = []
    nodo_hijo = "Informe técnico"

    # 1️⃣ Buscar el comentario que marca el fin del bloque anterior
    comentario = soup.find(
        string=lambda text: isinstance(text, str) and "Fin Nuevo registro cientifico" in text
    )

    if not comentario:
        print("⚠️ No se encontró el comentario de referencia")
        return resultados

    # 2️⃣ Desde ahí buscar el h3 correcto
    seccion = comentario.find_next("h3", id="trabajos_tec")

    if not seccion:
        print("⚠️ No se encontró la sección trabajos_tec")
        return resultados

    # 3️⃣ Subir a la tabla contenedora correcta
    tabla = seccion.find_parent("table")

    if not tabla:
        print("⚠️ No se encontró tabla contenedora")
        return resultados

    # 4️⃣ Buscar SOLO los blockquote dentro de esa tabla
    bloques = tabla.find_all("blockquote")

    for block in bloques:

        # Obtener texto con saltos de línea reales
        texto = block.get_text("\n", strip=True)

        # 🔹 Extraer año (último año de 4 dígitos)
        anios = re.findall(r"\b(?:19|20)\d{2}\b", texto)
        anio = anios[-1] if anios else ""

        # 🔹 Tomar solo la parte antes de "Nombre comercial"
        parte_principal = texto.split("Nombre comercial")[0]

        # 🔹 Separar líneas limpias
        lineas = [l.strip(" ,") for l in parte_principal.split("\n") if l.strip()]

        # 🔹 El título suele ser la última línea antes de "Nombre comercial"
        titulo = lineas[-1] if lineas else ""
        titulo = quitar_tildes(titulo)

        resultados.append({
            "NodoHijo": nodo_hijo,
            "Titulo_proyecto": titulo,
            "año": anio
        })

    print(f"✅ Total informes técnicos encontrados: {len(resultados)}")

    return resultados


#================================================
# EXTRAER CONCEPTOS TÉCNICOS
#================================================
def extraer_conceptos_tecnicos(soup):
    

    resultados = []

    # 1️⃣ Buscar todos los <b> que indiquen "Producción técnica - Concepto técnico"
    for b in soup.find_all("b"):
        if "Producción técnica - Concepto técnico" in b.get_text():
            # NodoHijo: lo que está después del guion
            nodo_hijo = b.get_text().split("-", 1)[1].strip()

            # 2️⃣ Buscar el blockquote siguiente
            block = b.find_parent("td").find_next("blockquote")
            if not block:
                continue

            texto = limpiar(block.get_text(" "))

            # 3️⃣ Título: todo hasta "Institución solicitante"
            hasta_institucion = re.search(r"^(.*?)(?=Institución solicitante)", texto, re.IGNORECASE)
            titulo = ""
            if hasta_institucion:
                texto_hasta_inst = hasta_institucion.group(1).strip()

                # Buscar la última coma que tenga mayúscula a la derecha (para eliminar autores)
                match_coma = list(re.finditer(r",\s*(?=[A-Z])", texto_hasta_inst))
                if match_coma:
                    ultima_coma = match_coma[-1].end()
                    titulo = texto_hasta_inst[ultima_coma:].strip(" ,")
                else:
                    titulo = texto_hasta_inst.strip(" ,")

            # 4️⃣ Año: buscar "Fecha solicitud" o "Fecha de envío" y tomar 4 dígitos
            anio_match = re.search(r"Fecha solicitud:.*?(\d{4})", texto)
            if not anio_match:
                anio_match = re.search(r"Fecha de envío:.*?(\d{4})", texto)
            anio = anio_match.group(1) if anio_match else ""

            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_proyecto": titulo,
                "año": anio
            })
    print(f"✅ Total CONCEPTOS TÉCNICOS: {len(resultados)}")
    return resultados




#================================================
# EXTRAER INFORMES FINALES DE INVESTIGACIÓN
#================================================
def extraer_informes_finales_investigacion(soup):

    resultados = []
    nodo_hijo = "Informes finales de investigación"

    # 🔹 Buscar el h3 correctamente (tolerante)
    seccion = soup.find(
        "h3",
        string=lambda t: t and "Informes de investig" in t
    )

    if not seccion:
        return resultados

    tabla = seccion.find_parent("table")
    if not tabla:
        return resultados

    # 🔹 Iterar cada bloque
    for block in tabla.find_all("blockquote", recursive=True):

        texto = block.get_text(" ", strip=True)

        # ========================
        # 1️⃣ Extraer año
        # ========================
        anio_match = re.search(r"\b(19|20)\d{2}\b", texto)
        anio = anio_match.group(0) if anio_match else ""

        # ========================
        # 2️⃣ Extraer título
        # ========================

        # Cortar antes de ". En:"
        titulo_bruto = re.split(r"\.?\s*En:", texto, flags=re.IGNORECASE)[0]

        # Eliminar el autor (todo hasta la primera coma)
        if "," in titulo_bruto:
            titulo_bruto = titulo_bruto.split(",", 1)[1]

        titulo = titulo_bruto.strip(" ,.")

        # Limpieza extra
        titulo = re.sub(r"\s{2,}", " ", titulo)

        if titulo:
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_proyecto": titulo,
                "año": anio
            })
    print(f"✅ Total INFORMES FINALES DE INVESTIGACIÓN: {len(resultados)}")
    return resultados

#================================================
# EXTRAER PROYECTOS
#================================================
def extraer_proyectos(soup):

    resultados = []

    # 1️⃣ Buscar la sección Proyectos
    h3 = soup.find("h3", string=re.compile(r"Proyectos", re.IGNORECASE))
    if not h3:
        print("⚠️ No se encontró la sección Proyectos")
        return resultados

    # 2️⃣ Recorrer hasta otro h3
    for elem in h3.find_all_next():

        if elem.name == "h3":
            break

        if elem.name != "blockquote":
            continue

        nodo_hijo = ""
        titulo = ""
        anio = ""

        children = list(elem.children)

        for i, child in enumerate(children):

            # 🔹 Detectar <i> Tipo de proyecto
            if getattr(child, "name", None) == "i" and "Tipo de proyecto" in child.get_text():

                # 👉 El valor REAL está en el siguiente nodo
                if i + 1 < len(children):
                    nodo_hijo = limpiar(children[i + 1])
                    nodo_hijo = nodo_hijo.replace(",", "")

            # 🔹 Texto plano
            if isinstance(child, str):
                texto = limpiar(child)

                if not texto:
                    continue

                # ✅ TÍTULO (primer texto largo que NO sea el tipo)
                if not titulo and texto != nodo_hijo and len(texto) > 5:
                    titulo = texto
                    titulo = limpiar_titulo(titulo)

                # ✅ AÑO
                anio_match = re.search(r"\b(19|20)\d{2}\b", texto)
                if anio_match:
                    anio = anio_match.group()

        if titulo:
            resultados.append({
                "NodoHijo": nodo_hijo,
                "Titulo_proyecto": titulo,
                "año": anio
            })
    print(f"✅ Total PROYECTOS: {len(resultados)}")
    return resultados



def guardar_csv(filas):
    archivo = "cv_datos_generales.csv"
    existe = os.path.exists(archivo)

    with open(archivo, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "categoria",
                "nombre",
                "sexo",
                "UltimaFormacionAcademica",
                "NodoHijo",
                "Titulo_proyecto",
                "año"
            ]
        )

        # 🔹 Escribir encabezado SOLO si el archivo no existe
        if not existe:
            writer.writeheader()

        writer.writerows(filas)

def main():
    print("Iniciando scraping CVLAC...")

    html = obtener_html()
    with open("debug.html", "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "lxml")

    # -----------------------------
    # Inicializar variables
    # -----------------------------
    filas_csv = []  # Para guardar todo antes de exportar CSV
    filas_mysql = []  # Para la base de datos

    # -----------------------------
    # Extraer secciones
    # -----------------------------
    datos_generales = extraer_datos_generales(soup)
    extra_formacion = extraer_ultima_formacion_academica(soup)
    trabajos = extraer_trabajos_dirigidos(soup)
    consultorias = extraer_consultorias(soup)
    eventos = extraer_eventos(soup)
    apropiacion_social = extraer_apropiacion_social(soup)
    apropiacion_normatividad = extraer_apropiacion_normatividad(soup)
    cadenas_productivas = extraer_apropiacion_cadenas_productivas(soup)
    contenido_transmedia = extraer_produccion_contenido_transmedia(soup)
    desarrollos_web = extraer_desarrollos_web(soup)
    articulos = extraer_articulos(soup)
    libros = extraer_libros(soup)
    capitulos_libro = extraer_capitulos_libro(soup)
    innovaciones_gestion_empresarial = extraer_innovaciones_gestion_empresarial(soup)
    documentos_trabajo = extraer_documentos_trabajo(soup)
    patentes = extraer_patentes(soup)
    secretos_empresariales = extraer_secretos_empresariales(soup)
    software = extraer_software(soup)
    prototipos_industriales = extraer_prototipos_industriales(soup)
    innovacion_procesos = extraer_innovacion_procesos(soup)
    informes_tecnicos = extraer_informes_tecnicos(soup)
    conceptos_tecnicos = extraer_conceptos_tecnicos(soup)
    informes_finales_investigacion = extraer_informes_finales_investigacion(soup)
    proyectos = extraer_proyectos(soup)

    # -----------------------------
    # Construir filas_csv
    # -----------------------------
    secciones = [
        (trabajos, "Titulo_proyecto"),
        (consultorias, "Titulo_proyecto"),
        (eventos, "Titulo_proyecto"),
        (apropiacion_social, "Titulo_producto"),
        (apropiacion_normatividad, "Titulo_producto"),
        (cadenas_productivas, "Titulo_producto"),
        (contenido_transmedia, "Titulo_producto"),
        (desarrollos_web, "Titulo_producto"),
        (articulos, "Titulo_proyecto"),
        (libros, "Titulo_proyecto"),
        (capitulos_libro, "Titulo_proyecto"),
        (innovaciones_gestion_empresarial, "Titulo_proyecto"),
        (documentos_trabajo, "Titulo_documento"),
        (patentes, "Titulo_patente"),
        (secretos_empresariales, "Titulo_secreto"),
        (software, "Titulo_proyecto"),
        (prototipos_industriales, "Titulo_prototipo"),
        (innovacion_procesos, "Titulo_proyecto"),
        (informes_tecnicos, "Titulo_proyecto"),
        (conceptos_tecnicos, "Titulo_proyecto"),
        (informes_finales_investigacion, "Titulo_proyecto"),
        (proyectos, "Titulo_proyecto"),
    ]

    for seccion, campo_titulo in secciones:
        for item in seccion:
            filas_csv.append({
                "categoria": datos_generales.get("categoria", ""),
                "nombre": datos_generales.get("nombre", ""),
                "sexo": datos_generales.get("sexo", ""),
                "UltimaFormacionAcademica": extra_formacion.get("UltimaFormacionAcademica", ""),
                "NodoHijo": item.get("NodoHijo", ""),
                "Titulo_proyecto": item.get(campo_titulo, ""),
                "año": item.get("año", "")
            })

    # -----------------------------
    # Guardar CSV
    # -----------------------------
    guardar_csv(filas_csv)
    print(f"✓ {len(filas_csv)} registros guardados en cvlac_completo.csv")

    # -----------------------------
    # Preparar filas para MySQL y guardar
    # -----------------------------
    for fila in filas_csv:
        filas_mysql.append({
            "categoria": fila["categoria"],
            "nombre": fila["nombre"],
            "sexo": fila["sexo"],
            "grado": fila["UltimaFormacionAcademica"],
            "tipo_proyecto": fila["NodoHijo"],
            "titulo_proyecto": fila["Titulo_proyecto"],
            "anio": fila["año"]
        })
    guardar_en_mysql(filas_mysql)
    print(f"✓ {len(filas_mysql)} registros guardados en MySQL")

if __name__ == "__main__":
    URLS = [
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001671425",
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001685519",
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0000113761",
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001473049",
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001740020",
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0000003029", #Javier Cordoba
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001006690", #Jhon niño
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0000674400&mostrar=produccion", #Walter arboleda
        "https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001413648"
    ]

    for url in URLS:
        print(f"\n📄 Procesando CVLAC: {url}")
        URL = url     # 🔥 ESTA LÍNEA ES LA CLAVE
        main()