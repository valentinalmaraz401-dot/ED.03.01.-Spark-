# ED.03.01 - Apache Spark Standalone y Procesamiento Distribuido con RDDs

Repositorio de prácticas del módulo **Apache Spark - Fundamentos y Entornos**, ejecutado de forma nativa en Ubuntu Linux con **PySpark 3.5.3** y **Java 11**.

---

## Guía Explicativa: ¿Qué se hizo en este proyecto y cómo funciona? (Para principiantes)

### ¿Qué es Apache Spark y para qué sirve?
Imagina que tienes una libreta con **10 millones de líneas de ventas** y necesitas calcular cuánto se vendió en total. Si lo hace una sola persona con una calculadora (como ocurre en un programa tradicional de Python), tardará horas y probablemente se sature.

**Apache Spark** funciona como un **director de orquesta (Driver)** que reparte las hojas de esa libreta entre varios **ayudantes (Executors / Núcleos del procesador)** para que todos calculen su parte al mismo tiempo y al final junten los resultados. A esas hojas repartidas en memoria se les llama **RDD** (*Resilient Distributed Dataset* o Conjunto de Datos Distribuido Resiliente).

---

### Recorrido paso a paso de lo que construimos

#### 1. Preparación del Entorno (`01_hola_spark.ipynb`)
* **¿Qué hicimos?** Instalamos Java 11 y PySpark 3.5.3 en un entorno aislado (`venv`) en Ubuntu, creamos nuestra primera `SparkSession` ("HolaSpark") y dividimos una lista sencilla de números (`[1, 2, 3, 4, 5]`) en **4 particiones** (trozos de trabajo) para sumar sus elementos en paralelo.
* **¿Por qué importa?** Comprobamos que Spark levantara su panel de control visual (**Spark UI** en `http://localhost:4040`), donde se observa cómo divide las tareas entre los núcleos de nuestra computadora.

#### 2. Dominando las Operaciones Básicas y Optimización (`02_ejercicios_practicos.ipynb`)
* **Transformaciones vs. Acciones (Ejercicios 1 al 3):** Aprendimos que en Spark existen dos tipos de órdenes:
  * **Transformaciones (`map`, `filter`):** Son como anotar la receta en un papel (ej. *"multiplica cada número por 2"* o *"quédate solo con los pares"*). Spark **no hace nada todavía**; solo apunta lo que tendrá que hacer (a esto se le llama *Lazy Evaluation* o evaluación perezosa).
  * **Acciones (`count`, `take`, `sum`, `collect`):** Son el botón de "¡Cocina ya!". En ese instante Spark ejecuta toda la receta de golpe y entrega el resultado.
* **Agrupación por Clave (`reduceByKey`, Ejercicio 4):** Simulamos un registro de ventas por producto (`manzana`, `banana`, `naranja`) y usamos `reduceByKey` para sumar automáticamente todas las ventas de cada fruta sin tener que escribir bucles manuales.
* **Lectura de Archivos Reales y Buenas Prácticas (Ejercicios 5 al 7):**
  * Descargamos `persons.csv` y lo cargamos usando rutas absolutas (`file://`) para evitar errores entre sistemas de archivos.
  * Optimizamos la limpieza de texto haciendo `split(",")` **una sola vez por fila** en lugar de tres veces repetidas.
  * Corregimos un código con 4 errores típicos: inicializar la sesión, corregir rutas relativas, cambiar `collect()` por `take(5)` para no saturar la memoria RAM y cerrar la sesión con `spark.stop()`.

#### 3. Proyecto Final: Análisis Completo de Inventario y Precios (`03_proyecto_stocks.ipynb`)
Aplicamos todo lo aprendido a un caso real con el archivo `stocks.txt` (30 registros de productos organizados por categorías):
1. **Carga y Limpieza (Pasos 1 y 2):** Configuramos una sesión dedicada (`AnalisisStocks` con `512m` de memoria), descartamos el encabezado y convertimos cada línea en una tupla limpia `(categoría, ID, precio, cantidad)`.
2. **Estadísticas por Categoría (Paso 3):** Calculamos el **precio máximo**, **precio mínimo** y **precio promedio** (usando `aggregateByKey`, que lleva la cuenta simultánea de la suma de precios y la cantidad de productos por categoría). Descubrimos que *Electronics* tiene el promedio más alto (`$586.66`).
3. **Análisis de Actividad / Volumen (Paso 4):** Ordenamos los registros de mayor a menor cantidad para detectar los 10 movimientos más grandes, encabezados por `ID-13` con `150 unidades`.
4. **Cálculo de Volatilidad (Paso 5):** Medimos qué tanto varía el precio dentro de cada categoría usando la fórmula `((Máximo - Mínimo) / Mínimo) * 100`. *Food* resultó ser la categoría más volátil (`2296.00%` de variación entre su producto más barato de `$0.25` y el más caro de `$5.99`), mientras que *Accessories* fue la más estable (`20.01%`).
5. **Guardado Distribuido y Resumen Ejecutivo (Paso 6):** Exportamos los resultados directamente a disco en la carpeta `RESULTADOS/` mediante `saveAsTextFile()` y generamos un reporte ejecutivo final antes de liberar los recursos con `spark.stop()`.

---

## Estructura del Repositorio
* `01_hola_spark.ipynb`: Configuración inicial de `SparkSession` y prueba de procesamiento paralelo.
* `02_ejercicios_practicos.ipynb`: Resolución de los 7 ejercicios prácticos (RDDs, `reduceByKey`, CSV, optimización y debugging).
* `03_proyecto_stocks.ipynb`: Proyecto final de análisis de datos sobre `stocks.txt`.
* `DATOS/`: Datasets utilizados (`persons.csv` y `stocks.txt`).
* `RESULTADOS/`: Archivos de salida generados de forma distribuida mediante `saveAsTextFile()`.
* `capturas/`: Evidencias gráficas de ejecución en JupyterLab y monitorización en Spark UI.

---

## Respuestas Detalladas a las Preguntas de Reflexión

### 1. Eficiencia: ¿Por qué es importante hacer `split()` una sola vez por línea? ¿Cuánto tiempo ahorrarías con 1 millón de líneas?
* **Explicación sencilla:** Imagina que tienes una tarjeta con 4 datos escritos y, para leer cada dato, cortas la tarjeta con tijeras desde cero tres veces seguidas en lugar de cortarla una sola vez y dejar los 4 trozos sobre la mesa.
* **Detalle técnico:** Cada llamada a `split()` recorre toda la cadena de texto carácter por carácter y reserva nueva memoria para crear una lista en cada fila. Si ejecutamos `split()` 3 veces por línea, triplicamos el trabajo de CPU y el uso de memoria (recolección de basura de Python/JVM). Con **1 millón de líneas**, en lugar de hacer 1 millón de operaciones de división haríamos 3 millones; evitar esas 2 millones de operaciones redundantes ahorra entre un **50% y un 65% del tiempo de CPU** en la etapa de parseo.

### 2. Particionamiento: ¿Cuántas particiones tiene tu RDD? ¿Cómo afecta esto al rendimiento? ¿Qué pasaría si tuvieras solo 1 partición?
* **Explicación sencilla:** Las particiones son las "cajas de trabajo" en las que se divide tu archivo. Si tienes 4 trabajadores (núcleos) y 4 cajas, todos trabajan al mismo tiempo y terminan 4 veces más rápido. Si metes todo en 1 sola caja, 3 trabajadores se quedan mirando mientras 1 solo carga con todo el peso.
* **Detalle técnico:** En modo `local[*]`, Spark crea por defecto tantas particiones como núcleos lógicos tenga el procesador (en nuestro entorno, **4 particiones**, verificable en la Spark UI). Esto permite procesar 4 bloques de datos en paralelo. Si tuviéramos **solo 1 partición**, perderíamos todo el paralelismo: un único núcleo haría todo el trabajo de forma secuencial y podría provocar errores de falta de memoria (`OutOfMemoryError`) si el archivo es muy grande.

### 3. Lazy Evaluation: ¿En qué momento se ejecutaron realmente las transformaciones? ¿Qué ventajas tiene este modelo?
* **Explicación sencilla:** Es como planear la ruta en Google Maps antes de encender el auto: primero trazas todo el camino, ves dónde hay tráfico y recortas atajos, y solo cuando pisas el acelerador (una *acción*) recorres la ruta ya optimizada.
* **Detalle técnico:** Las transformaciones (`map`, `filter`, `reduceByKey`, `sortBy`) **no se ejecutaron al definirse**; únicamente construyeron el grafo de ejecución lógico (DAG). Se ejecutaron realmente cuando invocamos **acciones** como `count()`, `take()`, `collect()`, `first()` o `saveAsTextFile()`. La gran ventaja es que Spark optimiza todo el plan antes de mover datos (por ejemplo, fusionando pasos en un mismo *Stage* o leyendo solo la primera partición cuando pedimos `take(5)`).

### 4. Memoria: ¿Qué pasaría si intentaras hacer `collect()` en un RDD de 10GB? ¿Cómo evitarías este problema?
* **Explicación sencilla:** `collect()` es como pedirle a 10 camiones de carga que vacíen toda su mercancía al mismo tiempo dentro de la sala de tu casa: la sala se desborda y colapsa.
* **Detalle técnico:** `collect()` intenta traer todos los datos distribuidos desde los *Executors* hacia la memoria RAM de un único nodo: el **Driver**. Si el Driver tiene 1GB o 2GB de RAM asignados e intentamos traer 10GB, el programa fallará con un **`OutOfMemoryError`** (`Py4JJavaError`). Para evitarlo:
  * Usar `.take(n)` para inspeccionar muestras pequeñas.
  * Guardar los resultados directamente desde los trabajadores a disco o HDFS usando `.saveAsTextFile()`.
  * Filtrar y resumir los datos (`filter`, `reduceByKey`) antes de traer únicamente los totales al Driver.

### 5. Optimización: Si tuvieras que procesar 100x más datos, ¿qué cambiarías en tu código? ¿Usarías más particiones? ¿Cachearías algún RDD?
* **Explicación sencilla:** Si el trabajo crece 100 veces, necesitas dividirlo en paquetes más pequeños para que nadie se atore, y dejar sobre la mesa los datos que vas a consultar muchas veces en lugar de ir a buscarlos al archivo cada vez.
* **Detalle técnico:**
  * **Más particiones:** Aumentaría el número de particiones (`repartition()` y `spark.sql.shuffle.partitions`) para que cada tarea procese bloques manejables (~128MB por partición) y aprovechar más núcleos o servidores.
  * **Caché (`persist()` / `cache()`):** Como `rdd_parseado` y `simbolo_precio` se reutilizan múltiples veces (para máximo, mínimo, promedio, volatilidad y resumen final), llamaría a **`rdd_parseado.cache()`** para conservarlo en memoria RAM y evitar que Spark relea y re-parsee el archivo desde disco en cada acción.
  * **DataFrames y Parquet:** Migraría de RDDs a **DataFrames** con formato Parquet para aprovechar el optimizador Catalyst y la compresión columnar.

### 6. HDFS vs Local: ¿Cuáles son las ventajas de usar HDFS en lugar de archivos locales para este análisis?
* **Explicación sencilla:** Un archivo local es como guardar toda tu información en una sola USB: si se llena o se rompe, pierdes todo. HDFS es como una red de almacenes conectados donde tu archivo se guarda repartido y con copias de seguridad automáticas.
* **Detalle técnico:**
  * **Almacenamiento distribuido:** Un archivo local está limitado al disco de una sola máquina; HDFS divide archivos de terabytes en bloques distribuidos entre decenas o cientos de servidores.
  * **Localidad de datos (*Data Locality*):** Spark envía el código exactamente a los nodos donde ya residen los bloques de HDFS, minimizando el tráfico de red.
  * **Tolerancia a fallos:** HDFS replica automáticamente cada bloque (generalmente 3 veces); si un servidor se apaga a mitad del análisis, Spark lee la réplica desde otro nodo sin detener el trabajo.
