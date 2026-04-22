# Trabajo Práctico - Coordinación

# Informe

## Coordinacion de instancias de Sum

Todas las instancias de Sum comparten una cola de entrada, input_queue, para la cual el middleware distribuye los mensajes de fruta-cantidad de manera round robin.

Cada instancia mantiene un total por fruta para cada cliente. Cuando recibe un mensaje de EOF, (el client_id solo), utiliza un exchange de "control" y realiza un broadcast a todas las instancias de Sum. Luego, todas reciben el EOF y utilizan un flag eof_handled_by_client para evitar duplicados. 

La instancia que procesa el EOF envía los totales acumulados a las instancias de Aggregation, distribuyendo los datos por la primera letra de cada fruta (`ord(letra) % AGGREGATION_AMOUNT`) utilizando routing keys específicas. Luego envía un mensaje EOF a cada Aggregator.

## Coordinación de instancias de Aggregation

Cada instancia de Aggregation escucha un exchange con un routing key propio (`AGGREGATION_PREFIX_{ID}`), de modo que recibe solo las frutas que le corresponden según el criterio de distribución del Sum.

Para detectar el fin del envio de una instancia de SUM, cada Aggregator cuenta los EOFs recibidos por cliente. Como cada instancia de Sum envía un EOF a cada Aggregator, el Aggregator espera recibir `SUM_AMOUNT` EOFs antes de procesar el resultado. Una vez recibidos, calcula un top parcial de `TOP_SIZE` frutas y lo envía al Join, seguido de un mensaje EOF.

## Coordinación del Join

El Join recibe los tops parciales de todas las instancias de Aggregation. Cuenta los EOFs por cliente y espera `AGGREGATION_AMOUNT` EOFs antes de calcular el top final. El resultado se envía al Gateway incluyendo el `client_id` para que pueda ser entregado al cliente que corresponde.

## Escalabilidad

### Respecto a los clientes

Cada cliente recibe un `client_id` único que se propaga a través de todo el pipeline. Los componentes internos (Sum, Aggregation, Join) mantienen diccionarios indexados por `client_id`, lo que permite procesar múltiples consultas concurrentemente sin mezclar datos.

### Respecto a la cantidad de controles

- **Sum**: Se escala agregando más instancias que comparten la misma cola de entrada. RabbitMQ distribuye la carga automáticamente. El exchange de control asegura que todas las instancias se enteren del EOF independientemente de cuál lo reciba.
- **Aggregation**: Se escala agregando más instancias, cada una con un routing key distinto. El criterio de distribución por letra garantiza que cada fruta sea procesada por un único Aggregator, evitando procesamiento redundante.
- **Join**: Es un componente singular que consolida los tops parciales de todos los Aggregators.

# Enunciado

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.
