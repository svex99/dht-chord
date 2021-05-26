# Chord DHT

Para comprobar el proyecto primero es necesario contruir la imagen. La imagen tiene como base `docker.uclv.cu/python:3.8.5`, por tanto si no desea descargar esa imagen, porque tiene otra que presupone deba funcionar 🤞, modifique esto en el `Dockerfile` del proyecto.

Para obtener la imagen ejecute el comando:

```
docker image build -t xevs/chord .
```

Una vez la imagen lista, con el siguiente comando se levantan los contenedores según están definidos en el `docker-compose.yml`, redis, el channel que hace de intermediario entre redis y los nodos, 5 nodos y un cliente. 

```
docker-compose up -d
```

Una vez estén los contenedores listos el nodo cliente realiza un request y termina la ejecución suya y de los nodos una vez este request se resuelva de forma satisfactoria.

Para verificar la correcta ejecución use el comando:

```
docker container logs chord_client
```

La últimas tres líneas de la salida deben ser similar a las mostradas a continuación:

```
[ INFO/2021-05-26 03:31:37,798] root: Invoked RPC: recvFrom, [[8]], {'timeout': 0}
[ INFO/2021-05-26 03:31:38,900] root: 30 received final answer from 8
[ INFO/2021-05-26 03:31:38,901] root: Invoked RPC: sendTo, [[4, 8, 23, 24, 26], '6'], {}
```

Concentrándonos en la penúltima de las líneas vemos que el nodo cliente (en este caso con ID 30) logró resolver la llave de forma correcta desde el nodo con ID 8.

Puede verificar los logs de los demás contenedores con los siguientes comandos:

| contenedor | comando                               |
| ---------- | ------------------------------------- |
| redis      | `docker container logs chord_redis`   |
| channel    | `docker container logs chord_channel` |
| node i     | `docker container logs node{i}`       |