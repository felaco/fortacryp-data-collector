# Fortacrypt Data collector

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/51365674d17041219ba3104572953fd2)](https://app.codacy.com/app/felaco/fortacryp-data-collector?utm_source=github.com&utm_medium=referral&utm_content=felaco/fortacryp-data-collector&utm_campaign=Badge_Grade_Settings)
[![Build Status](https://travis-ci.org/felaco/fortacryp-data-collector.svg?branch=master)](https://travis-ci.org/felaco/fortacryp-data-collector)

Proyecto utilizado para recopilar datos de exchanges de crypto monedas y almacenarlos.
Actualmente solo los almacena en archivos csv.

## Fuentes de datos
Actualmente se ha integrado a tres fuentes de datos: [Buda](https://www.buda.com), [crypto compare](https://www.cryptocompare.com/)
y [kraken](https://www.kraken.com). Este ultimo posee una integración unicamente mediante websocket.
Las crypto monedas que se recuperan mediante los script son: Bitcoin (btc), Ethereum (eth), Bitcoin cash
(bch) y litecoin (ltc) que son las crypto monedas que transa el exchange Chileno Buda
 

## Configuración 
El repositorio no cuenta con un archivo de configuración creado, pero este se genera al correr el proyecto por
primera vez en la carpeta raiz con el nombre de config.json. Los valores por defecto se encuentran en el
script core.config._config. Realmente no es necesario editar la configuración y no se aconseja hacerlo,
puesto que los valores que se insertan allí se utilizan para mantener la consistencia entre diferentes 
ejecuciones.

Los datos historicos se almacenan en formato ohlc en intervalos de 1 hora.

## Ejecutar
El punto de entrada del proyecto es la interfaz cli FortacryptCLI.py 
por lo que la forma de rescatar los datos para cada integración es:

`python FortacryptCLI.py {integración} {moneda}`

siendo integración una de las opciones: buda, cryptoCompare o kraken.

### Kraken
La integración con kraken está pensada para servir como trigger para alertas mediante telegram
indicando si se cumple alguna condición (alguna señal buy/sell de algún indicador o la variación % en 24h, etc)
por lo que necesita de datos hístoricos. La integración con kraken fallará si es que no se han recuperado
los datos historicos de esa moneda mediante el uso de la integración con crypto compare.
Además se requiere que se hayan seteado las variables de entorno `FORTACRYP_BOT_ID` y `FORTACRYP_CHAT_ID`
que corresponden a la identidad del bot (ver: [Cómo crear un bot de telegram](https://core.telegram.org/bots#3-how-do-i-create-a-bot))
y el chat hacia el cual se quiere notificar. Sin estas variables no es posible saber como realizar la notificación.


