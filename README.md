# Surbtc-ANN

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/51365674d17041219ba3104572953fd2)](https://app.codacy.com/app/felaco/fortacryp-data-collector?utm_source=github.com&utm_medium=referral&utm_content=felaco/fortacryp-data-collector&utm_campaign=Badge_Grade_Settings)

Este es un proyecto personal en el cual se busca utilizar la data de las transacciones publicas del exchange de bitcoin Buda.com como entrada para una algún método de regresión.

Este proyecto aún se encuentra en fase muy preliminar , puesto que no se ha logrado obtener resultados aceptables. La principal causa es, al igual que para cualquier serie de tiempo en cualquier mercado, es que es muy difícil capturar su variabilidad a partir de los datos históricos. En ese sentido es probablemente imposible determinar con precisión como se comportará el precio cambiario de una divisa.

En un futuro implementaré algunas ideas que tengo en mente, ademas de otras que encuentre mientras continúo estudiando. Estas ideas se mostrarán utilizando jupyter notebooks.

## Ejecución
Es recomendable utilizar una distribución de python basada en [anaconda](https://www.anaconda.com/download/), puesto que facilita la instalación de paquetes dependientes del SO, que de lo contrario necesitarían ser compilados manualmente.

Crear un entorno virtual utilizando anaconda

    conda create -n surbtc
    # sistemas tipo unix
    source activate surbtc
    # windows
    activate surbtc

Instalar las dependencias. Éstas se encuentran separadas en dos archivos, en conda_requirements se encuentran las dependencias que que son más fácil instalarlas desde anaconda, mientras que pip_requirements contiene las dependencias de pip.

    conda install --file conda_requirements.txt
    pip install -r pip_requirements.txt

Los scripts mas importantes son surbtc.py y flask_api.py. El primero se encarga de recuperar todas las transacciones que han ocurrido en el exchange en bitcoin y ether, para almacenarlas en archivos .csv. El segundo, crea un miniservidor en flask para comunicarse con otras aplicaciones. Los demás scripts son utilidades para preprocesamiento y entrenamiento de redes neuronales.

Recuperar las transacciones desde el exchange [buda](https://www.buda.com)

    pip surbtc.py

exponer las funcionalidades principales con un api rest

    pip flask_api.py
