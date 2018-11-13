from random import randint
import time
import sys
import os

"""
Crea 30 archivos dinámicamente en la carpeta log , a partir del texto.txt , un nuevo cada 5 segundos  
"""


def main():

    a = 1
    # borra los archivos de la carpeta log
    for file in os.scandir('C:/GIT/SparkStreaming/log'):
        if file.name.endswith(".txt"):
            os.unlink(file.path)

    # Leo el archivo 'texto.txt' file
    with open('C:/GIT/SparkStreaming/texto.txt', 'r') as file:
        lineas = file.readlines()
        while a <= 30:
            numero_lineas = len(lineas)
            random_index = randint(0, numero_lineas - 10)
            with open('C:/GIT/SparkStreaming/log/log{}.txt'.format(a), 'w') as writefile:
                writefile.write(
                    ' '.join(line for line in lineas[random_index:numero_lineas]))
            print('Creando archivo stream[{}].txt'.format(a))
            a += 1
            time.sleep(5)


if __name__ == '__main__':
    main()
