from random import randint
import time
import sys
import os

""" 
Simula un flujo constante de archivos en la carpeta log

"""


def bufferClean():
    # borra el flujo de datos
    for file in os.scandir('C:/GIT/SparkStreaming/log'):
        if file.name.endswith(".txt"):
            os.unlink(file.path)


def main():
    intervalo_seg = 2   # cada cuanto genera un flujo
    a = 1
    tamañoBuffer = 8    # numero de archivos en la carpeta

    # Leo el archivo 'texto.txt' file
    with open('C:/GIT/SparkStreaming/texto.txt', 'r') as file:
        lineas = file.readlines()
        while a <= tamañoBuffer:
            numero_lineas = len(lineas)
            random_index = randint(0, numero_lineas - 10)
            with open('C:/GIT/SparkStreaming/log/stream[{}].txt'.format(a), 'w') as writefile:
                writefile.write(
                    ' '.join(line for line in lineas[random_index:numero_lineas]))
            print('Send -> stream[{}].txt'.format(a))
            a += 1

            if a == tamañoBuffer:
                bufferClean()
                a = 0

            time.sleep(intervalo_seg)


if __name__ == '__main__':
    bufferClean()
    main()
