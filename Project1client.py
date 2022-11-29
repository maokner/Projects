from tkinter import *
import matplotlib.pyplot as plt
import socket
from functools import partial
import pickle
statesList = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
              'Delaware', "District of Columbia", 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
              'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
              'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New-Hampshire',
              'New-Jersey', 'New-Mexico', 'New-York', 'North-Carolina', 'North-Dakota', 'Ohio', 'Oklahoma',
              'Oregon', 'Pennsylvania', 'Rhode-Island', 'South-Carolina', 'South-Dakota', 'Tennessee', 'Texas',
              'Utah', 'Vermont', 'Virginia', 'Washington', 'West-Virginia', 'Wisconsin', 'Wyoming']
yearslist = [i for i in range(1970, 2021)]

def drawgraph(tuple):
    plt.title(tuple[0])
    tuple = tuple[1:]
    plt.plot(yearslist, tuple)
    plt.show()


def activateClient(message):
    HEADERSIZE = 10
    host = socket.gethostname()
    port = 1024
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    while True:
        client_socket.send(message.encode())
        full_msg = b""
        new_msg = True
        while True:
            msg = client_socket.recv(16)
            if new_msg:
                msglen = int(msg[:HEADERSIZE])
                new_msg = False

            full_msg += msg

            if len(full_msg) - HEADERSIZE == msglen:
                data = pickle.loads(full_msg[HEADERSIZE:])
                data = data[:-4]
                drawgraph(data)
                break

        client_socket.close()


def buttoncommand(x):
    activateClient(x)

def placebuttons():
    window = Tk()
    i = 0
    for x in range(10):
        for y in range(5):
            state = statesList[i]
            Button(window, text=state, command= partial(buttoncommand, state)).grid(row=x, column=y)
            i += 1
    window.mainloop()




def main():
    placebuttons()



main()
