from time import sleep
from tkinter import *
from socket import *
from server import serverPort

class DDFrame:
    def __init__(self, mainFrame: Tk):
        self.mainFrame = mainFrame
        self.window = Tk()
        self.window.title('Delete Database')
        self.window.geometry('1200x800+350+100')
        self.label = Label(self.window, text = 'Database name:' , fg = 'black')
        self.label.place(x = 200,y = 200)
        self.textBox = Entry(self.window, textvariable = StringVar(None), width = 20)
        self.textBox.place(x = 330, y = 200)
        self.button = Button(self.window,text = 'Delete', fg = 'black', command = self.sendData)
        self.button.place(x = 480,y = 200)
                
    def sendData(self):
        data = self.textBox.get()

        message = "Delete Database\n" + data

        serverName = 'localhost'
        clientSocket = socket(AF_INET,SOCK_STREAM)
        clientSocket.connect((serverName,serverPort))
        clientSocket.send(message.encode())
        msg = clientSocket.recv(256).decode()
        if msg == '0':
            print('Successfully deleted database')
        if msg == '-1':
            print('Trying to delete non-existing database')
        clientSocket.close()

        self.destroy()
        self.mainFrame.show()

    def start(self):
        self.window.mainloop()

    def destroy(self):
        self.window.destroy()