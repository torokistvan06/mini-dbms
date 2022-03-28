from tkinter import *
from socket import *
from server import serverPort

class CDFrame:
    def __init__(self, mainFrame: Tk):
        self.mainFrame = mainFrame
        self.window = Tk()
        self.window.title('Create Database')
        self.window.geometry('1200x800+350+100')
        self.label = Label(self.window, text = 'Database name:' , fg = 'black')
        self.label.place(x = 200,y = 200)
        self.textBox = Entry(self.window, textvariable = StringVar(None), width = 20)
        self.textBox.place(x = 400, y = 200)
        self.button = Button(self.window,text = 'Create', fg = 'black', command = self.sendData)
        self.button.place(x = 600,y = 200)
                
    def sendData(self):
        data = self.textBox.get()

        message = "Create Database\n" + data

        serverName = 'localhost'
        
        clientSocket = socket(AF_INET,SOCK_STREAM)
        clientSocket.connect((serverName,serverPort))
        clientSocket.send(message.encode())

        msg = clientSocket.recv(256).decode()
        if msg == '0':
            print('Database succesfully created')
        elif msg == '-1':
            print("Trying to create existing database")

        clientSocket.close()
        self.destroy()
        self.mainFrame.show()

    def start(self):
        self.window.mainloop()

    def destroy(self):
        self.window.destroy()