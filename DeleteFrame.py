from tkinter import *
from socket import *
from tkinter import ttk
import tkinter as tk
from CTRow import CTRow
from server import serverPort
from tkinter import messagebox

class DeleteFrame:
    def __init__(self, mainFrame):
        self.mainFrame = mainFrame
        self.window = Tk()
        self.window.title('Delete Data')
        self.window.geometry('1200x800+350+100')
        Label(self.window, text = 'Database name:', fg = 'black').place(x = 40, y = 100)
        self.textBox2 = Entry(self.window, textvariable = StringVar(None))
        self.textBox2.place(x = 150, y = 100)
        self.label = Label(self.window, text = 'Table name:' , fg = 'black')
        self.label.place(x = 50,y = 200)
        self.textBox = Entry(self.window, textvariable = StringVar(None))
        self.textBox.place(x = 150, y = 200)
        self.button = Button(self.window,text = 'Delete', fg = 'black', command = self.sendData)
        self.button.place(x = 300,y = 200)
        self.label2 = Label(self.window, text = 'Where:', fg = 'black');
        self.label2.place(x = 50, y = 350)
        self.textBox3 = Entry(self.window, textvariable = StringVar(None), width = 75)
        self.textBox3.place(x = 150, y = 350)

    def sendData(self):
        dataBaseName = self.textBox2.get()
        tableName = self.textBox.get()
        conditions = self.textBox3.get()

        message = "Delete\n" + dataBaseName + '\n' + tableName + '\n' + conditions

        serverName = 'localhost'
        clientSocket = socket(AF_INET,SOCK_STREAM)
        clientSocket.connect((serverName,serverPort))
        clientSocket.send(message.encode())

        msg = clientSocket.recv(256).decode()
        if msg == '0':
            print('Data inserted successfully')
        elif msg == '-1':
            messagebox.showerror("Error", "Wrong input types!")
            #print('Wrong input types')
        elif msg == '-2':
            messagebox.showerror("Error", "Trying to delete from non-existing database")
            #print('Primary Key already exists in database')
        elif msg == '-3':
            messagebox.showerror("Error", "Trying to delete from non-existing table")
            #print('Reference on non-existing table')
        elif msg == '-4':
            messagebox.showerror("Error", "Bad separators")
            #print('Reference on non-existing column in table')
        elif msg == '-5':
            messagebox.showerror("Error", "Bad conditions")
            
        clientSocket.close()

        self.destroy()
        self.mainFrame.show()

    def start(self):
        self.window.mainloop()

    def destroy(self):
        self.window.destroy()