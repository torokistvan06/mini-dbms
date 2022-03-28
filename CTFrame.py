from tkinter import *
from socket import *
from tkinter import ttk
import tkinter as tk
from CTRow import CTRow
from server import serverPort
from tkinter import messagebox

class CTFrame:
    def __init__(self, mainFrame: Tk):
        self.mainFrame = mainFrame
        self.window = Tk()
        self.window.title('Create Table')
        self.window.geometry('1200x800+350+100')
        Label(self.window, text = 'Database name:', fg = 'black').place(x = 40, y = 100)
        self.textBox2 = Entry(self.window, textvariable = StringVar(None))
        self.textBox2.place(x = 150, y = 100)
        self.label = Label(self.window, text = 'Table name:' , fg = 'black')
        self.label.place(x = 50,y = 200)
        self.textBox = Entry(self.window, textvariable = StringVar(None))
        self.textBox.place(x = 150, y = 200)
        self.button = Button(self.window,text = 'Create', fg = 'black', command = self.sendData)
        self.button.place(x = 300,y = 200)
        self.label2 = Label(self.window, text = 'Column count:', fg = 'black')
        self.label2.place(x = 400, y = 200)
        self.drop = ttk.Combobox(self.window, textvariable = tk.IntVar())
        self.drop['values'] = [i for i in range(1,25)]
        self.drop['state'] = 'readonly'
        self.drop.current(0)
        self.drop.bind("<<ComboboxSelected>>",self.changeRowCount)
        self.drop.place(x = 450, y = 200)

        self.rows = []
        self.rows.append(CTRow(self.window,250))

    def changeRowCount(self, event):
        for i in self.rows:
            i.clear()

        i = int(self.drop.get())
        self.rows = []
        for j in range (0,i):
            self.rows.append(CTRow(self.window,200 + (j + 1) * 50))


    def sendData(self):
        dataBaseName = self.textBox2.get()
        tableName = self.textBox.get()

        message = "Create Table\n" + dataBaseName + '\n' + tableName

        for i in self.rows:
            message += '\n' + i.getData()

        serverName = 'localhost'
        clientSocket = socket(AF_INET,SOCK_STREAM)
        clientSocket.connect((serverName,serverPort))
        clientSocket.send(message.encode())

        msg = clientSocket.recv(256).decode()
        if msg == '0':
            print('Table created successfully')
        elif msg == '-1':
            messagebox.showerror("Error", "Trying to create table in non-existing database!")
            #print('Trying to create table in non-existing database')
        elif msg == '-2':
            messagebox.showerror("Error", "Trying to create existing table!")
            #print('Trying to create existing table')
        elif msg == '-3':
            messagebox.showerror("Error", "Reference on non-existing table!")
            #print('Reference on non-existing table')
        elif msg == '-4':
            messagebox.showerror("Error", "Reference on non-existing column in table!")
            #print('Reference on non-existing column in table')
            
        clientSocket.close()
        self.destroy()
        self.mainFrame.show()

    def start(self):
        self.window.mainloop()

    def destroy(self):
        self.window.destroy()