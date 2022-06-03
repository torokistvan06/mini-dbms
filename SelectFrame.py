from tkinter import *
from socket import *
from tkinter import ttk
import tkinter as tk
from CTRow import CTRow
from server import serverPort
from tkinter import messagebox

class SelectFrame:
    def __init__(self, mainFrame):
        self.mainFrame = mainFrame
        self.window = Tk()
        self.window.title('Delete Data')
        self.window.geometry('1200x800+350+100')
        Label(self.window, text = 'Database name:', fg = 'black').place(x = 40, y = 100)
        self.textBox2 = Entry(self.window, textvariable = StringVar(None))
        self.textBox2.place(x = 150, y = 100)
        self.button = Button(self.window,text = 'Select', fg = 'black', command = self.sendData)
        self.button.place(x = 300,y = 200)
        self.label2 = Label(self.window, text = 'Select', fg = 'black');
        self.label2.place(x = 50, y = 350)
        self.textBox3 = Entry(self.window, textvariable = StringVar(None), width = 75)
        self.textBox3.place(x = 150, y = 350)
        self.label2 = Label(self.window, text = 'From', fg = 'black');
        self.label2.place(x = 50, y = 410)
        self.textBox4 = Entry(self.window, textvariable = StringVar(None), width = 75)
        self.textBox4.place(x = 150, y = 410)
        self.label2 = Label(self.window, text = 'Where', fg = 'black');
        self.label2.place(x = 50, y = 470)
        self.textBox5 = Entry(self.window, textvariable = StringVar(None), width = 75)
        self.textBox5.place(x = 150, y = 470)
        self.label2 = Label(self.window, text = 'Join', fg = 'black');
        self.label2.place(x = 50, y = 530)
        self.textBox6 = Entry(self.window, textvariable = StringVar(None), width = 75)
        self.textBox6.place(x = 150, y = 530)
        self.label2 = Label(self.window, text = 'Group by', fg = 'black');
        self.label2.place(x = 50, y = 590)
        self.textBox7 = Entry(self.window, textvariable = StringVar(None), width = 75)
        self.textBox7.place(x = 150, y = 590)
        self.label2 = Label(self.window, text = 'Having', fg = 'black');
        self.label2.place(x = 50, y = 650)
        self.textBox8 = Entry(self.window, textvariable = StringVar(None), width = 75)
        self.textBox8.place(x = 150, y = 650)

    def sendData(self):
        dataBaseName = self.textBox2.get()
        dataName = self.textBox3.get()
        tableName = self.textBox4.get()
        conditions = self.textBox5.get();
        joins = self.textBox6.get()
        groups = self.textBox7.get()


        message = "Select\n" + dataBaseName + '\nSELECT ' + dataName + '\nFROM ' + tableName + '\nWHERE ' + conditions + '\nJOIN ' + joins + '\nGROUP BY ' + groups
        print(message);

        serverName = 'localhost'
        clientSocket = socket(AF_INET,SOCK_STREAM)
        clientSocket.connect((serverName,serverPort))
        clientSocket.send(message.encode())

        msg = clientSocket.recv(256).decode()

        if msg == '0':
            print('Data inserted successfully')
        elif msg == '-1':
            messagebox.showerror("Error", "Wrong input types!")
        elif msg == '-2':
            messagebox.showerror("Error", "Trying to delete from non-existing database")
        elif msg == '-3':
            messagebox.showerror("Error", "Trying to delete from non-existing table")
        elif msg == '-4':
            messagebox.showerror("Error", "Bad separators")
        elif msg == '-5':
            messagebox.showerror("Error", "Bad conditions")
        elif msg == '-7':
            messagebox.showerror("Error", "Column doesn't exists")
        elif msg == '-8':
            messagebox.showerror("Error", "Trying to group by with non selected column")
        elif msg == '-9':
            messagebox.showerror("Error", "Cannot group by on SELECT *")
        elif msg == '-10':
            messagebox.showerror("Error", "Row referenced by child table")
        elif msg == '-11':
            messagebox.showerror("Error", "Unsupported/Invalid type of join")
            
        outFile = open('clientOutput.txt', 'r')
        print(outFile.read())
        clientSocket.close()

        self.destroy()
        self.mainFrame.show()

    def start(self):
        self.window.mainloop()

    def destroy(self):
        self.window.destroy()