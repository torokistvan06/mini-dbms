from time import sleep
from tkinter import *
from socket import *
from server import serverPort
from tkinter import messagebox

class DTFrame:
    def __init__(self, mainFrame: Tk):
        self.mainFrame = mainFrame
        self.window = Tk()
        self.window.title('Delete Table')
        self.window.geometry('1200x800+350+100')
        Label(self.window, text = 'Table name:' , fg = 'black').place(x = 200,y = 200)
        Label(self.window, text = 'Database name', fg = 'black').place(x = 200, y= 100)
        self.textBox = Entry(self.window, textvariable = StringVar(None), width = 20)
        self.textBox.place(x = 300, y = 200)
        self.textBox2 = Entry(self.window, textvariable = StringVar(None), width = 20)
        self.textBox2.place(x = 300, y = 100)
        self.button = Button(self.window,text = 'Delete', fg = 'black', command = self.sendData)
        self.button.place(x = 450,y = 200)
                
    def sendData(self):
        databaseName = self.textBox2.get()
        tableName = self.textBox.get()

        message = "Delete Table\n" + databaseName + '\n' + tableName

        serverName = 'localhost'
        clientSocket = socket(AF_INET,SOCK_STREAM)
        clientSocket.connect((serverName,serverPort))
        clientSocket.send(message.encode())
        msg = clientSocket.recv(256).decode()
        if msg == '0':
            messagebox.showinfo("Success", "Successfully deleted database")
            #print('Successfully deleted table')
        if msg == '-1':
            messagebox.showerror("Error", "Trying to delete from non-existing database!")
            #print('Trying to delete from non-existing database')
        elif msg == '-2':
            messagebox.showerror("Error", "Trying to delete non-existing table!")
            #print('Trying to delete non-existing table')
        elif msg == '-3':
            messagebox.showerror("Error", "Trying to delete table that is referenced by another table!")
            #print('Trying to delete table that is referenced by another table')
        clientSocket.close()

        self.destroy()
        self.mainFrame.show()

    def start(self):
        self.window.mainloop()

    def destroy(self):
        self.window.destroy()