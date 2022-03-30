from curses import window
from tkinter import *
from CDFrame import CDFrame as CDF
from CTFrame import CTFrame as CTF
from DDFrame import DDFrame as DDF
from DTFrame import DTFrame as DTF
from InsertFrame import InsertFrame as IF
from DeleteFrame import DeleteFrame as DF

class MainFrame:
    def __init__(self):
        self.window = Tk()
        self.window.title('ABKR')
        self.window.geometry("1200x800+350+100")
        self.CDButton = self.createButton(self.window, 'Create Database', 350, 200, self.createDatabase)
        self.CTButton = self.createButton(self.window, 'Create Table', 350, 350, self.createTable)
        self.DDButton = self.createButton(self.window, 'Drop Database', 620, 200, self.deleteDatabase)
        self.DTButton = self.createButton(self.window, 'Drop Table', 620, 350, self.deleteTable)
        self.InsertButton = self.createButton(self.window, 'Insert', 350, 500, self.insertData)
        self.DeleteButton = self.createButton(self.window, 'Delete', 620, 500, self.deleteData)
        self.window.mainloop()

    def createButton(self, window: Tk, t: str, xx: int, yy :int, func):
        aButton = Button(window, text = t,fg = 'black', command = func)
        aButton.place(x = xx, y = yy)

    def createDatabase(self):
        CDFrame = CDF(self)
        self.close()
        CDFrame.start()

    def createTable(self):
        CTFrame = CTF(self)
        self.close()
        CTFrame.start()

    def deleteDatabase(self):
        CDFrame = DDF(self)
        self.close()
        CDFrame.start()

    def deleteTable(self):
        CTFrame = DTF(self)
        self.close()
        CTFrame.start()

    def insertData(self):
        InsertFrame = IF(self)
        self.close()
        InsertFrame.start()

    def deleteData(self):
        DeleteFrame = DF(self)
        self.close();
        DeleteFrame.start()

    def show(self):
        self.window.deiconify()

    def close(self):
        self.window.withdraw()
    
    