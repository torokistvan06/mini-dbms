from tkinter import *
from tkinter import ttk
class CTRow:
    def __init__(self, window: Tk, yy: int):
        self.window = window
        self.y = yy
        self.label = Label(window, text = 'Column name:', fg = 'black')
        self.label.place(x = 50, y = self.y)
        self.label2 = Label(window, text='Type:', fg='black')
        self.label2.place(x=300, y=self.y)
        self.entry = Entry(window, textvariable = StringVar())
        self.entry.place(x = 150, y = self.y)
        options = ['int', 'float', 'bit', 'date', 'datetime', 'string']
        self.option = ttk.Combobox(window,textvariable = StringVar())
        self.option['values'] = [options[i] for i in range (0,6)]
        self.option['state'] = 'readonly'
        self.option.current(0)
        self.option.place(x = 330, y = self.y)
        self.isUnique = IntVar()
        self.checkbox = Checkbutton(window, text = 'Unique', variable = self.isUnique)
        self.checkbox.place(x = 530,y = self.y)
        self.isIndex = IntVar()
        self.checkbox2 = Checkbutton(window, text = 'Index', variable = self.isIndex)
        self.checkbox2.place(x = 605,y = self.y)
        self.isPrimary = IntVar()
        self.checkbox3 = Checkbutton(window, text = 'Primary Key', variable = self.isPrimary)
        self.checkbox3.place(x = 665,y = self.y)
        self.isForeign = IntVar()
        self.checkbox4 = Checkbutton(window, text = 'Foreign Key on Table(key):', variable = self.isForeign)
        self.checkbox4.place(x = 760,y = self.y)
        self.entry2 = Entry(window, textvariable = StringVar())
        self.entry2.place(x = 970,y = self.y)

    def clear(self):
        self.label.destroy()
        self.entry.destroy()
        self.option.destroy()
        self.checkbox.destroy()
        self.checkbox2.destroy()
        self.checkbox3.destroy()
        self.checkbox4.destroy()
        self.entry2.destroy()
        self.label2.destroy()
    
    def getData(self):
        return self.entry.get() + ' ' + self.option.get() + ' ' + self.checkbox.getvar(str(self.checkbox.cget('variable'))) + ' ' + self.checkbox.getvar(str(self.checkbox2.cget('variable'))) + ' ' + self.checkbox.getvar(str(self.checkbox3.cget('variable'))) + ' ' + self.checkbox.getvar( str(self.checkbox4.cget('variable'))) + ' ' + self.entry2.get()