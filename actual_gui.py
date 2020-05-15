from tkinter import Tk,Label,Entry,Button,OUTSIDE,LEFT
from PIL import ImageTk, Image
from query_gui import query
import os

searcher = None


#Start the root
root = Tk()

#Parameters for root/window
root.title("Burrito Search")
root.geometry("800x500")
root.resizable(False, False)

#Cool Text title <3
w = Label(root, text="Burrito Searcher\nBurrito free since 2020", font = "Helvetica 16 bold italic")
w.pack()
#t.insert(Tk.END, "Burrito Searcher\nBurrito free since 2020")

#Cool image memes
img = ImageTk.PhotoImage(Image.open("Unsee.jpg"))
panel = Label(root, image = img)
panel.pack(side = "bottom", fill = "both", expand = "no")

def pressed_enter(event=None):
    start_query()

def start_query():
    global e
    search = e.get()
    top_five = searcher.run_query(search)
    main_text = ""
    for i in top_five:
        main_text = main_text + i[0] + i[1] + i[2]
        main_text += '\n'
    result = Label(root, text = main_text, justify=LEFT)
    result.pack()
    result.place(height = 250, width = root.winfo_width(), rely = .4)

#Search Entry Box
e = Entry(root)
e.pack()
e.place(height=22, width=200, relx = .35, rely = .15)
e.focus_set()
e.bind("<Return>", pressed_enter)

#Search Button
btn = Button(root, text = 'Search', command = start_query) 
btn.pack()
btn.place(bordermode=OUTSIDE, height=20, width=100, relx = .41, rely = .2)

#Kickstart mainloop
searcher = query()
root.mainloop()
