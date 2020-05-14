from tkinter import Tk,Label,Entry,Button,OUTSIDE
from PIL import ImageTk, Image
import os

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

#Search Entry Box
e = Entry(root)
e.pack()
e.place(height=22, width=200, relx = .35, rely = .45)
e.focus_set()

#Search Button
btn = Button(root, text = 'Search', command = root.destroy) 
btn.pack()
btn.place(bordermode=OUTSIDE, height=20, width=100, relx = .41, rely = .5)

#Kickstart mainloop
root.mainloop()