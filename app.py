import tkinter as tk

import db
from ui_main import MainWindow


def main():
    db.initialize_database()
    root = tk.Tk()
    root.title("RugBase Inventory")
    root.geometry("1000x600")
    MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
