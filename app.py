import tkinter as tk

import db
from core.version import __version__
from ui_main import MainWindow


def main() -> None:
    db.initialize_database()
    root = tk.Tk()
    root.title(f"RugBase Inventory v{__version__}")
    root.geometry("1000x600")
    MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
