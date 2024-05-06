# External packages
from tkinter import *
from tkinter import ttk

class GUI:
    """
    Represents a graphical user interface (GUI) manager for a Tkinter application.

    Attributes:
        root (Tk): The Tkinter root window for the GUI.
        text_frame (Frame): The frame for displaying text elements.
        button_frame (Frame): The frame for displaying buttons.
        dimensions (list): A list containing the width and height dimensions of the GUI window.

    Methods:
        __init__(root, title, dimensions): Initializes the GUI with the specified root window, title, and dimensions.
        set_title(title): Sets the title of the root window.
        set_geom(geometry): Sets the geometry of the root window.
        add_text_frame(): Adds a frame for displaying text elements.
        remove_text_frame(): Removes the text frame from the GUI.
        reset_text_frame(): Removes and recreates the text frame.
        add_label(text, text_frame=True, pady=20): Adds a label with the specified text to either the text frame or the root window.
        add_button_frame(): Adds a frame for displaying buttons.
        remove_button_frame(): Removes the button frame from the GUI.
        reset_button_frame(): Removes and recreates the button frame.
        add_button(text, command, side, padx=0, pady=0): Adds a button with the specified text, command, and layout options.
        add_progress_bar(max, len, var): Adds a progress bar with the specified maximum value, length, and variable.
        remove_progress_bar(progress_bar): Removes the specified progress bar from the GUI.
        center_window(top=True): Centers the GUI window on the screen or moves it to the left by half its width if top is False.
        run(): Enters the Tkinter main event loop to start the GUI application.
        close(): Destroys the root window to close the GUI application.
    """

    root = None
    text_frame = None
    button_frame = None
    dimensions = None

    def __init__(self, root, title, dimensions):
        self.root = root
        self.dimensions = dimensions.split('x')
        self.root.title(title)
        self.root.geometry(dimensions)

    def set_title(self, title):
        self.root.title = title

    def set_geom(self, geometry):
        self.root.geometry(geometry)

    def add_text_frame(self):
        self.text_frame = Frame(self.root)
        self.text_frame.pack()

    def remove_text_frame(self):
        self.text_frame.destroy()

    def reset_text_frame(self):
        self.text_frame.destroy()
        self.add_text_frame()
    
    def add_label(self, text, text_frame=True, pady=20):
        if text_frame:
            label = Label(self.text_frame, text=text)
        else:
            label = Label(self.root, text=text)
        label.pack(pady=pady)
        self.root.update()

        return label

    def add_button_frame(self):
        self.button_frame = Frame(self.root)
        self.button_frame.pack()

    def remove_button_frame(self):
        self.button_frame.destroy()

    def reset_button_frame(self):
        self.button_frame.destroy()
        self.add_button_frame()

    def add_button(self, text, command, side, padx=0, pady=0):
        button = Button(self.button_frame, text=text, command=command, width=10)
        button.pack(side=side, padx=padx, pady=pady)

    def add_progres_bar(self, max, len, var):
        self.progress_bar = ttk.Progressbar(self.root, maximum=max, len=len,
                                            variable=var)
        self.progress_bar.pack(pady=20)

    def remove_progress_bar(self, progress_bar):
        progress_bar.destroy()

    def center_window(self, top=True):
        # Get the screen width and height
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()

        # Get the window width and height
        window_width = self.root.winfo_reqwidth()
        window_height = self.root.winfo_reqheight()

        # Calculate the x and y coordinates to center the window
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2

        # Position window to side to center top-level window(s)
        if not top:
            x -= int(self.dimensions[0])
            y -= int(self.dimensions[1])

        # Set the window position
        self.root.geometry(f"+{x}+{y}")

    def run(self):
        self.root.mainloop()

    def close(self):
        self.root.destroy()
        
