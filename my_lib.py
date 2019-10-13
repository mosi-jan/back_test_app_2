from termcolor import colored


class PrintColored:
    def __init__(self, default_color):
        self.default_color = default_color

    def print(self, text='', color=None):
        try:
            if color is None:
                print(colored(text, self.default_color))
            else:
                print(colored('| ', self.default_color) + colored(text, color))
        except Exception as e:
            print(str(e))
