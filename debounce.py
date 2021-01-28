import threading


def debounce(wait=3):
    def deco(fn):
        timer: threading.Timer = None

        def wrap(*args):
            nonlocal timer

            if timer is None:
                timer = threading.Timer(wait, fn, args)
                timer.start()
            else:
                timer.cancel()
                timer = threading.Timer(wait, fn, args)
                timer.start()

        return wrap

    return deco
