from src.client import Client
import signal
import sys

def signal_handler(sig, frame):
    print('\nKeyboard interrupt received, exiting...')
    c.close()
    sys.exit(0)

c = Client()
c.request("randcam")
c.run()

# Register the signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)

try:
    while True:
        print(c.latest("randcam")[0].shape)
except KeyboardInterrupt:
    print('\nKeyboard interrupt received, exiting...')
    c.close()