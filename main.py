from src.sensor import RandCamera

if __name__ == "__main__":
    p = RandCamera()
    p.start()
    p.join()
