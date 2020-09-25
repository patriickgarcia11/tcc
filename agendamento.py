import schedule
import time


def test():
    print('Hello, World!')


schedule.every().day.at_time('12:00')

while 1:
    schedule.run_pending()
    time.sleep(1)
