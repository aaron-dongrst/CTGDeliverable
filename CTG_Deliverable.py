import os
import threading
import csv
from datetime import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

class ctgData:
    def __init__(self, dataFile): #creates instance 
        self.dataFile = dataFile #stores data from file
        self.dataQueue = Queue() #Create a queue to store each data set
        self.cleanedData = []

    def loadData(self):
        # Goes through each file
        files = [f for f in os.listdir(self.dataFile)]
        # Worker function to process each file
        def worker(file):
            filePath = os.path.join(self.dataFile, file)
            try:
                with open(filePath, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        # Adding each row of trade data to the queue
                        self.dataQueue.put(row)
            except Exception as e:
                print(f"File Error")
        
        # Use ThreadPoolExecutor to limit the number of threads
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(worker, files)

        print(f"Number of Loaded Rows: {self.dataQueue.qsize()}")
        print(f"Number of Loaded Files {len(files)}")

    def cleanData(self):
        market_open = datetime.strptime('09:30:00', '%H:%M:%S').time() #create datetime objects
        market_close = datetime.strptime('16:00:00', '%H:%M:%S').time()

        seen_entries = set()  # To track duplicates

        while not self.dataQueue.empty():
            entry = self.dataQueue.get()

            #skip if not full
            if not all(entry.values()):
                continue 

            # creates datetime timestamp 
            try:
                timestamp = datetime.strptime(entry['Timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                continue 

            
            if timestamp.weekday() > 4:  
                continue
            if not (timestamp.time() >= market_open and timestamp.time() <= market_close):
                continue

            try:
                price = float(entry['Price'])
                if not price > 0:
                    continue
            except ValueError:
                continue  

            #check for duplicates
            currentEntry = (entry['Timestamp'], entry['Price'], entry['Size'])

            if currentEntry in seen_entries:
                continue  
            seen_entries.add(currentEntry)

            
            self.cleanedData.append(entry)

        print(f"Number of Cleaned Rows: {len(self.cleanedData)}")
        
    def parseInterval(self, intervalString):
        timeMap = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
        totalSeconds = 0
        num = ''
        for char in intervalString:
            if char.isdigit():
                num += char
            else:
                totalSeconds += int(num) * timeMap[char]
                num = ''
        return totalSeconds
    def processOHLCV(self, data):
        prices = [float(d['Price']) for d in data]
        volumes = [float(d['Size']) for d in data]
        return {
        'Timestamp': data[0]['Timestamp'],  # Interval start timestamp
        'Open': prices[0],
        'High': max(prices),
        'Low': min(prices),
        'Close': prices[-1],
        'Volume': sum(volumes)
        }

    def aggregateOHLCV(self, interval_str, output_file='ohlcvOutput.csv'):
        ohlcv_bars = []
        interval_seconds = self.parseInterval(interval_str)
        interval_start = None # Current interval's start time
        intervalData = []

        # Sort data by timestamp
        self.cleanedData.sort(key=lambda row: row['Timestamp'])

        for entry in self.cleanedData:
            timestamp = datetime.strptime(entry['Timestamp'], '%Y-%m-%d %H:%M:%S.%f')

            # Initializes the first interval
            if interval_start is None:
                interval_start = timestamp
                intervalData.append(entry)
                continue

            # If interval is up
            if (timestamp - interval_start).total_seconds() >= interval_seconds:
                #Stores the current interval
                ohlcv_bars.append(self.processOHLCV(intervalData))

                # Next Interval
                interval_start = timestamp
                intervalData = [entry]
            else:
                # Keep adding
                intervalData.append(entry)

        # Adds to Last interval
        if intervalData:
            ohlcv_bars.append(self.processOHLCV(intervalData))

        # Output the OHLCV data to the CSV file
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ohlcv_bars)

        print(f"OHLCV data successfully written to {output_file}.")
        print(f"Number of OHLCV bars: {len(ohlcv_bars)}")


#Testing
folder_path = '/Users/aarondong/Desktop/CTG/sw-challenge-fall-2024/data'
csv_loader = ctgData(folder_path)
csv_loader.loadData()
csv_loader.cleanData()
csv_loader.aggregateOHLCV('15m')
        