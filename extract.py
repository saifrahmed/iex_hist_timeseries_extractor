import os
import sys
from datetime import datetime
import csv

from IEXTools import Parser, messages


# securities of interest (limit this, or your outputs will be huge!)
SOI = ['AIG', "ACN", "GS", "OMC"]
SOI = ["GME", "AMC", "CLOV", "AFRM", "WISH"]


# get data from https://iextrading.com/trading/market-data/
DATA_FOLDER = "/Users/saif/Downloads"
DATA_FILE = 'data_feeds_20211019_20211019_IEXTP1_TOPS1.6.pcap'
DATA_FILE = 'data_feeds_20211018_20211018_IEXTP1_TOPS1.6.pcap'
DATA_FILE = 'data_feeds_20211015_20211015_IEXTP1_TOPS1.6.pcap'
OUTFILE_CSV = "ticker_tape.csv"


HEADER = ['timestamp', 'symbol', 'bid_size', 'bid_price', 'ask_price', 'ask_size']

def save_quotes(input_pcap, output_csv, secs_of_interest):

	csv_file = open(output_csv, "w")
	writer = csv.writer(csv_file, delimiter=',')
	writer.writerow(HEADER)		

	allowed = [messages.QuoteUpdate]
	with Parser(input_pcap, tops=True, deep=False) as iex_messages:
		while True:
			x = iex_messages.get_next_message(allowed)
			if x.symbol in SOI:
				if x.bid_size!=0 and x.ask_size!=0:
					dt_object = datetime.fromtimestamp(int(str(x.timestamp)[:10]))

					tick = {
						"timestamp": x.timestamp,
						"symbol": x.symbol,
						"bid_size": x.bid_size,
						"bid_price": x.bid_price_int/10000,
						"ask_price": x.ask_price_int/10000,
						"ask_size": x.ask_size
						}
					print(dt_object, tick)
					writer.writerow([x.timestamp,x.symbol,x.bid_size,tick['bid_price'],tick['ask_price'],x.ask_size])

if __name__ == "__main__":

	save_quotes(input_pcap=f"{DATA_FOLDER}/{DATA_FILE}", 
				output_csv=OUTFILE_CSV, 
				secs_of_interest=SOI)
	

