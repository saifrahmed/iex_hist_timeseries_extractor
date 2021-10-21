import os
import sys
from datetime import datetime
import csv
import random
import argparse

from IEXTools import Parser, messages

HEADER = ['timestamp', 'symbol', 'bid_size', 'bid_price', 'ask_price', 'ask_size']

def save_quotes(input_pcap, output_csv, secs_of_interest):

	csv_file = open(output_csv, "w")
	writer = csv.writer(csv_file, delimiter=',')
	writer.writerow(HEADER)		

	allowed = [messages.QuoteUpdate]
	with Parser(input_pcap, tops=True, deep=False) as iex_messages:
		while True:
			try:
				x = iex_messages.get_next_message(allowed)
				if x.symbol in secs_of_interest:
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
						if random.random()<0.01:
							print(dt_object, tick)
						writer.writerow([x.timestamp,x.symbol,x.bid_size,tick['bid_price'],tick['ask_price'],x.ask_size])
			except StopIteration:
				print(f"Reached the end of file {input_pcap}")
				csv_file.close()
				return

if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--input", help="Input PCAP file")
	parser.add_argument("-o", "--output", help="Output CSV file")
	parser.add_argument("-t", '--tickers', help="Comman separated list of tickers of interest")
	args = parser.parse_args()

	print(args)

	save_quotes(input_pcap=args.input, 
				output_csv=args.output, 
				secs_of_interest=args.tickers.split(","))
	

