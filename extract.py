import os
import sys
import copy
import time, datetime
import math
from functools import reduce
from operator import add
import threading

from iex_parser import Parser, TOPS_1_6
from pprint import pprint

# get data from https://iextrading.com/trading/market-data/

#DATA_FILE = 'data_feeds_20210303_20210303_IEXTP1_TOPS1.6.pcap.gz'
#DATA_FILE = 'data_feeds_20210303_20210303_IEXTP1_TOPS1.6.pcap'
DATA_FILE = 'data_feeds_20191219_20191219_IEXTP1_TOPS1.6.pcap.gz'

#SOI = ['QQQ', 'SPY', 'TSLA', 'GME', 'PLTR', 'GBTC', 'MARA']
SOI = ['QQQ', 'SPY']
EMAS_TO_CALC = [3, 5, 11]

latest = {}
snaps = []
emas = {}
tapetime = None

for e in EMAS_TO_CALC:
	emas[e] = []


def show_ts():
	toi = "SPY"
	for i, s in reversed(list(enumerate(snaps))):
		if toi in s:
			(ts, b, a) = s[toi]
			ema5 = ""
			ema3 = ""
			print(emas)
			if len(emas[5]>= i):
				if toi in emas[5][i]:
					if len(emas[3])>= i:
						ema3 = emas[3][-i][toi]
					if len(emas[5])>= i:
						ema5 = emas[5][-i][toi]

			print (f"{ts} \tb: {b:4.2f}  a:{a:4.2f}  m:{round((b+a)/2,2):4.2f} ema3: {ema3}  ema5: {ema5}")


def calc_ema():
	global emas

	for e in EMAS_TO_CALC:
		if len(snaps) < e:
			print(f"Too little historical snapshots to calculate {e} period EMA")
		else:
			print(f"Calculating EMA{e}")
			records_of_interest = snaps[:e]
			ema_insertable = {}
			for s in SOI:
				ema_components = []
				for r in records_of_interest:
					if s in r:
						(t, b, a) = r[s]
						m = round((b + a) / 2, 2)
						if m != 0 :
							ema_components.append(m)
				if (len(ema_components)) > 0:
					ema = round(reduce(add, ema_components) / len(ema_components), 2)
					print(f"EMA Components {ema_components}   ---> {ema}")
					ema_insertable[s] = ema
			emas[e].append(ema_insertable)


def clocker():

	global latest
	global snaps
	global tapetime

	min_current = None
	min_last = None

	while True:
		if tapetime:
			min_current = tapetime.timetuple().tm_min

			if min_last != min_current:
				# we just turned to a new minute
				print(f"First quote for the minute: {tapetime}  hist avail: {len(snaps)}")
				snaps.append(copy.deepcopy(latest))
				calc_ema()
				show_ts()
		else:
			time.sleep(.5)
	
		min_last = min_current
		#time.sleep(.5)
	
def save_quotes():
	global tapetime
	with Parser(DATA_FILE, TOPS_1_6) as reader:
		for message in reader:
			if message['type']=='quote_update':
				s = message['symbol'].decode()
				if s in SOI:
					b = float(message['bid_price'])
					a = float(message['ask_price'])
					t = message['timestamp']
					tapetime = t
					latest[s]=(t, b, a)					

if __name__ == "__main__":

	print("Starting thread monitor clock", end="")
	threading.Thread(target=clocker).start()
	print("...started")

	print("Starting thread to read quotes off the wire", end="")
	threading.Thread(target=save_quotes).start()
	print("...started")


