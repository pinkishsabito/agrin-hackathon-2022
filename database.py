from datetime import datetime
import json
import sqlite3


def writeClients():
	with open('dataset/clients.json', 'r') as file:
		data = json.loads(file.read())
		try:
			with sqlite3.connect('db.db') as connection:
				cursor = connection.cursor()
				sqlQuery = 'delete from clients where clientId >= 0'
				cursor.execute(sqlQuery)
				for d in data:
					index = d['ClientId']['Id']
					name = d['Name']
					regDate = d['Date']
					sqlQuery = 'insert into clients values(?, ?, ?)'
					sqlData = (index, name, regDate)
					cursor.execute(sqlQuery, sqlData)
				connection.commit()
		except sqlite3.Error:
			exit('Connection Error Clients')


def writeDeliveryCompanies():
	with open('dataset/deliveryCompanies.json', 'r') as file:
		data = json.loads(file.read())
		try:
			with sqlite3.connect('db.db') as connection:
				cursor = connection.cursor()
				sqlQuery = 'delete from deliveryCompanies where deliveryCompanyId >= 0'
				cursor.execute(sqlQuery)
				for d in data:
					index = d['DeliveryCompanyId']['Id']
					name = d['Name']
					regDate = d['Date']
					sqlQuery = 'insert into deliveryCompanies values(?, ?, ?)'
					sqlData = (index, name, regDate)
					cursor.execute(sqlQuery, sqlData)
				connection.commit()
		except sqlite3.Error:
			exit('Connection Error Delivery Companies')


def writeEvents():
	def writeOfferAcceptEvent(value):
		OfferId = value['OfferId']['Id']
		Date = value['Date']
		sqlQuery = 'insert into offerAcceptEvent values(?, ?)'
		sqlData = (OfferId, Date)
		cursor.execute(sqlQuery, sqlData)

	def writeOfferEvent(value):
		OfferId = value['OfferId']['Id']
		DeliveryCompanyId = value['DeliveryCompanyId']['Id']
		OfferedPriceTenge = value['OfferedPriceTenge']
		Date = value['Date']
		OrderId = value['OrderId']['Id']
		sqlQuery = 'insert into offerEvent values(?, ?, ?, ?, ?)'
		sqlData = (OfferId, DeliveryCompanyId, OfferedPriceTenge, Date, OrderId)
		cursor.execute(sqlQuery, sqlData)

	def writeOrderCancelEvent(value):
		CancelReason = value['CancelReason']
		Date = value['Date']
		OrderId = value['OrderId']['Id']
		sqlQuery = 'insert into orderCancelEvent values(?, ?, ?)'
		sqlData = (CancelReason, Date, OrderId)
		cursor.execute(sqlQuery, sqlData)

	def writeOrderEvent(value):
		ClientId = value['ClientId']['Id']
		FromLocationId = value['FromLocationId']
		ToLocationId = value['ToLocationId']
		WeightKg = value['WeightKg']
		ExpectingPrice = value['ExpectingPriceTenge']
		ExpectingDeliveryDate = value['ExpectingDeliveryDate']
		Date = value['Date']
		OrderId = value['OrderId']['Id']
		sqlQuery = 'insert into orderEvent values(?, ?, ?, ?, ?, ?, ?, ?)'
		sqlData = (ClientId, FromLocationId, ToLocationId, WeightKg, ExpectingPrice, ExpectingDeliveryDate, Date, OrderId)
		cursor.execute(sqlQuery, sqlData)

	def writeOrderFailEvent(value):
		FailReason = value['FailReason']
		Date = value['Date']
		OrderId = value['OrderId']['Id']
		sqlQuery = 'insert into orderFailEvent values(?, ?, ?)'
		sqlData = (FailReason, Date, OrderId)
		cursor.execute(sqlQuery, sqlData)

	def writeOrderFreightReceiveEvent(value):
		Date = value['Date']
		OrderId = value['OrderId']['Id']
		sqlQuery = 'insert into orderFreightReceiveEvent values(?, ?)'
		sqlData = (Date, OrderId)
		cursor.execute(sqlQuery, sqlData)

	def writeOrderFulfillmentEvent(value):
		Date = value['Date']
		OrderId = value['OrderId']['Id']
		sqlQuery = 'insert into orderFulfillmentEvent values(?, ?)'
		sqlData = (Date, OrderId)
		cursor.execute(sqlQuery, sqlData)

	with open('dataset/events.json', 'r') as file:
		data = json.loads(file.read())
		try:
			with sqlite3.connect('db.db') as connection:
				cursor = connection.cursor()
				cursor.execute('delete from offerAcceptEvent where offerId >= 0')
				cursor.execute('delete from offerEvent where orderId >= 0')
				cursor.execute('delete from orderCancelEvent where orderId >= 0')
				cursor.execute('delete from orderEvent where orderId >= 0')
				cursor.execute('delete from orderFailEvent where orderId >= 0')
				cursor.execute('delete from orderFreightReceiveEvent where orderId >= 0')
				cursor.execute('delete from orderFulfillmentEvent where orderId >= 0')
				for i, v in enumerate(data):
					if v['$type'] == 'Hackathon2022.OrderEvent, Hackathon2022':
						writeOrderEvent(v)
					elif v['$type'] == 'Hackathon2022.OfferEvent, Hackathon2022':
						writeOfferEvent(v)
					elif v['$type'] == 'Hackathon2022.OfferAcceptEvent, Hackathon2022':
						writeOfferAcceptEvent(v)
					elif v['$type'] == 'Hackathon2022.OrderCancelEvent, Hackathon2022':
						writeOrderCancelEvent(v)
					elif v['$type'] == 'Hackathon2022.OrderFreightReceiveEvent, Hackathon2022':
						writeOrderFreightReceiveEvent(v)
					elif v['$type'] == 'Hackathon2022.OrderFulfillmentEvent, Hackathon2022':
						writeOrderFulfillmentEvent(v)
					elif v['$type'] == 'Hackathon2022.OrderFailEvent, Hackathon2022':
						writeOrderFailEvent(v)
					print(f'COMMENT: {i + 1} completed')
				connection.commit()
		except sqlite3.Error:
			exit('Connection Error Events')


def writeLocations():
	with open('dataset/locations.json', 'r') as file:
		data = json.loads(file.read())
		try:
			with sqlite3.connect('db.db') as connection:
				cursor = connection.cursor()
				sqlQuery = 'delete from locations where locationId >= 0'
				cursor.execute(sqlQuery)
				for d in data:
					index = d['LocationId']
					name = d['Name']
					x = d['Coordinates'][0]
					y = d['Coordinates'][1]
					srid = d['CoordinatesSrid']
					sqlQuery = 'insert into locations values(?, ?, ?, ?, ?)'
					sqlData = (index, name, x, y, srid)
					cursor.execute(sqlQuery, sqlData)
				connection.commit()
		except sqlite3.Error:
			exit('Connection Error Locations')


def doIt():
	BEFORE = datetime.now()
	writeClients()
	writeDeliveryCompanies()
	writeEvents()
	writeLocations()
	AFTER = datetime.now()
	print(f'COMMENT: Time elapsed: {AFTER - BEFORE}')
