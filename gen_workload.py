import random

workload_size = 18
query_size = 18
intersect = 9

min_price  = 1.0
max_price  = 100.0
min_im = 1 
max_im = 10000
min_id = 1
max_id = 100000

fo = open("workload.txt", "w")
for i in range(workload_size):
	sql = ""
	field = ""
	a = random.randint(0,1)
	b = random.randint(0,2)
	if(b == 0):
		field = "i_price"
		upperbound = random.randint(min_price,max_price)
		lowerbound = random.randint(min_price,upperbound)
	elif(b == 1):
		field = "i_id"
		upperbound = random.randint(min_id,max_id)
		lowerbound = random.randint(min_id,upperbound)
	else:
		field = "i_im_id"
		upperbound = random.randint(min_im,max_im)
		lowerbound = random.randint(min_im,upperbound) 

	if(a == 0):
		#COUNT
		sql = "\"SELECT COUNT("+field+") FROM item WHERE "+field+" < "+str(upperbound)+" and " +field+" > "+str(lowerbound) +"\","
	else:
		#SUM
		sql ="\"SELECT SUM("+field+") FROM item WHERE "+field+" < "+str(upperbound)+" and " +field+" > "+str(lowerbound) +"\","

	fo.write( sql+"\n" )

fo.close()

#generate incoming query
fo2 = open("workload.txt","r")
fo3 = open("query.txt","w")
for i in range(intersect):
	line = fo2.readline()
	fo3.write(line)
fo2.close()

for i in range(query_size-intersect):
	sql = ""
	field = ""
	a = random.randint(0,1)
	b = random.randint(0,2)
	if(b == 0):
		field = "i_price"
		upperbound = random.randint(min_price,max_price)
		lowerbound = random.randint(min_price,upperbound)
	elif(b == 1):
		field = "i_id"
		upperbound = random.randint(min_id,max_id)
		lowerbound = random.randint(min_id,upperbound)
	else:
		field = "i_im_id"
		upperbound = random.randint(min_im,max_im)
		lowerbound = random.randint(min_im,upperbound) 

	if(a == 0):
		#COUNT
		sql = "\"SELECT COUNT("+field+") FROM item WHERE "+field+" < "+str(upperbound)+" and " +field+" > "+str(lowerbound) +"\","
	else:
		#SUM
		sql ="\"SELECT SUM("+field+") FROM item WHERE "+field+" < "+str(upperbound)+" and " +field+" > "+str(lowerbound) +"\","

	fo3.write( sql+"\n" )

fo3.close()

