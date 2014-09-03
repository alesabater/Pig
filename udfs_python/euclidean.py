from math import sqrt

#ax = [10.2,12.3,10.7,11.3] 
#ay = [56.2,62.3,67.7,63.3]
#val = []

#@outputSchema("dis:tuple(val:double)")
@outputSchema("dis:chararray")
def distance(ax, ay):
	val = []
	#return len(ax)
	for i in range(0,len(ax)-1):
		x = (ax[0][i+1] - ax[0][i])**2
		y = (ay[0][i+1] - ay[0][i])**2
		res = sqrt(x+y)
		val.append(res);
	
	return val
