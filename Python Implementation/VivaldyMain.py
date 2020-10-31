#-------------------------------------------#
#Authors: Pekko Lipsanen, Martin Melhus
#Source: https://github.com/pekko/vivaldi
#-------------------------------------------#

#!/usr/bin/python

from GraphVivaldi import GraphVivaldi
from Configuration import Configuration
from Vivaldi import Vivaldi

import sys
from matplotlib.pyplot import *

class VivaldyMain:
	def buildgraph(self,comCost):
		g = GraphVivaldi(len(comCost))
		for node in range(len(comCost)):
			
			for neighbor in range(len(comCost)):
			
				if(comCost[node][neighbor]!=0):
					cost=1/comCost[node][neighbor]
				else:
					cost=0
				g.addVertex(node,neighbor,cost)
	
		return g
	
	def table(self,data, title=""):
		print()
		print ("="*30)
		print (title)
		print ("-"*len(title))

		length = len(data)
		data = sorted(data)

		print ("Average %4d" % (float(sum(data))/length ))
		print ("Median  %4d" % (data[int(length/2)] ))
		print ("Min     %4d" % (min(data) ))
		print ("Max     %4d" % (max(data) ))
		print ("="*30)
	
	def run(self,numberOfDevices,comCost):

		
	# These parameters are part of the Configuration.
	# Modify them according to your need.
		num_neighbors  = 10
		num_iterations = 200
		num_dimension = 3
	
	# build a configuration and load the matrix into the graph
		c = Configuration(numberOfDevices, num_neighbors, num_iterations, d=num_dimension)
		init_graph = self.buildgraph(comCost)

	
	# run vivaldi: here, only the CDF of the relative error is retrieved. 
	# Modify to retrieve what's requested.
		v = Vivaldi(init_graph, c)
		v.run()
		positions=[]
		for i in range(numberOfDevices):
			positions.append(v.getPositions(i))
		return(positions)

