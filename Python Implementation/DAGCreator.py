#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class that creates DAGs.
#-------------------------------------------#

from Graph import Graph
import random
from collections import defaultdict 
class DAGCreator():
	def __init__(self,typeOfDAG,number):
		self.typeOfDAG=typeOfDAG
		self.number=number
		

	def run(self):
		paths=[]
		pairs={}
		graph= defaultdict(list)  
		parents=[]
		children=[]
		if (self.typeOfDAG=="S"):#Sequential
			noOfSources=1
			numberOfOperators=self.number+2
			g = Graph(numberOfOperators,paths,pairs,graph)
			for i in range (self.number+1):
				if (i==0):
					cost=1
				else:
					cost=round(random.uniform(0.5,2),2)
				g.addEdge(i,i+1,cost)
			source=[0]#Source nodes
			sink=[self.number+1]#Sink nodes
			for i in range(numberOfOperators):
				(c,p)=g.findParentsAndChildren(i)
				children.append(c)
				parents.append(p)
				
		elif(self.typeOfDAG=="R"):#Replicated
			noOfSources=1
			numberOfOperators=self.number+self.number*2+2
			g = Graph(numberOfOperators,paths,pairs,graph)
			for i in range(self.number*2):
				cost=1
				g.addEdge(0,i+1,cost)
			for i in range(self.number*2):
				cost=round(random.uniform(0.5,2),2)
				for j in range(self.number):
					g.addEdge(i+1,j+self.number*2+1,cost)
			for i in range(self.number):	
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(i+self.number*2+1,self.number+self.number*2+1,cost)	
			source=[0]#Source nodes
			sink=[self.number+self.number*2+1]#Sink nodes
			
			for i in range(numberOfOperators):
				(c,p)=g.findParentsAndChildren(i)
				children.append(c)
				parents.append(p)
				
		elif(self.typeOfDAG=="D"):#Diamond
			noOfSources=1
			numberOfOperators=self.number+2
			g = Graph(numberOfOperators,paths,pairs,graph)
			for i in range(self.number):
				sourceCost=1
				g.addEdge(0,i+1,sourceCost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(i+1,self.number+1,cost)
			for i in range(numberOfOperators):
				(c,p)=g.findParentsAndChildren(i)
				children.append(c)
				parents.append(p)
			source=[0]#Source nodes
			sink=[self.number+1]#Sink nodes
	
		elif(self.typeOfDAG=="M"):#Medium DAGs
		
			if(self.number==0):
				noOfSources=1
				numberOfOperators=11
				g = Graph(numberOfOperators,paths,pairs,graph)
				for i in range (10):
					if (i==0):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
				source=[0]#Source nodes
				sink=[10]#Sink nodes
				for i in range(numberOfOperators):
					(c,p)=g.findParentsAndChildren(i)
					children.append(c)
					parents.append(p)
					
			elif(self.number==1):
				noOfSources=1
				numberOfOperators=11
				g = Graph(numberOfOperators,paths,pairs,graph)
				for i in range (4):
					if (i==0):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
					if(i==3):
						g.addEdge(i,5,cost)
				cost=1
				g.addEdge(0,6,cost)
				for i in range (6,8):
					cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
					if(i==6):
						g.addEdge(i,9,cost)
					if(i==7):
						g.addEdge(i,10,cost)
				source=[0]#Source nodes
				sink=[4,5,8,9,10]#Sink nodes
				for i in range(numberOfOperators):
					(c,p)=g.findParentsAndChildren(i)
					children.append(c)
					parents.append(p)
					
			elif(self.number==2):
				noOfSources=3
				numberOfOperators=13
				g = Graph(numberOfOperators,paths,pairs,graph)
				for i in range (2):
					if (i==0):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
				for i in range (3,8):
					if (i==3):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
				for i in range (9,12):
					if (i==9):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(2,6,cost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(8,12,cost)
				source=[0,3,9]#Source nodes
				sink=[12]#Sink nodes
				for i in range(numberOfOperators):
					(c,p)=g.findParentsAndChildren(i)
					children.append(c)
					parents.append(p)

			elif(self.number==3):
				noOfSources=4
				numberOfOperators=14
				g = Graph(numberOfOperators,paths,pairs,graph)
				for i in range (2):
					if (i==0):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
				for i in range (5,8):
					if (i==5):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
					if(i==7):
						g.addEdge(i,9,cost)
				for i in range (11,13):
					if (i==11):
						cost=1
					else:
						cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(2,7,cost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(9,10,cost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(10,13,cost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(8,13,cost)
				g.addEdge(3,4,1)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(4,7,cost)
				source=[0,3,5,11]#Source nodes
				sink=[13]#Sink nodes
				for i in range(numberOfOperators):
					(c,p)=g.findParentsAndChildren(i)
					children.append(c)
					parents.append(p)
					
			elif(self.number==4):
				noOfSources=1
				numberOfOperators=11
				g = Graph(numberOfOperators,paths,pairs,graph)
				for i in range (2,7):
					cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
					if(i==2):
						g.addEdge(i,8,cost)
					if(i==5):
						g.addEdge(i,9,cost)
				for i in range (8,10):
					cost=round(random.uniform(0.5,2),2)
					g.addEdge(i,i+1,cost)
				cost=round(random.uniform(0.5,2),2)
				g.addEdge(1,3,cost)
				g.addEdge(0,1,1)
				g.addEdge(0,2,1)
				source=[0]#Source nodes
				sink=[7,10]#Sink nodes
				for i in range(numberOfOperators):
					(c,p)=g.findParentsAndChildren(i)
					children.append(c)
					parents.append(p)
			
		#Find all paths from source to sink
		for s in source:
			for d in sink:
				paths=g.printAllPaths(s, d)

		return(paths,pairs,source,sink,children,parents,numberOfOperators,noOfSources)
