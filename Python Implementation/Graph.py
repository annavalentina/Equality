#-------------------------------------------#
#Code from: https://www.geeksforgeeks.org/find-paths-given-source-destination/
#Description:Python program to print all
#	paths from a source to destination. 
#-------------------------------------------#

from collections import defaultdict 
   
#This class represents a directed graph  
# using adjacency list representation 
class Graph: 
   
    def __init__(self,vertices,paths,pairs,graph): 
        #No. of vertices 
        self.V= vertices  
        self.paths=paths
        self.pairs=pairs
        self.graph=graph 

    def getPairs(self):
        return self.pairs
    # function to add an edge to graph 
    def addEdge(self,u,v,w): 
        self.graph[u].append(v) 
        self.pairs[(u,v)]=w

    '''A recursive function to print all paths from 'u' to 'd'. 
    visited[] keeps track of vertices in current path. 
    path[] stores actual vertices and path_index is current 
    index in path[]'''
    def printAllPathsUtil(self, u, d, visited, path): 
  
        # Mark the current node as visited and store in path 
        visited[u]= True
        path.append(u) 
        
  
        # If current vertex is same as destination, then print 
        # current path[] 
        if u == d: 
            self.paths.append(path[:])
        else: 
           
            # If current vertex is not destination 
            #Recur for all the vertices adjacent to this vertex 
            for i in self.graph[u]: 
                if visited[i]==False: 
                    self.printAllPathsUtil(i, d, visited, path) 
                      
        # Remove current vertex from path[] and mark it as unvisited 
        path.pop() 
        visited[u]= False
        
    def findParentsAndChildren(self,n):
        children=[]
        parents=[]
        
        for i in self.pairs:
            if(i[0]==n):
               children.append(i[1])
            elif(i[1]==n):
               parents.append(i[0])
        children.sort()
        parents.sort()
        return(children,parents)
   
   
    # Prints all paths from 's' to 'd' 
    def printAllPaths(self,s, d): 
  
        # Mark all the vertices as not visited 
        visited =[False]*(self.V) 
  
        # Create an array to store paths 
        path = [] 
  
        # Call the recursive helper function to print all paths 
        self.printAllPathsUtil(s, d,visited, path) 
        return (self.paths)
