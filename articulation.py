import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
	# Get the starting count of connected components
	# YOUR CODE HERE
	counts = g.connectedComponents().select("component").distinct().count()
	# Default version sparkifies the connected components process 
	# and serializes node iteration.
	output = []
	if usegraphframe:
		# Get vertex list for serial iteration
		# YOUR CODE HERE
		vs = g.vertices.map(lambda x: x.id).collect()

		# For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the output
		# YOUR CODE HERE
		for v in vs:
			subV = g.vertices.filter("id!='" + v + "'")
			subE = g.edges.filter("src!='"+v+"'").filter("dst!='"+v+"'")
			temp_graph = GraphFrame(subV, subE)
			subGCount = temp_graph.connectedComponents().select("component").distinct().count()
			if subGCount>counts:
				output.append((v,1))
			else:
				output.append((v,0))
		out = sqlContext.createDataFrame(sc.parallelize(output), ['id', 'articulation'])
		return out
		
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
	else:
        # YOUR CODE HERE
		nGraph = nx.Graph()
		nGraph.add_nodes_from(g.vertices.map(lambda x: x.id).collect())
		nGraph.add_edges_from(g.edges.map(lambda x: (x.src, x.dst)).collect())
		nodes = list(nGraph.nodes)

		for i, node in enumerate(nodes):
			subG = nGraph.subgraph(nodes[:i]+nodes[i+1:])
			subGCount = nx.number_connected_components(subG)
			if subGCount>counts:
				output.append((node,1))
			else:
				output.append((node,0))

		return sqlContext.createDataFrame(sc.parallelize(output), ['id', 'articulation'])

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")
df.filter('articulation = 1').toPandas().to_csv('articulation_out.csv')

#Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
