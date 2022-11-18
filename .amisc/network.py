import networkx as nx

G = nx.DiGraph([(0,1),(1,2)])
print(G)
print(list(G.nodes))
print(list(G.edges))