
class Node():
    def __init__(self, value):
        self.value = value
        self.children = []
        self.parents = []
    
    def __repr__(self):
        return f"{self.value}"
    
    def paths_to_root(self):
        if len(self.parents) == 0:
            return [[self]]
        paths = []
        for parent in self.parents:
            for path in parent.paths_to_root():
                path.append(self)
                paths.append(path)
        return paths
    

class Graph():
    def __init__(self):
        self.nodes = {}
    
    def add(self, node):
        self.nodes[node.value] = node
    
    def addEdge(self, from_value, to_value):
        if from_value not in self.nodes:
            self.add(Node(from_value))
        if to_value not in self.nodes:
            self.add(Node(to_value))
        self.nodes[from_value].children.append(self.nodes[to_value])
        self.nodes[to_value].parents.append(self.nodes[from_value])
    
    def __repr__(self):
        return str.join(",", map(str, self.nodes.values()))
    
    def roots(self):
        return list(filter(lambda x: len(x.parents) == 0, self.nodes.values()))
    
    def leafs(self):
        return list(filter(lambda x: len(x.children) == 0, self.nodes.values()))
    

    

# g = Graph()
# g.addEdge(1, 2)
# g.addEdge(2, 3)
# g.addEdge(1, 4)
# g.addEdge(4, 5)
# g.addEdge(1, 6)
# g.addEdge(2, 7)

# print(g)
# print(g.roots())
# print(g.leafs())

# for node in g.nodes.values():
#     print(node.value, node.paths_to_root())


