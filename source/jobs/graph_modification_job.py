from source.lib import network_library as nl

sub_graph_6000_nodes = nl.SocialGraph("sub_graph_6000_nodes")
sub_graph_14000_nodes = nl.SocialGraph("sub_graph_14000_nodes")
nodes_6000 = sub_graph_6000_nodes.graph.nodes()
node_14000 = sub_graph_14000_nodes.graph.nodes()
total_nodes = set(nodes_6000 + node_14000)
sub_graph_20000_nodes = nl.SocialGraph.build_graph_from_nodes(total_nodes, "sub_graph_20000_nodes")
#sub_graph_20000_nodes.export_gml()