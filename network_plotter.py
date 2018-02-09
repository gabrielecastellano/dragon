import plotly.plotly as py
from plotly.graph_objs import *
import networkx as nx

from config.configuration import Configuration
from sdo_node.utils.neighborhood import NeighborhoodDetector


class NetworkPlotter:

    def __init__(self, sdos):
        self.topology = nx.Graph()
        self.topology.add_nodes_from(sdos)
        self.neighborhoods = dict()
        for sdo in sdos:
            neighbors = NeighborhoodDetector(sdos=sdos,
                                             base_sdo=sdo,
                                             neighbor_probability=Configuration.NEIGHBOR_PROBABILITY,
                                             max_neighbors_ratio=Configuration.MAX_NEIGHBORS_RATIO,
                                             stable_connections=Configuration.STABLE_CONNECTIONS).get_neighborhood()
            self.topology.add_edges_from([(sdo, sdo2) for sdo2 in neighbors])
            self.neighborhoods[sdo] = neighbors

    def graphical_plot(self):

        pos = nx.fruchterman_reingold_layout(self.topology)
        # pos = nx.get_node_attributes(self.topology, 'pos')

        dmin = 1
        ncenter = 0
        for n in pos:
            x, y = pos[n]
            d = (x - 0.5) ** 2 + (y - 0.5) ** 2
            if d < dmin:
                ncenter = n
                dmin = d
        

        p = nx.single_source_shortest_path_length(self.topology, ncenter)

        # edges
        edge_trace = Scatter(
            x=[],
            y=[],
            line=Line(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines')
        for edge in self.topology.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_trace['x'] += [x0, x1, None]
            edge_trace['y'] += [y0, y1, None]

        # nodes
        node_trace = Scatter(
            x=[],
            y=[],
            text=[],
            mode='markers',
            hoverinfo='text',
            marker=Marker(
                showscale=True,
                # colorscale options
                # 'Greys' | 'Greens' | 'Bluered' | 'Hot' | 'Picnic' | 'Portland' |
                # Jet' | 'RdBu' | 'Blackbody' | 'Earth' | 'Electric' | 'YIOrRd' | 'YIGnBu'
                colorscale='YIGnBu',
                reversescale=True,
                color=[],
                size=10,
                colorbar=dict(
                    thickness=15,
                    title='Node Connections',
                    xanchor='left',
                    titleside='right'
                ),
                line=dict(width=2)))
        for node in self.topology.nodes():
            x, y = pos[node]
            node_trace['x'].append(x)
            node_trace['y'].append(y)

        # nx.draw(self.topology)
        # plt.show()

        '''
        Xv = [pos[k][0] for k in self.topology.nodes()]
        Yv = [pos[k][1] for k in self.topology.nodes()]
        Xed = []
        Yed = []
        for edge in self.topology.edges():
            Xed += [pos[edge[0]][0], pos[edge[1]][0], None]
            Yed += [pos[edge[0]][1], pos[edge[1]][1], None]

        edge_trace = Scatter(x=Xed,
                             y=Yed,
                             mode='lines',
                             line=Line(color='rgb(210,210,210)', width=1),
                             hoverinfo='none'
                             )
        node_trace = Scatter(x=Xv,
                             y=Yv,
                             mode='markers',
                             name='net',
                             marker=Marker(symbol='dot',
                                           size=5,
                                           color='#6959CD',
                                           line=Line(color='rgb(50,50,50)', width=0.5)
                                           ),
                             text=self.topology.nodes(),
                             hoverinfo='text'
                             )
        '''
        # Color node points by the number of connections.

        for node, adjacencies in enumerate(self.topology.adjacency_list()):
            node_trace['marker']['color'].append(len(adjacencies))
            node_info = '# of connections: ' + str(len(adjacencies))
            node_trace['text'].append(node_info)

        # Create Network Graph

        fig = Figure(data=Data([edge_trace, node_trace]),
                     layout=Layout(
                         title='<br>Network graph made with Python',
                         titlefont=dict(size=16),
                         showlegend=False,
                         hovermode='closest',
                         margin=dict(b=20, l=5, r=5, t=40),
                         annotations=[dict(
                             text="Python code: <a href='https://plot.ly/ipython-notebooks/network-graphs/'> https://plot.ly/ipython-notebooks/network-graphs/</a>",
                             showarrow=False,
                             xref="paper", yref="paper",
                             x=0.005, y=-0.002)],
                         xaxis=XAxis(showgrid=False, zeroline=False, showticklabels=False),
                         yaxis=YAxis(showgrid=False, zeroline=False, showticklabels=False)))

        # plot
        py.plot(fig, filename='networkx')

    def print_topology(self):
        print("-------- Topology ---------")
        for sdo, neighborhood in self.neighborhoods.items():
            print(sdo + " -> " + str(neighborhood))
        print("---------------------------")
