import dash
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
from collections import deque
import pandas as pd
# reference https://www.geeksforgeeks.org/plot-live-graphs-using-python-dash-and-plotly/
X = deque(maxlen = 20)
X.append(1)

Y = deque(maxlen = 20)
Y.append(1)
prevf = -1
app = dash.Dash(__name__)

app.layout = html.Div(children = [
    html.Div([html.H4('Interactive graph of Query5. Y-axis is fare value. X-axis is corresponding element of the incoming stream. Rightmost value is latest')]),
	html.Div([
        
		dcc.Graph(id = 'live-graph', animate = True),
		dcc.Interval(
			id = 'graph-update',
			interval = 2000,
			n_intervals = 0
		),
	]),
    html.Div([html.H4('Interactive graph of Query4')]),
    html.Div([
         
		dcc.Graph(id = 'live-graph2'),
		dcc.Interval(
			id = 'graph-update2',
			interval = 2000,
			n_intervals = 0
		),
	]),
    ]
)
@app.callback(
	Output('live-graph', 'figure'),
	[ Input('graph-update', 'n_intervals') ]
)

def update_graph_scatter(n):
    global prevf
    df = pd.read_csv('sharee/graph_query5.csv')
    if(df.iloc[0]['fare'] != prevf):
        X.append(X[-1]+1)
        Y.append(df.iloc[0]['fare'])
        prevf = df.iloc[0]['fare']

    data = plotly.graph_objs.Scatter(
            x=list(X),
			y=list(Y),
			name='Scatter',
			mode= 'lines+markers'
	)

    return {'data': [data],
			'layout' : go.Layout(xaxis=dict(range=[min(X),max(X)]),yaxis = dict(range = [min(Y),max(Y)]),)}


@app.callback(
	Output('live-graph2', 'figure'),
	[ Input('graph-update2', 'n_intervals') ]
)

def update_graph_scatter(n):
    # print(n)
    '''
    df = px.data.iris() # replace with your own data source
    a = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
    fig = px.scatter_matrix(
        df, dimensions=a[0:(n%4)+1], color="species")
    '''
    df = pd.read_csv('sharee/graph_query4.csv')
    df["km"]=df["km"].values.astype(str)
    # print(df.dtypes)

    fig = px.scatter(df, x="latitude", y="longitude", text = "vehicle_id", size = [30]*(len(df)), hover_name='vehicle_id', hover_data=['latitude','longitude','distance', 'km'], color='km' )
    fig.update_traces(textposition='top center', textfont=dict(color='#E58606'))
    return fig

if __name__ == '__main__':
    app.run_server()
    print("end")