import requests
import pandas as pd
from dash import Dash, dcc, html
import plotly.express as px

transactions_url = "http://127.0.0.1:5000/api/transactions"
social_media_url = "http://127.0.0.1:5000/api/social_media"
web_logs_url = "http://127.0.0.1:5000/api/web_logs"

transactions_data = requests.get(transactions_url).json()
social_media_data = requests.get(social_media_url).json()
web_logs_data = requests.get(web_logs_url).json()

transactions_df = pd.DataFrame(transactions_data)
social_media_df = pd.DataFrame(social_media_data)
web_logs_df = pd.DataFrame(web_logs_data)

web_logs_df['timestamp'] = web_logs_df['value'].apply(lambda x: x.split(' ')[0] if len(x.split(' ')) > 1 else None)  # Extraire le timestamp
web_logs_df['activity_level'] = web_logs_df['value'].apply(lambda x: x.split(' ')[1] if len(x.split(' ')) > 1 else None)  # Extraire le niveau d'activité

app = Dash(__name__)

transactions_fig = px.bar(
    transactions_df,
    x="id", 
    y="amount",
    title="Transactions par client",
    labels={"id": "ID Client", "amount": "Montant"}
)

social_media_fig = px.pie(
    social_media_df,
    names="payment_method",
    title="Répartition des utilisateurs par méthode de paiement"
)


web_logs_fig = px.line(
    web_logs_df,
    x="timestamp",
    y="activity_level",
    title="Niveau d'activité dans les logs web",
    labels={"timestamp": "Temps", "activity_level": "Niveau d'activité"}
)

app.layout = html.Div([
    html.H1("Tableau de bord des données collectées", style={"textAlign": "center"}),
    dcc.Graph(figure=transactions_fig),
    dcc.Graph(figure=social_media_fig),
    dcc.Graph(figure=web_logs_fig)
])

if __name__ == "__main__":
    app.run_server(debug=True)
