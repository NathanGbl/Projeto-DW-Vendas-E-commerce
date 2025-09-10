import pandas as pd

df_itens = pd.read_csv("order_items.csv")
df_pedidos = pd.read_csv("order.csv")
df_produtos = pd.read_csv("products.csv")
df_vendedores = pd.read_csv("sellers.csv")
df_lojas = pd.read_csv("stores.csv")
for i in range(1000):