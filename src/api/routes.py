import os
from fastapi import FastAPI, HTTPException, Query
import sys
sys.path.append("..")
from func.conn import nova_conexao
from sqlalchemy import text

app = FastAPI()

@app.get("/customers")
def lista_dados_clientes(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_customers_dataset
                    ORDER BY CUSTOMER_ID
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            # clientes = list(res.mappings())
            clientes = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar clientes {e}")
    return clientes

@app.get("/location")
def lista_dados_geograficos(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_geolocation_dataset
                    ORDER BY GEOLOCATION_ZIP_CODE_PREFIX
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            localizacoes = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar localizações {e}")
    return localizacoes

@app.get("/order_items")
def lista_itens_pedidos(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_order_items_dataset
                    ORDER BY ORDER_ID
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            itens_pedido = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar itens pedidos {e}")
    return itens_pedido

@app.get("/payments")
def lista_pagamentos(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_order_payments_dataset
                    ORDER BY ORDER_ID
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            pagamentos = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar pagamentos {e}")
    return pagamentos

@app.get("/reviews")
def lista_reviews(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_order_reviews_dataset
                    ORDER BY REVIEW_ID
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            reviews = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar reviews {e}")
    return reviews

@app.get("/orders")
def lista_pedidos(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_orders_dataset
                    ORDER BY ORDER_ID
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            pedidos = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar pedidos {e}")
    return pedidos

@app.get("/products")
def lista_produtos(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_products_dataset
                    ORDER BY PRODUCT_ID
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            produtos = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar produtos {e}")
    return produtos

@app.get("/sellers")
def lista_vendedores(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..olist_sellers_dataset
                    ORDER BY SELLER_ID
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            vendedores = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar vendedores {e}")
    return vendedores

@app.get("/product_category")
def lista_categoria_produto(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    server = os.getenv("SERVER")
    banco = os.getenv("OLTP")
    try:
        with nova_conexao(server, banco).connect() as conn:
            res = conn.execute(
                text(f"""
                    SELECT *
                    FROM {banco}..product_category_name_translation
                    ORDER BY PRODUCT_CATEGORY_NAME_ENGLISH
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                    """)
            )
            categorias_produto = res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar categorias de produto {e}")
    return categorias_produto
