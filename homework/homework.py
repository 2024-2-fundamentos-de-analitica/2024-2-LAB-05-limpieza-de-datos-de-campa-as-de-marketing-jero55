"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import os

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    archivos = os.listdir("files/input/")
    columnas =["client_id","age","job","marital","education","credit_default","mortgage","month","day","contact_duration","number_contacts","previous_campaign_contacts","previous_outcome","cons_price_idx","euribor_three_months","campaign_outcome"]
    df = pd.DataFrame(columns=columnas,index=None)
    # limitador = 0
    for archivo in archivos:
        # if limitador == 1:
        #     break
        if not archivo.endswith(".zip"):
            continue
        df_archivo = pd.read_csv(f"files/input/{archivo}", compression="zip",index_col=0)
        df = pd.concat([df, df_archivo], ignore_index=True)
        # limitador += 1

    df_cliente = df[["client_id","age","job","marital","education","credit_default","mortgage"]].copy()
    df_cliente["job"] = df_cliente["job"].str.replace(".","").str.replace("-","_")
    df_cliente["education"] = df_cliente["education"].str.replace(".","_").replace("unknown",pd.NA)
    df_cliente["credit_default"] = df_cliente["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    df_cliente["mortgage"] = df_cliente["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    df_cliente.dropna(how="all")
    # print(df_cliente.shape)

    meses = {"jan": "01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06","jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}

    df_campaign = df[["client_id","number_contacts","contact_duration","previous_campaign_contacts","previous_outcome","campaign_outcome","month","day"]].copy()
    df_campaign["previous_outcome"] = df_campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    df_campaign["campaign_outcome"] = df_campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    df_campaign["last_contact_date"] = "2022-" + df_campaign["month"].apply(lambda x: meses[x] ).astype(str) +"-"+ df_campaign["day"].astype(str)  
    df_campaign.dropna(how="all")
    df_campaign.drop(columns=["month","day"],inplace=True)
    # print(df_campaign.shape)

    df_economics = df[["client_id","cons_price_idx","euribor_three_months"]].copy()
    df_economics.dropna(how="all")
    # print(df_economics.shape)

    df_cliente.to_csv("files/output/client.csv",index=False)
    df_campaign.to_csv("files/output/campaign.csv",index=False)
    df_economics.to_csv("files/output/economics.csv",index=False)
    return


if __name__ == "__main__":
    # print("----------------------------------------------------------------------------------")
    clean_campaign_data()
