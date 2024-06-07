import oracledb
import pandas as pd


entidade_fields = [
    "entidade_id",
    "nome",
    "codigo",
    "grupo"
]

poluicao_cidade_fields = [
    "cidade",
    "regiao",
    "ano",
    "qualidade_ar",
    "poluicao_agua",
    "entidade_id"
]

producao_plastico_fields = [
    "ano",
    "producao_anual_plastico",
    "partici_emissao_oceanos",
    "partici_reciclagem_regional",
    "partici_queima_regional",
    "partici_lixo_mal_gerido",
    "partici_aterros_regional",
    "lixo_mal_gerido_per_capita",
    "entidade_id"
]

pertence_fields = [
    "entidade_id",
    "entidade_pertence"
]

usuario_fields = [
    "nome_usuario",
    "nome",
    "senha"
]

tables = {
    "entidade": entidade_fields,
    "poluicao_cidade": poluicao_cidade_fields,
    "producao_plastico": producao_plastico_fields,
    "pertence": pertence_fields,
    "usuario": usuario_fields
}


def conecta_BD():
    try:
        # conectar com o Servidor
        dnStr = oracledb.makedsn("oracle.fiap.com.br", "1521", "ORCL")
        # efetuar a conexao com o usuario
        conn = oracledb.connect(user='rm552824', password='280200', dsn=dnStr)

    except Exception as e:
        print("Erro: ", e)
        conexao = False
        conn = ""
    else:
        conexao = True

    return conexao, conn


def select(cursor, table_name, fields="*", where=None, join=None, order_by=None, group_by=None):
    query = f"SELECT {fields} FROM {table_name}"
    
    if join:
        query += f" {join}"
    
    if where:
        query += f" WHERE {where}"
        
    if order_by:
        query += f" ORDER BY {order_by}"
        
    if group_by:
        query += f" GROUP BY {group_by}"
    
    cursor.execute(query)
    return cursor.fetchall()


def inserir_arquivos_csv(conn):
    try:
        tabela_oracle = manipulating_dataframe_to_insert()[0]
        df_final = manipulating_dataframe_to_insert()[1]

        str_insert = f"""INSERT INTO {tabela_oracle} (nome, codigo, grupo) VALUES (:1, :2, :3)"""

        cursor = conn.cursor()

        try:
            for _, row in df_final.iterrows():
                cursor.execute(str_insert, (row['Entidade'], row['Código'], row['grupo']))

            conn.commit()
        except Exception as error:
            print(f"Erro ao inserir os dados: {error}")
        finally:
            cursor.close()
            conn.close()

    except Exception as error:
        print(f"Erro ao inserir os dados: {error}")
    finally:
        print("Operação realizada com sucesso!\n")


def print_table(table_name, fields, data):
    col_widths = [max(len(str(item)) for item in col) for col in zip(*([fields] + data))]

    def print_separator():
        print("+" + "+".join("-" * (width + 2) for width in col_widths) + "+")

    def print_row(row):
        print("| " + " | ".join((str(val) + " " * (width - len(str(val)))) for val, width in zip(row, col_widths)) + " |")

    print(f"Tabela: {table_name}")
    print_separator()
    print_row(fields)
    print_separator()
    for row in data:
        print_row(row)
    print_separator()
    print()
    

def main():
    conexao, conn = conecta_BD()
    opc = -1
    while (opc != 0 and conexao == True):
        print("1-Cadastro de informações contidas no CSV")
        print("2-Listar todas as entidades")
        print("3-Listar todas as linhas referentes à poluição na cidade")
        print("4-Listar todas as linhas referentes à produção de plástico")
        print("5-Listar todos os usuários")
        print("0-Sair\n")

        opc = int(input("Digite a opção desejada, ou 0 para sair: "))

        if (opc == 1):
            inserir_arquivos_csv(conn)
        elif (opc == 2):
            resposta = select(conn.cursor(), "entidade")
            print_table("Entidade", tables["entidade"], resposta)
        elif (opc == 3):
            resposta = select(conn.cursor(), "poluicao_cidade")
            print_table("Poluição na Cidade", tables["poluicao_cidade"], resposta)
        elif (opc == 4):
            resposta = select(conn.cursor(), "producao_plastico")
            print_table("Produção de Plastico", tables["producao_plastico"], resposta)
        elif (opc == 5):
            resposta = select(conn.cursor(), "usuario")
            print_table("Usuário", tables["usuario"], resposta)
        elif (opc == 0):
            print("Saindo...")
        else:
            print("Opção inválida. Tente novamente.")
            
        print("\n")


def manipulating_dataframe_to_insert():
    path = '../csv_files/'
    df1 = pd.read_csv(path + '1- producao-de-plastico-global.csv')
    df2 = pd.read_csv(path + '2- participacao-despejo-residuo-plastico.csv')
    df3 = pd.read_csv(path + '3- destino-plastico.csv')
    df4 = pd.read_csv(path + '4- desperdicio-plastico-per-capita.csv')
    df5 = pd.read_csv(path + '5- poluicao-agua-cidades.csv')

    df5_renew = df5.rename(columns={' Entidade': 'Entidade'})
    df5_grupo_paises = df5_renew.groupby('Entidade').mean(numeric_only=True)

    df_final = df2.set_index('Entidade').join(df5_grupo_paises)
    df_final = df_final.join(df4.set_index('Entidade'))

    df_final = df_final.dropna()

    df_entidade = df_final.drop(
        columns=['Ano', ' Ano', 'Participação na emissão global de plásticos para o oceano', ' Qualidade do Ar',
                 ' Poluição da Água', ' Código', ' Lixo plástico mal gerenciado por pessoa (kg por ano)'])

    def definir_grupo(codigo):
        if codigo in ['USA', 'IND', 'CHN']:
            return 0
        else:
            return 1

    df_entidade['grupo'] = df_entidade['Código'].apply(definir_grupo)

    df_entidade = df_entidade.reset_index()
    tabela_oracle = 'entidade'

    return [tabela_oracle, df_entidade]


if (__name__ == "__main__"):
    main()
