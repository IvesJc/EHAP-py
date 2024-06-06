import oracledb
import pandas as pd


def main():
    conexao, inst_SQL, conn = conecta_BD()
    opc = 0
    while (opc != 3 and conexao == True):
        print("1-Cadastro de informações contidas no CSV")
        print("3-Sair")

        opc = int(input("Digite a opcao (1-3): "))

        if (opc == 1):

            try:
                tabela_oracle = manipulating_dataframe_to_insert()[0]
                df_final = manipulating_dataframe_to_insert()[1]

                str_insert = f"""INSERT INTO {tabela_oracle} (nome, codigo, grupo) VALUES (:1, :2, :3)"""

                cursor = conn.cursor()

                try:
                    for index, row in df_final.iterrows():
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


def conecta_BD():
    try:
        # conectar com o Servidor
        dnStr = oracledb.makedsn("oracle.fiap.com.br", "1521", "ORCL")
        # efetuar a conexao com o usuario
        conn = oracledb.connect(user='rm553243', password='180600', dsn=dnStr)

        # Criar as instrucoes para cada modulo
        inst_SQL = conn.cursor()

    except Exception as e:
        print("Erro: ", e)
        conexao = False
        inst_SQL = ""
        conn = ""
    else:
        conexao = True

    return (conexao, inst_SQL, conn)


def insert_tabela(inst_SQL, conn, str_insert):
    try:
        inst_SQL.execute(str_insert)
        conn.commit()

    except:
        print("Erro de transacao com o BD")
    else:
        print("Dados gravados com sucesso")


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

    coluna_entidade = df_entidade['Entidade']
    coluna_codigo = df_entidade['Código']
    coluna_grupo = df_entidade['grupo']

    tabela_oracle = 'entidade'

    return [tabela_oracle, df_entidade]



if (__name__ == "__main__"):
    main()
