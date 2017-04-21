'''
Created on 13 de abr 2017

@author: marcelo.fernandes
'''

import sys
from sys import argv
import pandas as pd
import os


def main(argv):
    print(':::Início da execução:::')
    print('Parametros de execução:', sys.argv)

    # TODO: incluir crítica para número de parâmetros inválido

    if str(sys.argv).find('csv'):
        meusDados = get_dados_redmine(sys.argv[1])
        # calcula os valores de faturamento
        publica_faturamento(meusDados)
    else:
        print('Arquivo de origem dos dados não informado!')
        print('Indique o caminho completo para o arquivo CSV a ser importado.')
    print(':::Fim da Execução:::')


def get_dados_redmine(arquivo):
    '''
        retorna um objeto DATAFRAME (Pandas) para pesquisa e manipulação
        arquivo - caminho completo para o arquivo CSV
    '''
    meus_dados = pd.read_csv(arquivo, sep=';', encoding='latin-1', header=0, index_col=0)
    meus_dados.sort_values(['Projeto'])
    # Ajusta as datas de planejamento
    meus_dados['Data de início'] = pd.to_datetime(meus_dados['Data de início'], format='%d/%m/%Y', errors='ignore')
    meus_dados['Data de fim'] = pd.to_datetime(meus_dados['Data de fim'], format='%d/%m/%Y', errors='ignore')

    # tratamento de dados usados nos cálculos
    for index, reg in meus_dados.iterrows():
        # Trata os percentuais inválidos (apenas para estimar valores e evitar distorçoes)
        val_teste = str(reg['Percentual da Fase']).replace(',', '.')
        if float(val_teste) > 1:
            print('Percentual tratado ', str((float(val_teste) / 100)).replace('.', ','))
            meus_dados.set_value(index, 'Percentual da Fase', str((float(val_teste) / 100)).replace('.', ','))
        # Trata o valor da Nota para a OS
        if str(reg['Tipo']).find('Ordem') >= 0:
            if str(reg['Valor da NF-e']).strip() == '':
                meus_dados.set_value(index, 'Valor da NF-e', '0.0')

    # remove valores inválidos
    meus_dados.fillna({'Valor da NF-e': 0}, inplace=True)

    # Prepara o campo de ordenação por data
    datas_ord = meus_dados['Data de início']
    datas_ord.rename(columns={'Data de início': 'Dt_ini_ord'}, inplace=True)
    datas_ord.apply(lambda d: str(str(d.year) + '-' + str(d.month) + '-' + str(d.day)))

    # merge dos dados
    meus_dados = pd.concat([meus_dados, datas_ord], axis=1)
    meus_dados.rename(columns={0: 'Dt_ini_ord'}, inplace=True)
    return meus_dados


def get_path_output():
    # testa se a app é script ou frozen exe
    if getattr(sys, 'frozen', False):
        path_dir_saida = os.path.dirname(sys.executable)
    elif __file__:
        path_dir_saida = os.path.dirname(__file__)

    # trata a execução em desenv
    path_dir_saida = str(path_dir_saida).replace('\\src', '')
    return str(path_dir_saida)


def get_faturamento_consolidado_periodo(dados_fat):
    print('Consolidando faturamento...')

    new_columns = ['Projeto', 'Periodo', 'Valor']
    dados_consolid = pd.DataFrame(columns=new_columns)

    # consolida os valores por periodo
    for item, linha in dados_fat.groupby(['Projeto', 'Periodo']):
        # filtra os dados do projeto/periodo

        df_itens = dados_fat.loc[(dados_fat["Projeto"] == item[0]) & (dados_fat["Periodo"] == item[1])]

        val_consolidado = 0
        for index, it_periodo in df_itens.iterrows():
            val_consolidado += (float(it_periodo['Valor da Fase'])*100)

        # preenche o dataframe
        dados_consolid = dados_consolid.append({'Projeto': item[0],
                                                'Periodo': item[1],
                                                'Valor': val_consolidado/100
        }, ignore_index=True)

    # convert/pivot para plotagem
    dados_consolid = dados_consolid.pivot(index='Projeto', columns='Periodo', values='Valor')
    dados_consolid.fillna(0, inplace=True)

    return dados_consolid


def get_status_fase(df_dados, id_fase, status_nominal):
    str_status = ''

    if status_nominal.find('Fase - Solicita') >= 0:
        str_status = 'Entregue'
    elif status_nominal.find('Fase - Recebimento') >= 0:
        str_status = 'Entregue'
    elif status_nominal.find('Fase - Termo') >= 0:
        str_status = 'Entregue'
    elif status_nominal.find('Nova') >= 0:
        # testa estado dos itens da fase e valida entrega prévia ao fluxo
        df_produtos = df_dados.loc[df_dados['Tarefa principal'] == id_fase]

        for item, linha in df_produtos.groupby(['Estado']):
            if item[0] == 'Concluída':
                str_status = 'Entrega Antecipada'
            else:
                str_status = 'Planejada'
    else:
        str_status = 'Em Andamento'

    return str_status


def publica_faturamento(df_dados):
    print('Analisando Faturamento')
    # Preparar dataframe dos itens faturáveis válidos
    df_itens_fat = df_dados.loc[((df_dados['Tipo'] == 'Fase') & (df_dados['Data de fim']))]
    # Cria o DF para plotagem dos dados
    df_plot_faturamento = pd.DataFrame(columns={'Projeto',
                                                'Fase',
                                                'Valor da Fase',
                                                'Periodo',
                                                'Perc Fat',
                                                'Tarefa Princ',
                                                'Status Atv'})

    for index, fase in df_itens_fat.iterrows():
        val_fat_fase = df_dados.loc[fase['Tarefa principal'], 'Valor da NF-e'] * float(fase['Percentual da Fase'].replace(',', '.'))

        str_status_fase = get_status_fase(df_dados, index, fase['Estado'])

        df_plot_faturamento = df_plot_faturamento.append(
            {'Projeto': fase['Projeto'],
             'Fase': index,
             'Valor da Fase': '{:.2f}'.format(val_fat_fase),
             'Periodo': str(str(fase['Data de fim'].year) + '-' + str(fase['Data de fim'].month)),
             'Perc Fat': fase['Percentual da Fase'],
             'Tarefa Princ': fase['Tarefa principal'],
             'Status Atv': str_status_fase}
            , ignore_index=True)

        #

    df_plot_faturamento.fillna(method='ffill', inplace=True) # remove valores inválidos

    # publica a apuração base
    df_plot_faturamento.to_csv(str(get_path_output()) + '\itens_plan_fat.csv', sep=';')

    # publica a sumarização do faturamento
    df_plot_faturamento = df_plot_faturamento.apply(pd.to_numeric, errors='ignore')  # converte para numerico
    df_plot_consolid = get_faturamento_consolidado_periodo(df_plot_faturamento) # Agrupa os dados de faturamento por período
    df_plot_consolid.to_csv(str(get_path_output()) + '\sum_plan_fat.csv', sep=';')


if __name__ == '__main__':
    main(argv)