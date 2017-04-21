'''
Created on 27 de mar de 2017

@author: marcelo.fernandes
'''

import sys
from sys import argv
import pandas as pd
import numpy as np
import os


def main(argv):
    print(':::Início da execução:::')
    print('Parametros de execução:', sys.argv)

    # TODO: incluir crítica para número de parâmetros inválido

    if str(sys.argv).find('csv'):
        meusDados = get_dados_redmine(sys.argv[1])
        # organiza os dados e monta o arquivo de cronograma
        monta_cronograma(meusDados)
        # calcula os valores de faturamento
        # publica_faturamento(meusDados)
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
        if (float(val_teste) > 1):
            print('Percentual tratado ', str((float(val_teste) / 100)).replace('.', ','))
            meus_dados.set_value(index, 'Percentual da Fase', str((float(val_teste) / 100)).replace('.', ','))
        # Trata o valor da Nota para a OS
        if (str(reg['Tipo']).find('Ordem') >= 0):
            if (str(reg['Valor da NF-e']).strip() == ''):
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





def calc_duracao_tarefa(dt_ini_tar, dt_fim_tar):
    if (
        (str(dt_ini_tar) and str(dt_fim_tar)) and
        (str(dt_fim_tar) != 'NaN') and
        (str(dt_fim_tar) != 'NaN') and
        (str(dt_ini_tar) != 'NaT') and
        (str(dt_fim_tar) != 'NaT')
    ):
        dti_dur_tarefa = pd.bdate_range(dt_ini_tar, dt_fim_tar)
        duracao_dias = dti_dur_tarefa.size
    elif (not (dt_fim_tar)):
        duracao_dias = 1
    else:
        duracao_dias = 0

    return duracao_dias





def get_folhas_cronograma(lista_completa, id_no_pai, ind_nivel_origem):
    '''
        lista_completa - dataframe com o sdados importados do csv
        id_no_pai - index do nó/antecessor origem da busca
        ind_nivel_origem - string com os caracteres de representação do nível hierarquico '>'
    '''
    # atualiza o nível de busca no indicador
    str_nivel_atual = ' > ' + ind_nivel_origem

    # localiza os filhos do nó informado
    df_lista_filtrada = lista_completa.loc[lista_completa['Tarefa principal'] == id_no_pai]
    df_lista_filtrada.sort_values('Dt_ini_ord', inplace=True)

    df_folhas = pd.DataFrame()

    if (df_lista_filtrada.empty == False):
        # TODO: interagir sobre os ponteiros
        for index, filha in df_lista_filtrada.iterrows():

            custo_folha = 0
            if (str(filha['Tipo']).find('Fase') >= 0):
                custo_folha = lista_completa.loc[id_no_pai, 'Valor da NF-e'] * float(
                    str(filha['Percentual da Fase']).replace(',', '.'))
                print('-- Custo formatado #' + str(index) + ' - ' + str(custo_folha))
            else:
                custo_folha = filha['Valor da NF-e']

            dur_tarefa = calc_duracao_tarefa(filha['Data de início'], filha['Data de fim'])

            # print('Incluindo filha #',index, filha['Assunto'])
            df_folhas = df_folhas.append({
                'info': '',
                'nome gp': '',
                'nome projeto': filha['Projeto'],
                'nome tarefa': str_nivel_atual + filha['Assunto'],
                '% concluída': str(int(filha['% Completo'])),
                'predecessora': '',
                'status da demanda': filha['Estado'],
                'duração': str(int(dur_tarefa)),
                'início': filha['Data de início'],
                'término': filha['Data de fim'],
                'nome dos recursos': filha['Atribuído a'],
                'custo': '{:,.2f}'.format(custo_folha),
                'número da demanda': str(int(index)),
                'Tipo de demanda': ''
            }, ignore_index=True)

            # Busca recursiva dos demais elementos
            df_sub_folhas = get_folhas_cronograma(lista_completa, index, str_nivel_atual)
            if (df_sub_folhas.empty == False):
                df_folhas = df_folhas.append(df_sub_folhas)


                # print('Filhos do nó #',id_no_pai)
                # print(df_folhas)

    return df_folhas  # retorna a estrutura de folhas do nó informado como parâmetro


'''
---------------------------------------------------------------------------------
'''


def cria_layout_cronograma():
    '''
        retorna um objeto cronograma com o layout esperado na saída 
    '''
    df_header = pd.DataFrame(columns=[
        'info',
        'nome gp',
        'nome projeto',
        'nome tarefa',
        '% concluída',
        'predecessora',
        'status da demanda',
        'duração',
        'início',
        'término',
        'nome dos recursos',
        'custo',
        'número da demanda',
        'Tipo de demanda'
    ])

    return df_header


'''
---------------------------------------------------------------------------------
'''


def get_path_output():
    # testa se a app é script ou frozen exe
    if getattr(sys, 'frozen', False):
        path_dir_saida = os.path.dirname(sys.executable)
    elif __file__:
        path_dir_saida = os.path.dirname(__file__)

    # trata a execução em desenv
    path_dir_saida = str(path_dir_saida).replace('\\src', '')
    return str(path_dir_saida)


'''
---------------------------------------------------------------------------------
'''


def monta_cronograma(df: pd.DataFrame):
    print('Iniciando montagem do cronograma')
    # cria novo objeto para receber os dados ordenados
    df_cronograma = cria_layout_cronograma()

    # Localiza as ordens de serviço existentes
    df_ord_projeto = df.sort_values(['Projeto', 'Criado'], ascending=[1, 1])

    for index, linha in df_ord_projeto.iterrows():

        # testa se a linha é uma OS
        if str(linha['Tipo']).find('Ordem') >= 0:
            # Inclui a OS
            df_cronograma = df_cronograma.append({
                'info': '',
                'nome gp': '',
                'nome projeto': linha['Projeto'],
                'nome tarefa': linha['Assunto'],
                '% concluída': str(int(linha['% Completo'])),
                'predecessora': '',
                'status da demanda': linha['Estado'],
                'duração': '',
                'início': linha['Data de início'],
                'término': linha['Data de fim'],
                'nome dos recursos': linha['Atribuído a'],
                'custo': linha['Valor da NF-e'],
                'número da demanda': str(int(index)),
                'Tipo de demanda':linha['Tipo']
            }, ignore_index=True)

            # print('Adicionando folhas da OS #',index)
            df_folhas = get_folhas_cronograma(df, index, '')

            if (df_folhas.empty == False):
                df_cronograma = df_cronograma.append(df_folhas)

    print('Cronograma montado..')
    dir_saida = str(get_path_output() + '\\cronograma.csv')
    df_cronograma.to_csv(dir_saida, sep=';', columns=[
        'info',
        'nome gp',
        'nome projeto',
        'nome tarefa',
        '% concluída',
        'predecessora',
        'status da demanda',
        'duração',
        'início',
        'término',
        'nome dos recursos',
        'custo',
        'número da demanda',
        'Tipo de demanda'
    ])
    return df_cronograma


'''
---------------------------------------------------------------------------------
'''


def reg_log(strlog, tplog):
    if (True):
        print(strlog)


'''
---------------------------------------------------------------------------------
'''

if __name__ == '__main__':
    main(argv)