'''

    Criado em 09/05/2017
    Autor: Marcelo
    
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
        monta_mapa_os(meusDados)
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


def cria_layout_mapa_basico():
    '''
        retorna um objeto com o layout esperado na saída 
    '''
    df_header = pd.DataFrame(columns=[
        'Gerente de Projeto',
        'Existe OS',
        'Documentação',
        'Tipo OS',
        'Data Execução',
        'OS',
        'Sistema',
        'Descrição',
        'Solicitante',
        'PF Est.',
        'Valor OS',
        'Situação Real',
        'Recursos',
        'Status Redmine',
        'PF Liq Detalhada'
     ])

    return df_header


def get_recursos_alocados(id_os, lista):

    df_recursos = pd.DataFrame(columns=['Atribuído a'])
    lista_recursos = ''

    # todas as fases
    df_fases = lista.loc[lista['Tarefa principal'] == id_os]
    for idf, fs in df_fases.iterrows():
        # todos os produtos da fase
        df_prods = lista.loc[lista['Tarefa principal'] == idf]
        for idp, prd in df_prods.iterrows():
            df_recursos = df_recursos.append({'Atribuído a': str(prd['Atribuído a'])}, ignore_index=True)

    #agrupa e reduz
    for item, pdg in df_recursos.groupby(['Atribuído a']):
        lista_recursos = lista_recursos + str(item) + ' | '

    return lista_recursos


def get_status_por_fases_os(id_os, lista_completa):
    os_status = ''
    df_fases = lista_completa.loc[lista_completa['Tarefa principal'] == int(id_os)]
    for itfase, itm in df_fases.groupby(['Estado']):

        if str(itfase).find('Fiscal Técnico') >= 0:
            os_status = 'Aguardando Fiscal Técnico'
        elif str(itfase).find('Fiscal Requisitante') >= 0 and os_status == '':
            os_status = 'Aguardando Fiscal Requisitante'
        elif str(itfase).find('Gestor do Contrato') >= 0 and os_status == '':
            os_status = 'Aguardando Gestor do Contrato'

    return os_status


def atualiza_status_os(df_lista_os, lista_completa):
    # Atualiza a situação de cada OS com base na execução interna e tramitação realizada
    for index, ordem in df_lista_os.iterrows():
        if str(ordem['Status Redmine']).find('Em andamento') >= 0 or \
                        str(ordem['Status Redmine']).find('Em Andamento') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Em andamento')
        elif str(ordem['Status Redmine']).find('Espera') >= 0:
            df_lista_os.set_value(index, 'Situação Real', get_status_por_fases_os(ordem['OS'], lista_completa))
        elif str(ordem['Status Redmine']).find('Planejamento') >= 0 or \
                        str(ordem['Status Redmine']).find('Recebida') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Em andamento')
        elif str(ordem['Status Redmine']).find('Nova') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Em definição pelo gestor')
            df_lista_os.set_value(index, 'Data Execução', '')
        elif str(ordem['Status Redmine']).find('Solicita Ajuste') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Em definição pelo gestor')
            df_lista_os.set_value(index, 'Data Execução', '')
        elif str(ordem['Status Redmine']).find('Elaborar Proposta') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Em análise inicial')
            df_lista_os.set_value(index, 'Data Execução', '')
        elif str(ordem['Status Redmine']).find('Finalizada') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Aguardando Gestor do Contrato')
        elif str(ordem['Status Redmine']).find('Emissão de NF-e') >= 0 or \
                        str(ordem['Status Redmine']).find('Contagem Detalhada') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Concluída (Em produção)')
            df_lista_os.set_value(index, 'Documentação', 'Sim')
        elif str(ordem['Status Redmine']).find('Pagamento') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Concluída (Em produção)')
            df_lista_os.set_value(index, 'Documentação', 'Sim')
        elif str(ordem['Status Redmine']).find('Solicitada') >= 0 or \
                        str(ordem['Status Redmine']).find('Contagem Estimada') >= 0 or \
                        str(ordem['Status Redmine']).find('Empenho') >= 0 or \
                        str(ordem['Status Redmine']).find('Solicita Homologação') >= 0 or \
                        str(ordem['Status Redmine']).find('Em Análise') >= 0:
            df_lista_os.set_value(index, 'Situação Real', 'Aguardando autorização do DATASUS')
            df_lista_os.set_value(index, 'Data Execução', '')

    return df_lista_os


def monta_mapa_os(df: pd.DataFrame):
    print('Iniciando mapeamento de OS')
    # cria novo objeto para receber os dados ordenados
    df_mapa = cria_layout_mapa_basico()

    # ordena por projeto
    df_ord_projeto = df.sort_values(['Projeto', 'Criado'], ascending=[1, 1])

    for index, linha in df_ord_projeto.iterrows():

        # testa se a linha é uma OS e não esta cancelada
        if str(linha['Tipo']).find('Ordem') >= 0:
            if str(linha['Estado']).find('Cancelada') < 0:
                # Inclui a OS
                df_mapa = df_mapa.append({
                    'Gerente de Projeto': '',
                    'Existe OS': 'Sim',
                    'Documentação': '',
                    'Tipo OS': linha['Tipo'],
                    'Data Execução': linha['Data de fim'],
                    'OS': str(int(index)),
                    'Sistema': linha['Projeto'],
                    'Descrição': linha['Assunto'],
                    'Solicitante': linha['Autor'],
                    'PF Est.': linha['Contagem Estimada Líquida'],
                    'Valor OS': linha['Valor da NF-e'],
                    'Situação Real': '',
                    'Recursos': get_recursos_alocados(index, df),
                    'Status Redmine' : linha['Estado'],
                    'PF Liq Detalhada': linha['Contagem Detalhada Líquida']
                }, ignore_index=True)

    # TODO: montar verificação de cada OS
    df_mapa = atualiza_status_os(df_mapa, df_ord_projeto)

    print('mapa montado..')
    dir_saida = str(get_path_output() + '\\mapa_os_situacao.csv')
    df_mapa.to_csv(dir_saida, sep=';', columns=[
        'Gerente de Projeto',
        'Existe OS',
        'Documentação',
        'Tipo OS',
        'Data Execução',
        'OS',
        'Sistema',
        'Descrição',
        'Solicitante',
        'PF Est.',
        'Valor OS',
        'Situação Real',
        'Recursos',
        'Status Redmine',
        'PF Liq Detalhada'
    ])


if __name__ == '__main__':
    main(argv)