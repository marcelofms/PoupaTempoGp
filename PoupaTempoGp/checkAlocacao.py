import sys
import pandas as pd
import datetime as dt
import os
from calendar import monthrange


def get_path_output():
    # testa se a app é script ou frozen exe
    if getattr(sys, 'frozen', False):
        path_dir_saida = os.path.dirname(sys.executable)
    elif __file__:
        path_dir_saida = os.path.dirname(__file__)

    # trata a execução em desenv
    path_dir_saida = str(path_dir_saida).replace('\\src', '')
    return str(path_dir_saida)


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


def show_alocacao_per_recurso(df_dados):
    # separa os itens de alocação
    df_alocacao = df_dados.loc[df_dados["Tipo"].isin(['Produto', 'Atividade'])]
    # cria o novo DF de dados da alocação
    df_schedule = pd.DataFrame(columns={'Recurso', 'Ano', 'Mês', 'Atividade', 'Dia', 'Aloc'})

    for index, task in df_alocacao.iterrows():
        # interage sobre o periodo de alocação de cada tarefa
        mes_ini = task['Data de início'].month
        mes_fim = task['Data de fim'].month
        ano_ini = task['Data de início'].year
        ano_fim = task['Data de fim'].year
        ind_aloc = 0

        # preenche o mês inicial
        faixa_mes_ini = monthrange(ano_ini, mes_ini)
        for dia in range(1, faixa_mes_ini[1]+1):
            ind_aloc = 0
            itdate = dt.datetime.strptime(str(ano_ini) + '-' + str(mes_ini) + '-' + str(dia), '%Y-%m-%d')
            if task['Data de início'] <= itdate <= task['Data de fim']:
                ind_aloc = 1

            df_schedule = df_schedule.append({'Recurso': task['Atribuído a'],
                                'Ano': ano_ini,
                                'Mês': mes_ini,
                                'Atividade': index,
                                'Dia': dia,
                                'Aloc': ind_aloc}, ignore_index=True)

        # preenche o mês final, se necessario
        if mes_ini != mes_fim:
            faixa_mes_fim = monthrange(ano_fim, mes_fim)
            for dia in range(1, faixa_mes_fim[1] + 1):
                ind_aloc = 0
                itdate = dt.datetime.strptime(str(ano_fim) + '-' + str(mes_fim) + '-' + str(dia), '%Y-%m-%d')
                if task['Data de início'] <= itdate <= task['Data de fim']:
                    ind_aloc = 1

                df_schedule = df_schedule.append({'Recurso': task['Atribuído a'],
                                    'Ano': ano_fim,
                                    'Mês': mes_fim,
                                    'Atividade': index,
                                    'Dia': dia,
                                    'Aloc': ind_aloc}, ignore_index=True)

    # TODO: sinaliza os dias alocados

    df_schedule = pd.pivot_table(df_schedule, index=['Recurso', 'Atividade', 'Ano', 'Mês'], columns='Dia')
    df_schedule.fillna(0, inplace=True)
    df_schedule.to_csv(str(get_path_output() + '\\alocacao_recurso.csv'), sep=';')


def main(argv):
    print(':::Início da execução:::')
    print('Parametros de execução:', argv)

    if str(argv).find('csv'):
        print('Analisando alocação...')
        dados_prod = get_dados_redmine(sys.argv[1])
        show_alocacao_per_recurso(dados_prod)
    else:
        print('Arquivo de origem dos dados não informado!')
        print('Indique o caminho completo para o arquivo CSV a ser importado.')
    print(':::Fim da Execução:::')


if __name__ == '__main__':
    main(sys.argv)
