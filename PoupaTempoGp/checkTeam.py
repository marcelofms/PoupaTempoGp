'''

criado em 19/04/2017

'''

import sys
import pandas as pd
import os


def show_producao_per_recurso(dados_prod):
    print('Calculando produção individual...')
    '''
     calcula as proporções de valor de cada produto feito pelos recursos para
     agrupar por período (totalizado)
    '''
    # dataframe a ser preenchido
    df_producao_per_recurso = pd.DataFrame(columns=['Recurso',
                                                    'Projeto',
                                                    'Fase',
                                                    'Valor_fase',
                                                    'Esforco_fase',
                                                    'Produto',
                                                    'Valor_produto',
                                                    'Esforco_produto',
                                                    'Entrega',
                                                    'Status'])
    ####

    df_fases = dados_prod.loc[dados_prod['Tipo'] == 'Fase']  # Lista todas as fases
    df_valores_fase = pd.DataFrame(columns=['Projeto', 'Fase', 'Esforco', 'Valor', 'Entrega'])

    # preenche a relação de fases com os dados necessarios
    for index, i_fase in df_fases.iterrows():
        ''' calcula valor da fase '''
        custo_folha = dados_prod.loc[i_fase['Tarefa principal'], 'Valor da NF-e'] * float(str(i_fase['Percentual da Fase']).replace(',', '.'))

        ''' Calcula esforço total (soma do esforço dos produtos da fase) '''
        df_prods = dados_prod.loc[(dados_prod['Tipo'] == 'Produto') & (dados_prod['Tarefa principal'] == index)] 
        esforco_total = 0

        if not df_prods.empty:
            for idx, i_prod in df_prods.iterrows():
                esforco_total += int(calc_duracao_tarefa(i_prod['Data de início'], i_prod['Data de fim']))

            df_valores_fase = df_valores_fase.append({'Projeto': i_fase['Projeto'],
                                    'Fase': index,
                                    'Esforco': esforco_total,
                                    'Valor': custo_folha,
                                    'Entrega': i_fase['Data de fim']
                                    }, ignore_index=True)
   
    df_valores_fase.fillna(0, inplace=True)
    
    
    ''' busca todos os recursos que figuram no planejamento '''
    df_recursos = dados_prod.loc[dados_prod['Tipo'] == 'Produto']  # apenas produtos para identificar a atribuição
        
    for id_rec, rec in df_recursos.groupby(['Atribuído a']):
        ''' busca todos os produtos do recurso '''
        df_prod_rec = dados_prod.loc[(dados_prod['Tipo'].isin(['Produto','Atividade'])) &
                                     (dados_prod['Atribuído a'] == id_rec)]

        ''' Apura o valor proporcional de cada produto '''
        
        vlr_prod = 0
        if not df_prod_rec.empty:
            for id_prd, prds in df_prod_rec.iterrows():
                esforco_prod = calc_duracao_tarefa(prds['Data de início'], prds['Data de fim'])
                
                df_fase_prod = df_valores_fase.loc[df_valores_fase['Fase'] == prds['Tarefa principal']]
                
                if not df_fase_prod.empty:
                    esf_fase = 0
                    val_fase = 0
                    for indx, fs in df_fase_prod.iterrows():
                        esf_fase  += fs['Esforco']
                        val_fase += fs['Valor']

                    vlr_prod = (esforco_prod / esf_fase) * val_fase

                    df_producao_per_recurso = df_producao_per_recurso.append({'Recurso': id_rec,
                                                'Projeto': prds['Projeto'],
                                                'Fase':prds['Tarefa principal'],
                                                'Valor_fase': "{0:.2f}".format(val_fase),
                                                'Esforco_fase': esf_fase,
                                                'Produto': id_prd,
                                                'Valor_produto': "{0:.2f}".format(vlr_prod),
                                                'Esforco_produto': esforco_prod,
                                                'Entrega': prds['Data de fim'],
                                                'Status': prds['Estado']
                                                }, ignore_index=True)
    
    ''' Publica produção por recurso detalhada '''
    df_producao_per_recurso.to_csv(str(get_path_output()) + '\producao_recursos_detalhe.csv', sep=';')

    ''' versão agrupada dos dados '''
    publica_prod_consolidada(df_producao_per_recurso)


def publica_prod_consolidada(df_producao):
    print('Consolidando dados...')
    df_periodos = df_producao['Entrega']
    df_periodos.apply(lambda d: str(str(d.year) + '-' + str(d.month)))
    df_producao = pd.concat([df_produção,df_periodos], axis=1)
    # TODO: testar consolidação da produção individual
    df_producao.to_csv(str(get_path_output()) + '\prod_rec_consolid.csv', sep=';')


def calc_duracao_tarefa(dt_ini_tar, dt_fim_tar):
    if ((str(dt_ini_tar) and str(dt_fim_tar)) and
        (str(dt_fim_tar) != 'NaN') and
        (str(dt_fim_tar) != 'NaN') and
        (str(dt_ini_tar) != 'NaT') and
        (str(dt_fim_tar) != 'NaT')
    ):
        dti_dur_tarefa = pd.bdate_range(dt_ini_tar, dt_fim_tar)
        duracao_dias = dti_dur_tarefa.size
    elif not dt_fim_tar:
        duracao_dias = 1
    else:
        duracao_dias = 0

    if not str(duracao_dias).isnumeric:
        duracao_dias = 0 

    return duracao_dias


def main(argv):
    print(':::Início da execução:::')
    print('Parametros de execução:', argv)

    # TODO: incluir crítica para número de parâmetros inválido

    if str(argv).find('csv'):
        # calcula estatisticas do time
        print('Analisando equipes...')
        dados_prod = get_dados_redmine(sys.argv[1])
        show_producao_per_recurso(dados_prod)
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

if __name__=='__main__':
    main(sys.argv)
